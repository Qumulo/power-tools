#!/usr/bin/env python
# Copyright (c) 2018 Qumulo, Inc. All rights reserved.
#
# NOTICE: All information and intellectual property contained herein is the
# confidential property of Qumulo, Inc. Reproduction or dissemination of the
# information or intellectual property contained herein is strictly forbidden,
# unless separate prior written permission has been obtained from Qumulo, Inc.
# encoding=utf8
import sys
reload(sys)
# This is in here because python 2.7 struggles sometimes.
sys.setdefaultencoding('utf8')
# This is in here because of issues with OpenSSL on some platforms
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
try:
    import requests
    from qumulo.rest_client import RestClient
except:
    print("Unable to import requests and Qumulo api bindings")
    print("Please run the following command:")
    print("pip install qumulo_api requests")
    sys.exit()
import argparse
import re
import os
import time
import json
from collections import OrderedDict

TRENDS_DOMAIN = "https://trends.qumulo.com"
QUMULO_SUPPORT_EMAIL = "care@qumulo.com"


class QSettings(object):
    host = None
    user = None
    password = None
    upgrade_path = None
    sharepass = None
    rc = None
    download_only = None
    current_version = None
    to_version = None


def version_num(vers):
    p1, p2, p3 = map(int, re.sub(r'[^0-9.]', '', vers).split('.')[:3])
    return p1 * 10000 + p2 * 100 + p3


def version_short(vers):
    p1, p2, p3 = re.sub(r'[^0-9.]', '', vers).split('.')[:3]
    return p1 + "." + p2  + "." + p3


def log_print(msg):
    print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))


def get_download_versions(releases, qs):
    download_versions = []

    # Whether or not we're skipping releases. I.e. looking for the "skipto" release.
    skipping = None

    from_num = version_num(qs.current_version)
    to_num = version_num(qs.to_version)
    for release in releases:
        print("get_download_versions: release %s" % release)
        release_num = version_num(release["version"])

        # Resetting the "skipto" version. I.e. we don't have a target version to match.
        skip_to_version = None

        # If the current release has a "skipto" field, it becomes our "skipto" target.
        if "skipto" in release:
            skip_to_version = release["skipto"]

        if release_num < from_num:
            print("get_download_versions: release too old: %s < %s" % (release_num, from_num))
            continue
        elif release_num > to_num:
            print("get_download_versions: release too new: %s > %s" % (release_num, to_num))
            continue
        elif release_num == from_num:
            # This release is the start version. I.e. the version the cluster is currently on.

            print("get_download_versions: release just right: %s == %s" % (release_num, from_num))
            # If there's a "skipto" target already, we go into skipping mode.
            if skip_to_version != None:
                ### Is this state possible?
                print("get_download_versions: this is the start/current release, yet we have a skipto target defined already")
                ### We mark skipping mode by setting the skipping mode flag to the "skipto" version.
                skipping = skip_to_version
        else:
            # This release is in the range of releases we need. I.e. current_rel < this_rel <= target_rel

            # If we're in skipping mode, let's see if this is the one we're waiting for.
            if skipping != None:
                ### We check whether this is our "skipto" target by comparing it to the skipping mode flag
                if skipping == release["version"]:
                    print("get_download_versions: adding release because it's one we skip to")
                    download_versions.append(release)
                    ### Maybe the skipping flag should be cleared here?
                    #skipping = None
                    ### I don't know what would happen if a release were both a "skipto" target and
                    ### a "skipto" initiator release. Is that even possible?
                else:
                    print("get_download_versions: skipping release because we're waiting for the one to skip to")
            # If we're not in skipping mode, we just grab this release.
            elif skipping == None:
                print("get_download_versions: adding release because it's in range")
                download_versions.append(release)

            # We do this for every release in the target range, whether we've added it or skipped it.

            # If there's a "skipto" target specified, we go into skipping mode.
            if skip_to_version != None:
                ### We mark skipping mode by setting the skipping mode flag to the "skipto" version.
                skipping = skip_to_version
                ### At the top of the loop the "skipto" version will be reset to None, but the
                ### skipping mode is never cleared.

    print("get_download_versions: download list: %s" % download_versions)
    return download_versions


def get_upgrade_list(qs = None):
    r = requests.get(TRENDS_DOMAIN + "/data/upgrade/versions/")
    releases = json.loads(r.text)
    if qs is None:
        return releases
    download_versions = get_download_versions(releases, qs)
    return download_versions


def download_from_trends(qs):
    ####    Get list of releases  ####
    r = requests.get(TRENDS_DOMAIN + "/data/upgrade/versions/")
    releases = json.loads(r.text)

    release_exists = False
    for release in releases:
        if release["version"] == qs.to_version:
            release_exists = True
    if not release_exists:
        log_print("Specified Qumulo Core version %s does not exist." % qs.to_version)
        print("Please correct the upgrade version.")
        sys.exit()

    #### create directory for upgrade qimgs ####
    try:
        qs.rc.fs.create_directory(name=qs.upgrade_path, dir_path='/')
    except:
        e = sys.exc_info()[1]
        if 'fs_entry_exists_error' not in str(e):
            log_print("Error creating directory '/%s' on %s. ** %s **" % (
                                                    qs.upgrade_path,
                                                    qs.host,
                                                    e))

    #### get all qimgs from trends to qumulo cluster
    download_versions = get_download_versions(releases, qs)
    for rel in download_versions:
        qimg = 'qumulo_core_%s.qimg' % rel["version"]
        qumulo_qimg = '/' + qs.upgrade_path + '/' + qimg
        #### check to see if qimg already exists on cluster ####
        file_exists = False
        try:
            attrs = qs.rc.fs.get_attr(path = qumulo_qimg)
            if int(attrs['size']) == rel["size"]:
                file_exists = True
        except:
            e = sys.exc_info()[1]

        if file_exists:
            log_print("qimg file is already uploaded: %s" % qumulo_qimg)
            continue

        log_print("Preparing to download Qumulo Core: %s" % rel["version"])
        try:
            qs.rc.fs.create_file(dir_path='/' + qs.upgrade_path,
                                        name=qimg)
        except:
            e = sys.exc_info()[1]
            log_print("File creation error while trying to upload %s: %s" % (qimg, e))
            log_print("Warning: If the uploaded %s is incomplete or corrupt, you will need to delete it manually." % qimg)
            log_print("(Via a mountpoint or via the API using something like `qq fs_delete`.")

        ####  Only download if a local version of file doesn't exist.
        if not os.path.exists(qimg) or os.path.getsize(qimg) != rel["size"]:
            download_file(qimg, qs)

        log_print("Load qimg file onto Qumulo cluster via API: %s" % qimg)
        with open(qimg, 'rb') as fr:
            qs.rc.fs.write_file(path = '/%s/%s' % (
                                            qs.upgrade_path,
                                            qimg), 
                                        data_file=fr)
        log_print("Upgrade file ready on Qumulo cluster: %s" % qimg)
        log_print("Removing local qimg file: %s" % qimg)
        os.remove(qimg)


def download_file(qimg, qs):
    log_print("Starting download of qimg file: %s" % qimg)
    rsp = requests.get(TRENDS_DOMAIN + "/data/upgrade/version/%s?access_code=%s" % \
                    (qimg, qs.sharepass), allow_redirects=False)
    if rsp.status_code == 404:
        print("Unable to download qimg file. Please check the --sharepass password.")
        sys.exit()
    rsp = requests.get(rsp.headers["Location"], stream=True)
    file_size = int(rsp.headers["content-length"])
    perc = int(file_size * 0.05)
    done_buckets = []
    for i in range(0, int(1 / 0.05)):
        done_buckets.append((i+1) * perc)
    downloaded_bytes = 0
    bucket_num = 0
    sys.stdout.flush()
    with open(qimg, 'wb') as fw:
        for chunk in rsp.iter_content(chunk_size=1000000):
            if chunk: # filter out keep-alive new chunks
                fw.write(chunk)
                downloaded_bytes += 1000000
                if downloaded_bytes > done_buckets[bucket_num]:
                    sys.stdout.write("%s%%  " % (bucket_num * 5, ))
                    sys.stdout.flush()
                    bucket_num += 1
    sys.stdout.write("\n")
    log_print("Completed download of qimg file: %s" % qimg)


def upgrade_cluster():
    qs = QSettings()
    parser = argparse.ArgumentParser()
    parser.add_argument('--qhost', required=True, help='Qumulo hostname or ip address')
    parser.add_argument('--quser', required=True, help='Qumulo API user')
    parser.add_argument('--qpass', required=True, help='Qumulo API password')
    parser.add_argument('--qpath', default='upgrade', help='Root-based path to install/find the upgrade qimg file on the cluster')
    parser.add_argument('--sharepass', help='Fileserver download password. Contact %s for details' % QUMULO_SUPPORT_EMAIL)
    parser.add_argument('--vers', required=True, help='The Qumulo Core version to upgrade to.')
    parser.add_argument('--download-only', default=False, help='Do not perform upgrades. Only download qimg files from fileserver', action='store_true')
    args = parser.parse_args()

    if args.vers == "latest":
        args.vers = get_upgrade_list()[-1]["version"]
    elif re.match(r'^[0-9]+[.][0-9]+[.][0-9]+[A-Zz-z]*$', args.vers):
        # this is a valid qumulo version
        pass
    elif re.match(r'^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+[A-Zz-z]*$', args.vers):
        # this is a valid qumulo version
        pass
    else:
        log_print("Exiting")
        print("'%s' is not a valid Qumulo Core version" % args.vers)
        sys.exit()

    qs.to_version     = version_short(args.vers)
    qs.host           = args.qhost
    qs.user           = args.quser
    qs.password       = args.qpass
    qs.upgrade_path   = args.qpath
    qs.sharepass      = args.sharepass
    qs.download_only  = args.download_only

    ####   Set up the Qumulo REST client
    log_print("Logging into Qumulo cluster %s to begin upgrade process" % qs.host)
    try:
        qs.rc = RestClient(qs.host, 8000)
        qs.rc.login(qs.user, qs.password)
        log_print("Login succesful")
    except:
        log_print("Unable to connect to Qumulo cluster %s via api" % qs.host)
        log_print("Credentials used: username=%s, password=%s" % (
                                                    qs.user, qs.password))
        print("Please correct your Qumulo credentials and try again.")
        sys.exit()

    revision_id = qs.rc.version.version()['revision_id']
    qs.current_version = version_short(revision_id.replace("Qumulo Core ", ""))
    ####  Make sure our first install build version is greater than the current
    if version_num(qs.current_version) >= version_num(qs.to_version):
        log_print("!! Error !! Unable to upgrade")
        err_msg = "Can't upgrade to %s as you're " + \
                "already on or past that release (%s)."
        print(err_msg % qs.to_version, qs.current_version)
        sys.exit()
    log_print("Current Qumulo Core version     : %s" % qs.current_version)
    log_print("Upgrading to Qumulo Core version: %s" % qs.to_version)

    if qs.sharepass is not None:
        download_from_trends(qs)
    elif qs.download_only:
        print("Please specify the --sharepass argument and value.")
        print("If you don't have the fileserver download password, please contact %s" % QUMULO_SUPPORT_EMAIL)
        sys.exit()


    if qs.download_only:
        print("Exiting before upgrade as --download-only was specified")
        sys.exit()

    for vers in get_upgrade_list(qs):
        connected = False
        tries = 0
        while not connected and tries < 6:
            try:
                qs.rc = RestClient(qs.host, 8000)
                qs.rc.login(qs.user, qs.password)
                connected = True
            except:
                exc = sys.exc_info()[1]
                log_print("Qumulo API exception: %s" % exc)
                log_print("Retrying in 10 seconds: %s" % exc)
                time.sleep(10)
            tries += 1
        if not connected:
            log_print("Qumulo API exception: Unable to login to Qumulo cluster %s" % qs.host)

        qimg = 'qumulo_core_%s.qimg' % vers['version']
        log_print("Upgrading to: %s" % vers['version'])
        qimg_path = '/' + qs.upgrade_path + '/' + qimg
        file_exists = False
        try:
            attrs = qs.rc.fs.get_attr(path = qimg_path)
            if int(attrs['size']) == vers["size"]:
                file_exists = True
            else:
                log_print("Upgrade image %s not fully downloaded." % qimg_path)
        except:
            log_print("Upgrade image %s not found." % qimg_path)
        if not file_exists:
            print("Unable to upgrade.")
            sys.exit()

        resp = qs.rc.upgrade.status_get()
        if resp['error_state'] != 'UPGRADE_ERROR_NO_ERROR':
            log_print("!Fatal Error! " + resp['error_state'])
            print("Please contact %s" % QUMULO_SUPPORT_EMAIL)
            sys.exit()
        log_print("Upgrading cluster with: %s" % qimg_path)
        log_print("Upgrade PREPARE: %s" % vers['version'])
        try:
            qs.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_PREPARE')
        except:
            exc = sys.exc_info()[1]
            log_print("!Fatal Error! Prepare exception: %s" % exc)
            print("Please contact %s" % QUMULO_SUPPORT_EMAIL)
            sys.exit()

        resp = qs.rc.upgrade.status_get()
        upgrade_state = resp['state']
        log_print("Wait for PREPARE of %s - typically takes about a minute." % \
                                                vers['version'])
        while upgrade_state == 'UPGRADE_PREPARING':
            resp = qs.rc.upgrade.status_get()
            log_print("... %s for %s ..." % (resp['state'],
                                                vers['version']))
            upgrade_state = resp['state']
            if upgrade_state == 'UPGRADE_PREPARED':
                break
            time.sleep(15)
        if upgrade_state == 'UPGRADE_PREPARED':
            log_print("Upgrade ARM %s - typically takes a minute or two." % \
                                                    vers['version'])
            resp = qs.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_ARM')
        else:
            log_print("!Fatal Error! The upgrade state is currently " + \
                            "unknown. Unable to arm.")
            print("Please contact %s" % QUMULO_SUPPORT_EMAIL)
            sys.exit()

        err_msg = "Qumulo cluster ARMed with %s. Reloading kernel via " + \
                        "kexec. Takes about a minute."
        log_print(err_msg % vers['version'])
        time.sleep(10)
        version_data = None
        while version_data == None:
            log_print("... Loading Qumulo Core %s ..." % \
                                                vers['version'])
            try:
                ####  10 second timeout for rest client while waiting.
                qs.rc = RestClient(qs.host, 8000, timeout=10)
                qs.rc.login(qs.user, qs.password)
                version_data = qs.rc.version.version()
                version_data["revision_id"] = version_short(version_data["revision_id"])
            except:
                time.sleep(14)
        err_msg = "Completed upgrade to %(revision_id)s, " + \
                    "build: %(build_id)s"
        log_print(err_msg % version_data)
        log_print("-" * 40)


if __name__ == "__main__":
    upgrade_cluster()

