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
# This is in here because ptyhon 2.7 struggles sometimes.
sys.setdefaultencoding('utf8')
# This is in here because MacOSX struggles sometimes.
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
try:
    import requests
    from qumulo.rest_client import RestClient
except:
    print("Unable to import requests and qumulo api bindings")
    print("Please run the following command:")
    print("pip install qumulo_api requests")
    sys.exit()
import argparse
import re
import os
import time
import json
from collections import OrderedDict


RELEASES_URL = 'https://qumulo.app.box.com/v/releases'


class QSettings(object):
    versions = None
    host = None
    user = None
    password = None
    upgrade_path = None
    box_password = None
    rc = None
    start_vers = None
    release_list = OrderedDict()


def version_num(vers):
    p1, p2, p3 = map(int, vers.split('.'))
    return p1 * 10000 + p2 * 100 + p3


def log_print(msg):
    print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))


def move_from_box_to_qumulo(qs):
    ####    Set up the Box download web client     ####
    log_print("Set up Box client")
    s = requests.Session()

    ####    Get list of releases folders from Box  ####
    log_print("Get list of releases from Box")
    rsp = s.get(RELEASES_URL)
    rsp = s.post(RELEASES_URL,
                data = {"password": qs.box_password})
    rx = r',"id":([0-9]+).*?Qumulo Core ([0-9.]+)'

    upgrade_verified = OrderedDict()
    for m in re.findall(rx, rsp.text):
        # Create a release list based on intersection of Box & user list.
        if m[1] in qs.release_list:
            upgrade_verified[str(m[1])] = 1
            qs.release_list[m[1]]["folder_id"] = m[0]
            qs.release_list[m[1]]["version_id"] = m[1]

    if(sorted(upgrade_verified.keys()) != sorted(qs.release_list.keys())):
        log_print("Desired upgrade list does not " \
                    "match up with available releases on Box.")
        log_print("Desired list: %s" % qs.release_list.keys())
        log_print("Box list:     %s" % upgrade_verified.keys())
        print("Please correct the upgrade version(s)")
        sys.exit()

    ####    Get qimg file names and ids from Box    ####
    for version_id, release in qs.release_list.iteritems():
        log_print("Get release details from Box for: %s" % version_id)
        folder_url = "https://qumulo.app.box.com/v/releases/folder/%s"
        rsp = s.get(folder_url % release['folder_id'])
        qimg_rx = r',"id":([0-9]+)[^\{\}]*?,"name":"([^\"]+.qimg)"'
        ms = re.findall(qimg_rx, rsp.text)
        m = ms[0]
        release['qimg'] = m[1].strip()
        release['file_id'] = m[0]

    #### create directory for upgrade qimgs ####
    try:
        qs.rc.fs.create_directory(name=qs.upgrade_path, dir_path='/')
    except:
        e = sys.exc_info()[1]
        log_print("Error creating directory '/%s' on %s. ** %s **" % (
                                                qs.upgrade_path,
                                                qs.host,
                                                e))

    ####  loop that downloads file from Box and pushed them to Qumulo
    for version_id, release in qs.release_list.iteritems():
        qumulo_qimg = '/' + qs.upgrade_path + '/' + release['qimg']
        #### check to see if qimg already exists on cluster ####
        file_exists = False
        try:
            attrs = qs.rc.fs.get_attr(path = qumulo_qimg)
            if int(attrs['size']) > 200000000:
                file_exists = True
        except:
            e = sys.exc_info()[1]

        if file_exists:
            log_print("qimg file is already uploaded: %s" % qumulo_qimg)
            continue

        log_print("Preparing to download release: %s" % release['version_id'])
        url = "https://qumulo.app.box.com/v/releases/file/%s" % \
                                                    release['file_id']
        resp = s.get(url)
        ms = re.findall("Box.config.requestToken = '([^']+)'", resp.text)
        post_data = {
            "fileIDs[]": release['file_id'],
            "vanityName": "releases",
            "request_token": ms[0]
        }
        log_print("Generate Box API token")
        url = "https://qumulo.app.box.com/index.php?rm=preview_create_tokens"
        rsp = s.post(url, data=post_data)
        js = json.loads(rsp.text)

        try:
            qs.rc.fs.create_file(dir_path='/' + qs.upgrade_path,
                                        name=release['qimg'])
        except:
            e = sys.exc_info()[1]
            log_print("File creation error: %s" % e)

        ####  Only download from Box if a local version of file doesn't exist.
        if not os.path.exists(release['qimg']):
            log_print("Begin download of qimg from Box for %s" % \
                                                    release['version_id'])
            headers = {"Authorization": "Bearer %s" % js[release['file_id']]}
            rsp = s.get("https://api.box.com/2.0/files/%s/content" % \
                            release['file_id'], headers = headers, stream=True)
            file_size = float(rsp.headers['Content-Length'])
            log_print("Downloading %s byte qimg from box." % file_size)
            status_size = 10.0
            done_count = 0
            percent_complete = [0]*int(status_size)
            downloaded_bytes = 0
            sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S") + ": ")
            sys.stdout.flush()
            with open(release['qimg'], 'wb') as fw:
                for chunk in rsp.iter_content(chunk_size=1024*1024):
                    if chunk: # filter out keep-alive new chunks
                        fw.write(chunk)
                        downloaded_bytes += 1024*1024
                        if done_count < status_size \
                            and downloaded_bytes / file_size > \
                                                done_count / status_size \
                            and percent_complete[done_count] == 0:
                            percent_complete[done_count] = 1
                            sys.stdout.write("%s%%  " % (done_count * 10, ))
                            sys.stdout.flush()
                            done_count += 1
            print("")

        log_print("Load qimg file onto Qumulo via API: %s" % release['qimg'])
        with open(release['qimg'], 'rb') as fr:
            qs.rc.fs.write_file(path = '/%s/%s' % (qs.upgrade_path,
                                            release['qimg']), data_file=fr)
        log_print("Upgrade file ready on Qumulo: %s" % release['qimg'])


def upgrade_cluster():
    qs = QSettings()
    parser = argparse.ArgumentParser()
    parser.add_argument('--qhost', required=True)
    parser.add_argument('--quser', required=True)
    parser.add_argument('--qpass', required=True)
    parser.add_argument('--qpath', default='upgrade')
    parser.add_argument('--sharepass')
    parser.add_argument('--vers', nargs="+", required=True)
    args = parser.parse_args()
    if ',' in args.vers[0]:
        args.vers = args.vers[0].split(',')
    for v in args.vers:
        if not re.match(r'^[0-9]+[.][0-9]+[.][0-9]+[A-Zz-z]*$', v):
            log_print("Exiting")
            print("'%s' is not a valid Qumulo version" % v)
            sys.exit()

    qs.versions       = args.vers
    qs.host           = args.qhost
    qs.user           = args.quser
    qs.password       = args.qpass
    qs.upgrade_path   = args.qpath
    qs.box_password   = args.sharepass

    for vers_id in sorted(qs.versions, key=lambda s: map(int, s.split('.'))):
        qs.release_list[vers_id] = {"version_id": vers_id,
                                    "qimg": "qumulo_core_%s.qimg" % vers_id,
                                    "version_num": version_num(vers_id)}

    ####   Set up the Qumulo REST client
    log_print("Logging into qumulo cluster to begin upgrade process")
    try:
        qs.rc = RestClient(qs.host, 8000)
        qs.rc.login(qs.user, qs.password)
        log_print("Login succesful")
    except:
        log_print("Unable to connect to Qumulo cluster via api")
        log_print("Qumulo cluster details: %s, login: %s" % (
                                                    qs.user, qs.password))
        print("Please correct your Qumulo credientials and try again.")
        sys.exit()

    revision_id = qs.rc.version.version()['revision_id']
    qs.start_vers = revision_id.replace("Qumulo Core ", "")
    ####  Make sure our first install build version is greater than the current
    if not version_num(qs.start_vers) < version_num(qs.release_list.keys()[0]):
        log_print("!! Error !! Unable to upgrade")
        err_msg = "Can't upgrade to %s as you're " + \
                "already on or past that release."
        print(err_msg % qs.release_list.keys()[0])
        sys.exit()
    log_print("Current Qumulo version: %s" % qs.start_vers)
    log_print("Upgading Qumulo through: %s -> %s" % (qs.start_vers,
                                                    ' -> '.join(qs.versions)))

    if qs.box_password is not None:
        move_from_box_to_qumulo(qs)

    ####  loop through releases and upgrade the cluster serially
    for vers_id, rel in qs.release_list.iteritems():
        ####   re-initialize client to reset timeout
        qs.rc = RestClient(qs.host, 8000)
        qs.rc.login(qs.user, qs.password)
        qimg_path = '/' + qs.upgrade_path + '/' + rel['qimg']
        file_exists = False
        try:
            attrs = qs.rc.fs.get_attr(path = qimg_path)
            if int(attrs['size']) > 200000000:
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
            print("Please contact care@qumulo.com")
            sys.exit()
        log_print("Upgrading cluster with: %s" % qimg_path)
        log_print("Upgrade PREPARE: %s" % rel['version_id'])
        try:
            qs.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_PREPARE')
        except:
            exc = sys.exc_info()[1]
            log_print("!Fatal Error! Prepare exception: %s" % exc)
            print("Please contact care@qumulo.com")
            sys.exit()

        resp = qs.rc.upgrade.status_get()
        upgrade_state = resp['state']
        log_print("Wait for PREPARE of %s - typically takes about a minute." % \
                                                rel['version_id'])
        while upgrade_state == 'UPGRADE_PREPARING':
            resp = qs.rc.upgrade.status_get()
            log_print("... %s for %s ..." % (resp['state'],
                                                rel['version_id']))
            upgrade_state = resp['state']
            if upgrade_state == 'UPGRADE_PREPARED':
                break
            time.sleep(15)
        if upgrade_state == 'UPGRADE_PREPARED':
            log_print("Upgrade ARM %s - typically takes a minute or two." % \
                                                    rel['version_id'])
            resp = qs.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_ARM')
        else:
            log_print("!Fatal Error! The upgrade state is currently " + \
                            "unknown. Unable to arm.")
            print("Please contact care@qumulo.com")
            sys.exit()

        err_msg = "Qumulo cluster ARMed with %s. Reloading kernel via " + \
                        "kexec. Takes about a minute."
        log_print(err_msg % rel['version_id'])
        time.sleep(10)
        version_data = None
        while version_data == None:
            log_print("... Loading Qumulo software: %s ..." % \
                                                rel['version_id'])
            try:
                ####  10 second timeout for rest client while waiting.
                qs.rc = RestClient(qs.host, 8000, timeout=10)
                qs.rc.login(qs.user, qs.password)
                version_data = qs.rc.version.version()
            except:
                time.sleep(14)
        err_msg = "Completed upgrade to %(revision_id)s, " + \
                    "build: %(build_id)s"
        log_print(err_msg % version_data)
        log_print("-" * 40)


if __name__ == "__main__":
    upgrade_cluster()

