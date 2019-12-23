import json
import sys
import re
import os
import time
import math
import requests
import argparse
from collections import OrderedDict

try:
    import requests
    from qumulo.rest_client import RestClient
except:
    print("Unable to import requests and Qumulo api bindings")
    print("Please run the following command:")
    print("pip install qumulo_api requests")
    sys.exit()

TRENDS_DOMAIN = "https://trends.qumulo.com"
UPGRADE_PATH = "/upgrade"


def log_print(msg):
    print("%s | %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))


def get_version_num(vers, release_only = False):
    parts = vers.split('.')
    if len(parts) == 3:
        parts.append('0')
    num = int(parts[0]) * 1000000 + int(parts[1]) * 10000 + int(parts[2]) * 100
    if not release_only:
        num += int(parts[3])
    return num

def download_file(qimg, sharepass):
    log_print("Starting download of qimg file: %s" % qimg)
    rsp = requests.get(TRENDS_DOMAIN + "/data/download/%s?access_code=%s" % \
                    (qimg, sharepass), allow_redirects=False)
    if rsp.status_code == 404:
        log_print("Unable to download qimg file. Please check the --sharepass password.")
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


class qumulo_release_mgr:

    def __init__(self):
        self.release_list = OrderedDict()
        self.final_release_list = []
        self.release_id = 0
        self.release_num = 0

        r = requests.get(TRENDS_DOMAIN + "/data/qimg_versions/")
        releases = json.loads(r.text)
        sorted_releases = sorted(filter(lambda d: d['is_main_release'] == '1', releases), key = lambda d: int(d['release_num']))
        sorted_quarterly_releases = sorted(filter(lambda d: d['is_main_release'] == '1' and self.is_quarterly(d['full_release']), releases), key = lambda d: int(d['release_num']))
        for rel in sorted_releases:
            self.release_list[get_version_num(rel['main_release'])] = rel
        self.valid_main_releases = {}
        self.valid_releases = {}
        for rel in releases:
            if rel['is_main_release'] == '1':
                self.valid_main_releases[rel['main_release']] = rel
        for rel in releases:
            self.valid_releases[rel['full_release']] = rel
        self.latest_release = sorted_releases[-1]
        self.latest_quarterly_release = sorted_quarterly_releases[-1]

    def is_quarterly(self, vers):
        vers_num = get_version_num(vers)
        return ((vers_num / 100) % 100 == 0) and vers_num >= 2090000
    
    def get_next_q(self, vers):
        vers_num = get_version_num(vers, True)
        in_list = False
        if vers_num < 2090000:
            return None
        for k, rel in self.release_list.items():
            if in_list and self.is_quarterly(rel['main_release']):
                return k
            elif get_version_num(rel['main_release'], True) > vers_num:
                in_list = True

    def get_qimg_list(self, release_list, is_hpe, is_cloud):
        the_list = []
        for i, release in enumerate(release_list):
            prefix = ""
            qimg_size = int(self.release_list[release]["qimg_size"])
            if is_cloud:
                prefix = "cloud_"
                qimg_size = int(self.release_list[release]["qimg_size_cloud"])
            if is_hpe and self.release_list[release]["qimg_size_hpe"] != "":
                prefix = "hpe_"
                qimg_size = int(self.release_list[release]["qimg_size_hpe"])
            the_list.append({"release": self.release_list[release]['full_release']
                            , "qimg": "qumulo_install_%s%s.qimg" % (prefix, self.release_list[release]['full_release'])
                            , "size": qimg_size
                            })
        return the_list
        
    def get_path(self, start, end, is_hpe = False, is_cloud = False):
        start_num = get_version_num(start, True)
        end_num = get_version_num(end)
        release_short_list = OrderedDict()
        for k in self.release_list:
            if int(k) >= start_num and int(k) <= end_num:
                release_short_list[k] = self.release_list[k]
        final_list = []
        is_first = True
        between_qs = False
        skipto = None
        for k, rel in release_short_list.items():
            if skipto:
                if get_version_num(rel['main_release']) >= skipto:
                    skipto = None
            if self.is_quarterly(rel['main_release']):
                if self.get_next_q(rel['main_release']) <= end_num and self.get_next_q(rel['main_release']) is not None:
                    between_qs = True
                else:
                    between_qs = False
                if not is_first:
                    final_list.append(k)
            elif between_qs:
                pass
            elif skipto:
                pass
            elif not is_first:
                final_list.append(k)
                # handle a few custom skips
                if rel['skipto'] != '' and get_version_num(rel['skipto']) <= end_num:
                    final_list.append(get_version_num(rel['skipto']))
                    skipto = get_version_num(rel['skipto'])
            is_first = False

        # handle jumping off and jumping back on.
        final_final_list = []
        for i, k in enumerate(final_list):
            if k > 2130000:
                if i == len(final_list) - 1:
                    final_final_list.append(k)
                elif self.is_quarterly(self.release_list[k]['main_release']):
                    final_final_list.append(k)
            else:
                final_final_list.append(k)
        self.final_release_list = self.get_qimg_list(final_final_list, is_hpe, is_cloud)

    def print_qimg_list(self):
        results = []
        for i, d in enumerate(self.final_release_list):
            line = "%2s: %9s  |  qimg: %s" % (i + 1, d['release'], d['qimg'])
            log_print(line)
            results.append(line)
        return results

    def install_upgrades(self, api):
        for i, d in enumerate(self.final_release_list):
            log_print("upgrade to: %s with qimg: %s" % (d['release'], d['qimg']))
            api.upgrade_to(d['release'], "%s/%s" % (UPGRADE_PATH, d['qimg']))
            
    def download_qimgs(self, api, sharepass):
        for d in self.final_release_list:
            log_print("Downloading: %s" % d['qimg'])
            file_exists_on_cluster = api.file_exists("%s/%s" % (UPGRADE_PATH, d['qimg']), d["size"])
            file_exists_local = os.path.exists(d['qimg'])
            if file_exists_local and os.path.getsize(d['qimg']) != d["size"]:
                file_exists_local = False
            if file_exists_on_cluster:
                log_print("qimg already on cluster: %s" % (UPGRADE_PATH + "/" + d['qimg']))
                continue
            elif file_exists_local and not file_exists_on_cluster:
                log_print("qimg still needs to be copied to cluster: %s" % (UPGRADE_PATH + "/" + d['qimg']))
                pass
            elif not file_exists_local and not file_exists_on_cluster:
                download_file(d['qimg'], sharepass)
            
            try:
                api.rc.fs.create_file(dir_path=UPGRADE_PATH, name=d['qimg'])
            except:
                pass
            log_print("Load qimg file onto Qumulo Cluster via API: %s" % d['qimg'])
            with open(d['qimg'], 'rb') as fr:
                api.rc.fs.write_file(path = '%s/%s' % (
                                                UPGRADE_PATH,
                                                d['qimg']), 
                                            data_file=fr)
            log_print("qimg loaded: %s/%s" % (UPGRADE_PATH, d['qimg']))
            os.remove(d['qimg'])
            log_print("delete local qimg: %s" % d['qimg'])
    
    def is_valid_release(self, version):
        if version in self.valid_releases:
            return True
        return False
                
    def upgrade_cluster(self, to_version, api, sharepass, download_only = False):
        if to_version == "latest":
            to_version = self.latest_release
        elif to_version[0:8] == "latest_q":
            to_version = self.latest_quarterly_release
        elif to_version in self.valid_main_releases:
            to_version = self.valid_main_releases[to_version]
        elif to_version in self.valid_releases:
            to_version = self.valid_releases[to_version]
        else:
            log_print("'%s' is not a valid Qumulo Core version" % args.vers)
            log_print("Exiting")
            sys.exit()
        if get_version_num(api.get_current_version()) >= get_version_num(to_version["full_release"]):
            log_print("Current cluster version >= version specified")
            log_print("Cluster %s >= %s specified" % (api.get_current_version(), to_version["full_release"]))
            log_print("Exiting")
            sys.exit()
        log_print("Upgrading from: %s" % api.get_current_version())
        log_print("Upgrading to:   %s" % to_version["full_release"])
        self.get_path(api.get_current_version(), 
                      to_version['full_release'],
                      is_hpe = True if api.get_platform() == 'hpe' else False, 
                      is_cloud = True if api.get_platform() == 'cloud' else False)
        log_print("The upgrade steps will include:")
        self.print_qimg_list()
        if not api.file_exists(UPGRADE_PATH):
            api.create_directory(UPGRADE_PATH)
        log_print("Begin download of qimg(s) to Qumulo cluster")
        self.download_qimgs(api, sharepass)
        log_print("Completed download of qimg(s) to Qumulo cluster")
        if download_only:
            log_print("Exiting because download only was specified")
            log_print("If you run without --download-only, the following will be installed: ")
            self.print_qimg_list()
            return
        self.install_upgrades(api)


class qumulo_api:
    def __init__(self):
        self.rc = None
        self.creds = None

    def login(self):
        self.rc = RestClient(self.host, 8000)
        self.rc.login(self.user, self.password)

    def test_login(self, host, user, password):
        log_print("Logging into Qumulo Cluster [%s]" % host)
        try:
            self.host = host
            self.user = user
            self.password = password
            self.login()
            log_print("Login succesful")
        except:
            log_print("Unable to connect to Qumulo Cluster %s via api" % host)
            log_print("Credentials used: username=%s, password=********" % user)

    def get_current_version(self):
        revision_id = self.rc.version.version()['revision_id']
        self.version = revision_id.replace("Qumulo Core ", "")
        return self.version

    def get_platform(self):
        model_num = self.rc.cluster.list_node(1)["model_number"]
        if 'aws' in model_num:
            self.platform = 'cloud'
        elif 'gcp' in model_num:
            self.platform = 'cloud'
        elif 'hp' in model_num:
            self.platform = 'hpe'
        else:
            self.platform = 'qumulo'
        return self.platform

    def create_directory(self, full_path):
        m = re.match(r'^(.*?/)([^/]+)$', full_path)
        dir_path = m.groups()[0]
        name = m.groups()[1]
        if len(dir_path) > 1:
            dir_path = re.sub(r'/$', '', dir_path)
        if self.file_exists(full_path):
            log_print("Directory exists: %s" % full_path)
        else:
            try:
                self.rc.fs.create_directory(name=name, dir_path=dir_path)
            except:
                e = sys.exc_info()[1]
                log_print("Error creating directory '%s': %s" % (full_path, e))
    
    def file_exists(self, full_path, size = None):
        try:
            attr = self.rc.fs.get_attr(full_path)
            if size and int(attr["size"]) != size:
                log_print("File sizes different")
                log_print("Expected size: %s - size on Qumulo: %s" % (size, attr["size"]))
                return False
        except:
            return False
        return True

    def upgrade_arm(self, qimg_path):
        resp = self.rc.upgrade.status_get()
        if resp['state'] != 'UPGRADE_PREPARED':
            log_print("Can't arm in state: %s" % resp['state'])
            sys.exit()
        log_print("Begin upgrade arm process.")
        resp = self.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_ARM')
        msg = "Qumulo cluster armed with %s. Reloading Qumulo."
        log_print(msg % qimg_path)
        time.sleep(10)
        version_data = None
        while version_data == None:
            log_print("... Loading new Qumulo Software version: %s ..." %  qimg_path)
            try:
                ####  10 second timeout for rest client while waiting.
                self.login()
                version_data = self.get_current_version()
            except:
                time.sleep(17)
        err_msg = "Completed upgrade to %s"
        log_print(err_msg % version_data)
        log_print("-" * 80)

    def upgrade_prepare(self, version, qimg_path):
        log_print("Preparing cluster for upgrade. Cluster will be fully available during this time.")
        try:
            self.rc.upgrade.config_put(qimg_path, 'UPGRADE_TARGET_PREPARE', override_version=True)
        except:
            exc = sys.exc_info()[1]
            log_print("!Fatal Error! Prepare exception: %s" % exc)
            sys.exit()
        resp = self.rc.upgrade.status_get()
        while resp['state'] == 'UPGRADE_PREPARING':
            log_print("Preparing...")
            time.sleep(17)
            resp = self.rc.upgrade.status_get()
        log_print("Upgrade prepared for: %s - status: %s" % (qimg_path, resp['state'] ))

    def upgrade_to(self, version, qimg_path):
        resp = self.rc.upgrade.status_get()
        if resp['state'] == 'UPGRADE_PREPARED':
            self.upgrade_arm(qimg_path)
            return
        if resp['state'] != 'UPGRADE_IDLE':
            log_print("%(state)s - %(error_state)s" % resp)
            if 'error_message' in resp:
                log_print("%s" % resp['error_message'].strip())
            if 'is_blocked' in resp and resp['is_blocked']:
                log_print("Upgrade blocked: %(blocked_reason)s" % resp)
            sys.exit()
        self.upgrade_prepare(version, qimg_path)
        self.upgrade_arm(qimg_path)


def upgrade_cluster():
    parser = argparse.ArgumentParser()
    parser.add_argument('--qhost', required=True, help='Qumulo hostname or ip address')
    parser.add_argument('--quser', required=True, help='Qumulo API user')
    parser.add_argument('--qpass', required=True, help='Qumulo API password')
    parser.add_argument('--sharepass', help='Fileserver download password. Contact Qumulo for details')
    parser.add_argument('--vers', required=True, help='The Qumulo Core version to upgrade to. Valid values include: a version number (2.10.0), "latest" and "latest_quarterly"')
    parser.add_argument('--download-only', default=False, help='Do not perform upgrades, Only download qimg files from fileserver', action='store_true')
    args = parser.parse_args()

    qr = qumulo_release_mgr()
    api = qumulo_api()
    api.test_login(args.qhost, args.quser, args.qpass)
    qr.upgrade_cluster(args.vers, api, sharepass=args.sharepass, download_only = args.download_only)


if __name__ == "__main__":
    upgrade_cluster()