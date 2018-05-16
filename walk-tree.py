import os
import re
import sys
import ujson
import requests
import urllib
import time
import random
import argparse
import multiprocessing
from qumulo.rest_client import RestClient

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

# paralleliziation across nodes didn't help as this was client-bound
CLUSTER = ""
API_USER = ""
API_PASSWORD = ""
START_DIR = "/"
CLUSTER_IPS = []
OUT_FW = open("file-tree-output.txt", "wb")


def worker(q, val, lock, inode_count, dir_count, cluster_ips, api_user, api_pass):
    ip = random.choice(cluster_ips)
    rc = RestClient(ip, 8000)
    creds = rc.login(api_user, api_pass)
    ses = requests.Session()
    headers = {"Authorization": "Bearer %s" % str(creds.bearer_token)}
    ses.headers.update(headers)

    while True:
        item = q.get(True)
        ret_data = read_dir(ip, ses, q, val, lock, item)
        with lock:
            val.value -= 1
            inode_count.value += ret_data["inode_count"]
            dir_count.value += 1
            OUT_FW.write('\n'.join(ret_data["rows"]))
        time.sleep(1)

def read_dir(ip, ses, q, val, lock, path):
    inode_count = 0
    url = 'https://%s:8000/v1/files/%s/entries/?limit=1000000' % (ip, urllib.quote_plus(path))
    resp = ses.get(url, verify=False)
    items = ujson.loads(resp.text)['files']
    rows = []
    for d in items:
        inode_count += 1
        # Attributes that might be interesting:
        #                  id: 575005817
        #                name: geos
        #                path: /geos/
        #                size: 512
        #               owner: 12884901888
        #               group: 17179869184
        #                type: FS_FILE_TYPE_DIRECTORY
        # symlink_target_type: FS_FILE_TYPE_UNKNOWN
        #       creation_time: 2017-11-17T04:38:22.19858697Z
        #   modification_time: 2017-11-17T05:13:59.59775965Z
        #         change_time: 2017-11-17T05:13:59.59775965Z
        #                mode: 0755
        #       owner_details: {u'id_type': u'NFS_UID', u'id_value': u'0'}
        #       group_details: {u'id_type': u'NFS_GID', u'id_value': u'0'}
        # extended_attributes: {u'read_only': False, u'temporary': False, u'system': False, u'compressed': False, u'not_content_indexed': False, u'hidden': False, u'archive': False}
        #              blocks: 1
        #          metablocks: 1
        #          datablocks: 0
        #           num_links: 2
        #         child_count: 1
        row = "%(path)s\t%(size)s" % d
        rows.append(row.encode("UTF-8"))
        if d['type'] == "FS_FILE_TYPE_DIRECTORY":
            add_to_q(q, val, lock, d['id'])
    return {"inode_count": inode_count, "rows": rows}


def add_to_q(q, val, lock, item):
    q.put(item)
    with lock:
        val.value += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True, help='Qumulo host')
    parser.add_argument('--user', required=True, help='Qumulo api user')
    parser.add_argument('--password', required=True, help='Qumulo api password')
    parser.add_argument('--dir', required=False, help='Starting directory', default=START_DIR)

    args = parser.parse_args()
    CLUSTER = args.host
    API_USER = args.user
    API_PASSWORD = args.password

    start_time = time.time()
    q = multiprocessing.Queue()
    q_len = multiprocessing.Value('i', 0)
    q_lock = multiprocessing.Lock()

    rc = RestClient(CLUSTER, 8000)
    creds = rc.login(API_USER, API_PASSWORD)
    for d in rc.cluster.list_nodes():
        c = rc.network.get_network_status_v2(1, d['id'])
        if len(c['network_statuses'][0]['floating_addresses']) > 0:
            CLUSTER_IPS.append(c['network_statuses'][0]['floating_addresses'][0])
        else:
            CLUSTER_IPS.append(c['network_statuses'][0]['address'])

    start_time = time.time()
    inode_count = multiprocessing.Value('i', 0)
    dir_count = multiprocessing.Value('i', 0)
    last_inode_count = inode_count.value
    last_dir_count = dir_count.value 

    pool = multiprocessing.Pool(40, worker, (q, q_len, q_lock, inode_count, dir_count, CLUSTER_IPS, API_USER, API_PASSWORD))

    add_to_q(q, q_len, q_lock, args.dir)

    sleep_time = 8
    while q_len.value > 0:
        print("Inodes processed: %10s (%s / sec)   Dirs / sec: %s" % (
                                                inode_count.value
                                                , int((inode_count.value - last_inode_count) / sleep_time)
                                                , int((dir_count.value - last_dir_count) / sleep_time)
                                                ))
        sys.stdout.flush()
        last_inode_count = inode_count.value
        last_dir_count = dir_count.value
        time.sleep(sleep_time)

    total_time = time.time() - start_time
    OUT_FW.close()
    print("processed %s inodes per second" % (int(inode_count.value / total_time),))
    print("processed %s dirs per second" % (int(dir_count.value / total_time),))
