import os
import re
import sys
import time
# import ujson as json # might be faster in some cases
import json
import argparse
import urllib
import requests
import multiprocessing
from qumulo.rest_client import RestClient
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
reload(sys)
sys.setdefaultencoding('utf8')

from api_ops import set_permission

def get_client(config):
    rc = RestClient(config.s, 8000)
    creds = rc.login(config.u, config.p)
    ses = requests.Session()
    headers = {"Content-type": "application/json", 
                "Authorization": "Bearer %s" % str(creds.bearer_token)}
    ses.headers.update(headers)
    return ses


def get_files(api_type, config, ses, path):
    url = 'https://%s:8000/v1/files/%s/%s/?limit=1000000' % (
                config.s, 
                urllib.quote(path.encode('utf8')).replace("/", "%2F"),
                api_type)
    resp = ses.get(url, verify=False)
    return json.loads(resp.text)


def worker_main(config, queue, processed_files):
    ses = get_client(config)
    while True:
        item = queue.get(True)
        obj = get_files('entries', config, ses, item["path"])
        for d in obj["files"]:
            with processed_files.get_lock():
                processed_files.value += 1
            if d["type"] == "FS_FILE_TYPE_DIRECTORY":
                set_permission(config, ses, d["id"])
                pass
            elif d["type"] == "FS_FILE_TYPE_FILE":
                # if re.search(r'[.](mp3|mp4)$', d['name']):
                #     print(d['name'])
                pass
            elif d["type"] == "FS_FILE_TYPE_SYMLINK":
                pass
            else:
                pass
            if d["type"] == "FS_FILE_TYPE_DIRECTORY" and item["depth"] < config.l:
                queue.put({"path":d["id"], 
                            "full_path": item["path"] + d["name"] + "/",
                            "depth": item["depth"]+1})


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', required=True, help='Qumulo cluster ip/hostname')
    parser.add_argument('-u', required=True, help='Qumulo api user')
    parser.add_argument('-p', required=True, help='Qumulo api password')
    parser.add_argument('-l', required=False, help='Limit path depth to number', default=1, type=int)
    parser.add_argument('-d', required=False, help='Starting directory', default='/')
    args = parser.parse_args()

    start_dir = get_files('aggregates', args, get_client(args), args.d)
    print("Directories to walk: %12s" % "{:,}".format(int(start_dir["total_directories"])))
    print("      Files to walk: %12s" % "{:,}".format(int(start_dir["total_files"])))
    processed_files = multiprocessing.Value('i', 0)
    the_queue = multiprocessing.Queue()
    the_pool = multiprocessing.Pool(20, worker_main,(args, the_queue, processed_files))
    the_queue.put({"path":args.d, "full_path":args.d, "depth":0})
    time.sleep(3)
    while not the_queue.empty():
        with processed_files.get_lock():
            print("Processed files: %s at rate of %s/sec" % ("{:,}".format(processed_files.value), 
                                    "{:,}".format(int(processed_files.value / (time.time() - start_time)))))
        time.sleep(1)

    print("Completed the walk!")


if __name__== "__main__":
    main()

