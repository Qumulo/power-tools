import os
import sys
import glob
import time
import argparse
import pprint
import datetime
import multiprocessing
if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding('utf8')
from qumulo.rest_client import RestClient


class Gvars:
    def __init__(self, h, u, p):
        self.QHOST = h
        self.QUSER = u
        self.QPASS = p
        self.the_queue = multiprocessing.Queue()
        self.the_queue_len = multiprocessing.Value('i', 0)
        self.done_queue_len = multiprocessing.Value('i', 0)


def log(msg):
    t = datetime.datetime.utcnow()
    print("%s - %s" % (t.strftime('%Y-%m-%dT%H:%M:%SZ'), msg))


def add_to_queue(d):
    global gvars
    with gvars.the_queue_len.get_lock():
        gvars.the_queue_len.value += 1
    gvars.the_queue.put(d)


def list_dir(rc, d, out_file):
    global gvars
    next_page = "first"
    while next_page != "":
        if next_page == "first":
            try:
                r = rc.fs.read_directory(path=d["path"], page_size=1000)
            except:
                log("Error reading directory: %s" % d["path"])
                next
        else:
            r = rc.request("GET", r['paging']['next'])
        next_page = r['paging']['next']
        for ent in r["files"]:
            with gvars.done_queue_len.get_lock():
                gvars.done_queue_len.value += 1
            # all potential properties and sample date that are part of the *ent* object to search/track/filter
            """
             'path': '/tweets/snow/tweets-snow-2019011902.jsonline',
             'name': 'tweets-snow-2019011902.jsonline',
             'change_time': '2019-01-19T10:55:47.591487372Z',
             'creation_time': '2019-01-19T09:56:01.10147995Z',
             'modification_time': '2019-01-19T10:55:47.591487372Z',
             'child_count': 0,
             'extended_attributes': {'archive': True,
                                     'compressed': False,
                                     'hidden': False,
                                     'not_content_indexed': False,
                                     'read_only': False,
                                     'system': False,
                                     'temporary': False},
             'id': '11041240942',
             'group': '17179869184',
             'group_details': {'id_type': 'NFS_GID', 'id_value': '0'},
             'owner': '12884901888',
             'owner_details': {'id_type': 'NFS_UID', 'id_value': '0'},
             'mode': '0644',
             'num_links': 1,
             'size': '34342740',
             'symlink_target_type': 'FS_FILE_TYPE_UNKNOWN',
             'type': 'FS_FILE_TYPE_FILE'
            """
            # you might set some sort of conditional filter here.
            # you might even run an api command to set permissions here.
            out_file.write("%s\t%s\t%s\t%s\n" % (d["path"], ent["name"], ent["size"], ent["type"]))
            if ent["type"] == "FS_FILE_TYPE_DIRECTORY" and int(ent["child_count"]) > 0:
                add_to_queue({"path": d["path"] + ent["name"] + "/", "max_depth": d["max_depth"]})


def worker_main():
    global gvars
    proc = multiprocessing.current_process()
    rc = RestClient(gvars.QHOST, 8000)
    rc.login(gvars.QUSER, gvars.QPASS)
    out_file = open("out-%s.txt" % proc.pid, "w")
    while True:
        item = gvars.the_queue.get(True)
        list_dir(rc, item, out_file)
        out_file.flush()
        with gvars.the_queue_len.get_lock():
            gvars.the_queue_len.value -= 1


def walk_tree(QHOST, QUSER, QPASS, start_path):
    global gvars
    if start_path[-1] != "/":
        start_path = start_path + "/"
    log("Tree walk on Qumulo cluster %s starting at path %s" % (QHOST, start_path))
    gvars = Gvars(QHOST, QUSER, QPASS)
    the_pool = multiprocessing.Pool(16, worker_main)
    rc = RestClient(gvars.QHOST, 8000)
    rc.login(gvars.QUSER, gvars.QPASS)
    root = rc.fs.read_dir_aggregates(path=start_path, max_depth=0)
    log("Directories to walk: %12s" % "{:,}".format(int(root["total_directories"])))
    log("      Files to walk: %12s" % "{:,}".format(int(root["total_files"])))
    add_to_queue({"path": start_path, "max_depth": 5})
    time.sleep(0.1) # wait a bit for the queue to get build up.
    wait_count = 0
    while gvars.the_queue_len.value > 0:
        wait_count += 1
        if (wait_count % 50) == 0: # show status every ~5 seconds
            log("Processed %s entries. Queue length: %s" % (gvars.done_queue_len.value, gvars.the_queue_len.value))
        time.sleep(0.1)
    the_pool.terminate()
    log("Processed %s entries." % gvars.done_queue_len.value)
    log("Done with tree walk. Combining results")
    fw = open("file-list.txt", "w")
    for f in glob.glob('out-*.txt'):
        fr = open(f, "r")
        fw.write(fr.read())
        fr.close()
        os.remove(f)
    del gvars
    log("Results save to file: file-list.txt")


if __name__ == '__main__':
    usage_msg = "\nExample: python api-tree-walk.py -s qumulo -p password123 -d /home/\nSpecify -h for list of arguments."
    parser = argparse.ArgumentParser(description='Recursive parallel tree walk with Qumulo API', usage=usage_msg)
    parser.add_argument('-s', required=True, help='Qumulo cluster ip/hostname')
    parser.add_argument('-p', required=True, help='Qumulo api *admin* password')
    parser.add_argument('-d', required=False, help='Starting directory', default='/')
    args = parser.parse_args()
    r = walk_tree(args.s, "admin", args.p, args.d)
