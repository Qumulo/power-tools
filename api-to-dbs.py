import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import re
import os
import sys
import json
import time
import requests
import datetime
import traceback
from multiprocessing import Pool
from qumulo.rest_client import RestClient
from collections import OrderedDict

def log(msg, override=False):
    if DEBUG:
        t = datetime.datetime.utcnow()
        print("%s - %s" % (t.strftime('%Y-%m-%dT%H:%M:%SZ'), msg))

class QumuloActivityData:

    def __init__(self, cluster, conf):
        self.DIRECTORY_DEPTH_LIMIT = conf['DIRECTORY_DEPTH_LIMIT']
        self.IOPS_THRESHOLD = conf['IOPS_THRESHOLD']
        self.THROUGHPUT_THRESHOLD = conf['THROUGHPUT_THRESHOLD']
        self.DIRECTORIES_ONLY = conf['DIRECTORIES_ONLY']

        # initial fields in the data dict
        # "qumulo_host"
        # "client_ip"
        # "client_host_name"
        # "path"
        # "path_levels"
        # "timestamp"

        # default dict for the metrics
        self.EMPTY_DATA = OrderedDict([
            ('file-throughput-write', 0.0),
            ('file-throughput-read', 0.0),
            ('file-iops-write', 0.0),
            ('file-iops-read', 0.0),
            ('metadata-iops-write', 0.0),
            ('metadata-iops-read', 0.0),
            ('iops-total', 0.0),
            ('throughput-total', 0.0),
        ])

        PG_SETUP_SQL = """CREATE TABLE qumulo_activity(
                        qumulo_host VARCHAR(128),
                        client_ip VARCHAR(20),
                        client_hostname VARCHAR(256),
                        path VARCHAR(2048),
                        path_levels SMALLINT,
                        ts TIMESTAMP,
                        file_throughput_write FLOAT,
                        file_throughput_read FLOAT,
                        file_iops_write FLOAT,
                        file_iops_read FLOAT,
                        metadata_iops_write FLOAT,
                        metadata_iops_read FLOAT,
                        iops_total FLOAT,
                        throughput_total FLOAT
                        );"""
        self.ids_to_paths = {}
        self.ips_to_hostnames = {}
        self.combined_data = {}
        self.new_db_entries = []

        self.pool = Pool(6)
        self.cluster = cluster

        log("Connect to Qumulo API for cluster: %s" % self.cluster['host'])
        self.qumulo_client = RestClient(self.cluster['host'], 8000)
        self.qumulo_client.login(self.cluster['user'], self.cluster['password']);

        log("Get current activity data from Qumulo API for %s" % self.cluster['host'])
        activity_data = self.qumulo_client.analytics.current_activity_get()
        self.current_timestamp = datetime.datetime.utcnow()
        self.current_epoch = int(time.time())
        self.entries = activity_data['entries']
        log("Successfully recieved %s activity entries." % len(self.entries))


    def resolve_paths_and_ips(self):
        batch_size = 1000
        ids = list(set([d['id'] for d in self.entries]))
        log("Resolving %s inode ids." % len(ids))
        for offset in range(0, len(ids), batch_size):
            resolve_data = self.qumulo_client.fs.resolve_paths(ids[offset:offset+batch_size])
            for id_path in resolve_data:
                self.ids_to_paths[id_path['id']] = {"path": id_path['path'], "is-dir": False}

        check_ids = []
        pool = Pool(processes=10)
        for file_id, data in self.ids_to_paths.iteritems():
            if data["path"] != "" and data["path"] != "/":
                check_ids.append(file_id)
                if len(check_ids) >= 100:
                    pool.apply_async(QumuloActivityData.ids_to_attrs, (self.cluster, check_ids), callback=self.done_ids_to_attrs)
                    check_ids = []
        pool.apply_async(QumuloActivityData.ids_to_attrs, (self.cluster, check_ids), callback=self.done_ids_to_attrs)
        pool.close()
        pool.join()
        log("Completed resolving of %s inode ids." % len(ids))

        ips = list(set([d['ip'] for d in self.entries]))
        log("Resolving %s ips." % len(ips))
        dd = self.qumulo_client.dns.resolve_ips_to_names(ips)
        for d in dd:
            self.ips_to_hostnames[d['ip_address']] = d['hostname']
        log("Done resolving %s ips." % len(ips))


    def aggregate_data(self):
        log("Aggregating data for %s entries" % len(self.entries))
        for d in self.entries:
            path_info = self.ids_to_paths[d['id']]
            if path_info["path"] == '':
                shorter_path = '/'
            else:
                shorter_path = '/'.join(path_info["path"].split('/')[0:self.DIRECTORY_DEPTH_LIMIT+1])
                if shorter_path[-1] == '/':
                    shorter_path = shorter_path[:-1]
            ip_and_path = d['ip'] + ':' + shorter_path
            if self.DIRECTORIES_ONLY and not path_info["is-dir"] and shorter_path != '/':        
                ip_and_path = re.sub(r'/[^/]+$', '', ip_and_path)
            if ip_and_path[-1] == ":":
                ip_and_path = ip_and_path + "/"
            if ip_and_path not in self.combined_data:
                self.combined_data[ip_and_path] = self.EMPTY_DATA.copy()
            self.combined_data[ip_and_path][d['type']] += d['rate']
            if 'iops' in d['type']:
                self.combined_data[ip_and_path]['iops-total'] += d['rate']
            if 'throughput' in d['type']:
                self.combined_data[ip_and_path]['throughput-total'] += d['rate']
        log("Aggregated to %s entries" % len(self.combined_data))


    def prepare_data_for_dbs(self):
        log("Preparing data to add into databases and filter by iops/throughput")
        for ip_and_path, data in self.combined_data.iteritems():
            if data['iops-total'] < self.IOPS_THRESHOLD and data['throughput-total'] < self.THROUGHPUT_THRESHOLD:
                continue
            (ip, path) = ip_and_path.split(':', 1)
            entry = OrderedDict([
                ("qumulo_host", self.cluster['host']),
                ("client_ip", ip),
                ("client_host_name", self.ips_to_hostnames[ip] if ip in self.ips_to_hostnames else ""),
                ("path", path),
                ("path_levels", len(re.findall("/", path)) if path != '/' else 0 ),
                ("timestamp", self.current_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')),
            ])
            for k, v in data.iteritems():
                entry[k.replace('-', '_')] = int(v)
            self.new_db_entries.append(entry)
        log("%s entries ready for import to database(s)" % len(self.new_db_entries))


    def load_data_into_csv(self, csv):
        new_file = False
        day = self.current_timestamp.strftime('%Y%m%d')
        hour = self.current_timestamp.strftime('%H')
        csv_dir = "%s/%s/%s" % (csv['directory']
                                , self.cluster['host']
                                , day)
        csv_path = "%s/activity-data-%s.csv" % (csv_dir, hour)
        if not os.path.exists(csv_dir):
            os.makedirs(csv_dir)
        log("Load %s entries into csv %s" % (len(self.new_db_entries), csv_path))
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            new_file = True
        fw = open(csv_path, "a")
        for d in self.new_db_entries:
            if new_file:
                fw.write(','.join(d.keys()) + "\n")
                new_file = False
            d['timestamp'] = d['timestamp'].replace('T', ' ').replace('Z', '')
            fw.write(','.join([str(v) for v in d.values()]) + "\n")
        fw.close()
        log("Loaded %s entries into csv %s" % (len(self.new_db_entries), csv_path))


    def load_data_into_postgres(self, pgdb):
        log("Load %s entries into postgres [%s/%s]" % (len(self.new_db_entries), pgdb["host"], pgdb["db"]))
        pg_str = "host=%s dbname=%s user=%s password=%s"
        pg_cn = psycopg2.connect(pg_str % (pgdb["host"], pgdb["db"], pgdb["user"], pgdb["pass"]))
        file_name = "temp-import-file.txt"
        fw = open(file_name, "w")
        for d in self.new_db_entries:
            fw.write('|'.join([str(v) for v in d.values()]) + "\n")
        fw.close()
        cur = pg_cn.cursor()
        with open(file_name, 'r') as f:
            cur.copy_from(f, 'qumulo_activity', sep='|')
        pg_cn.commit()
        os.remove(file_name)
        log("Loaded %s entries into postgres [%s/%s]" % (len(self.new_db_entries), pgdb["host"], pgdb["db"]))


    def load_data_into_influxdb(self, influxdb):
        log("Load %s entries into influx [%s/%s]" % (len(self.new_db_entries), influxdb["host"], influxdb["db"]))
        influx_client = InfluxDBClient(host=influxdb["host"], database=influxdb["db"], port=8086)
        json_entries = []
        for d in self.new_db_entries:
            metrics_data = {}
            for k, v in d.iteritems():
                if k.replace('_', '-') in self.EMPTY_DATA:
                    metrics_data[k] = v
            entry = {
                "measurement": influxdb['measurement'],
                "tags": {
                    "qumulo_host": self.cluster['host'],
                    "client_ip": d["client_ip"],
                    "client_hostname": d["client_host_name"],
                    "path": d["path"],
                },
                "time": self.current_timestamp,
                "fields": metrics_data,
            }
            json_entries.append(entry)
        result = influx_client.write_points(json_entries)
        log("Influx import success: %s" % result)
        log("Loaded %s entries into influx [%s/%s]" % (len(self.new_db_entries), influxdb["host"], influxdb["db"]))


    def load_data_into_elastic_search(self, elast):
        log("Load %s entries into elastic [%s]" % (len(self.new_db_entries), elast["host"]))
        json_entries = []
        for d in self.new_db_entries:
            entry = OrderedDict([
                        ("_index", elast["index"]),
                        ("_type", elast["type"]),
                        ("qumulo_host", self.cluster['host']),
                        ("client_ip", d["client_ip"]),
                        ("client_hostname", d["client_host_name"]),
                        ("path", d["path"]),
                        ("timestamp", self.current_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')),
                    ])
            for k, v in d.iteritems():
                if k.replace('_', '-') in self.EMPTY_DATA:
                    entry[k] = v
            json_entries.append(entry)
        es_client = Elasticsearch([{'host': elast["host"], 'port': 9200}])
        result = helpers.bulk(es_client, json_entries)
        log("Elastic import reecord count %s, status: %s" % (result[0], result[1]))
        log("Loaded %s entries into elastic [%s]" % (len(self.new_db_entries), elast["host"]))


    def load_data_into_splunk(self, splk):
        log("Load %s entries into splunk [%s]" % (len(self.new_db_entries), splk["host"]))
        entries = []
        for d in self.new_db_entries:
            entry = OrderedDict([
                        ("time", self.current_epoch),
                        ("event", splk["event"]),
                        ("host", self.cluster['host']),
                        # ("index", splk['index']),
                        ("source", 'qumulo-api'),
                        ("sourcetype", 'storage-throughput-iops'),
                        ("fields", {
                            "client_ip": d["client_ip"],
                            "client_hostname": d["client_host_name"],
                            "path": d["path"],
                            }),
                    ])
            for k, v in d.iteritems():
                if k.replace('_', '-') in self.EMPTY_DATA:
                    entry['fields'][k] = v
            entries.append(str(json.dumps(entry)))
        entries = '\n'.join(entries)
        resp = requests.post('https://%s:8088/services/collector/event' % splk['host'], 
                        auth=('x', splk['token']), 
                        data=entries,
                        verify=False, )
        log("Splunk response: %s" % resp.text)
        log("Loaded %s entries into splunk [%s]" % (len(self.new_db_entries), splk["host"]))


    @staticmethod
    def ids_to_attrs(cluster, id_attr_list):
        inode_types = {}
        qumulo_client = RestClient(cluster['host'], 8000)
        qumulo_client.login(cluster['user'], cluster['password'])
        for inode_id in id_attr_list:
            try:
                attrs = qumulo_client.fs.get_attr(id_ = inode_id)
                if inode_id not in inode_types:
                    inode_types[inode_id] = attrs["type"]
            except:
                pass
        return inode_types


    def done_ids_to_attrs(resolved_ids):
        for inode_id, file_type in resolved_ids.iteritems():
            if file_type == 'FS_FILE_TYPE_DIRECTORY':
                self.ids_to_paths[inode_id]["is-dir"] = True



DEBUG = True

if __name__ == "__main__":
    config_file = "config.json"
    try:
        conf = json.loads(open(config_file, "r").read())
    except IOError, e:
        log("*** Error reading config file: %s" % config_file, True)
        log("*** Exception: %s" % e, True)
        sys.exit()
    except ValueError, e:
        log("*** Error parsing config file: %s" % config_file, True)
        log("*** Exception: %s" % e, True)
        sys.exit()

    QUMULO_CLUSTERS = conf['QUMULO_CLUSTERS']
    DBS = conf['DBS']
    if 'DEBUG' in conf:
        DEBUG = conf['DEBUG']
    for cluster in QUMULO_CLUSTERS:
        try:
            qad = QumuloActivityData(cluster, conf)
            qad.resolve_paths_and_ips()
            qad.aggregate_data()
            qad.prepare_data_for_dbs()
            if "influx" in DBS:
                from influxdb import InfluxDBClient
                qad.load_data_into_influxdb(DBS["influx"])
            if "csv" in DBS:
                qad.load_data_into_csv(DBS["csv"])
            if "postgres" in DBS:
                import psycopg2
                qad.load_data_into_postgres(DBS["postgres"])
            if "elastic" in DBS:
                from elasticsearch import Elasticsearch, helpers
                qad.load_data_into_elastic_search(DBS["elastic"])
            if "splunk" in DBS:
                qad.load_data_into_splunk(DBS["splunk"])
        except Exception, err:
            log("*** Exception ****", True)
            log(sys.exc_info()[0], True)
            log(sys.exc_info()[1], True)
            log(traceback.format_exc(), True)
