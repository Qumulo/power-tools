### Qumulo power tool requirements and setup
* python 2.7
* `git clone https://github.com/Qumulo/power-tools`
* `cd power-tools`


## Walk a Qumulo file system via the API

This python script uses the Qumulo api to quickly walk a Qumulo filesystem tree in a parallel manner. The output is a single file with a list of every file and directory as well as its size. You can look in the script to see other available file metadata you might wish to search/track.

Performance results: 
* 3,000+ files per second
* 100+ directories per second
Using Qumulo 4 node QC24 cluster with a Mac 2.3 GHz Intel i7 8-core CPU.

### Usage
1. Make sure you have the python requirements: `pip install -r requirements.txt`
2. `python api-tree-walk.py -s product -p ********* -d /home`


## Upgrade a Qumulo cluster

* This script downloads Qumulo software, loads it onto a Qumulo cluster, and upgrades the Qumulo cluster via the API.
* Script must be run from a MacOSX or linux client machine. It cannot be run directly on a Qumulo cluster due to reboots.
* You can upgrade to a specified build number, or the latest build, or the latest quarterly build. Specifically one of the following examples:
  * --vers 2.10.0
  * --vers latest
  * --vers latest_quarter

#### Recommended usage

1. Step 1: Prepare for an upgrade by *only* downloading the qimg(s) to the Qumulo cluster

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers latest --download-only`

2. Step 2: Upgrade the latest build

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers latest`

!Note! - If you want to only upgrade to our latest quarterly release please specify the following:

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers latest_quarterly`

#### A few other examples

Upgrade from an older build to a not-quite latest build (2.9.0)

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.9.0`

Upgrade to 2.9.0 without downloading, assuming you already have the qimg file on Qumulo and properly named

`python qupgrade.py --qhost product --quser admin --qpass secret --vers 2.9.0`


## Add Qumulo activity to various databases from the API

We know you're excited to get your Qumulo API data into your centralized databases and monitoring systems. Use this script to send activity (throughput, data and metadata IOPS) by path and client into influx, elastic search, postgres, splunk, and/or csv.

1. Install the python 2.7 prequisites. `pip install qumulo_api`
2. Copy `sample-config.json` to `config.json` and then specify your databases and Qumulo clusters you wish to use in the new `config.json` file.
3. run `python api-to-dbs.py` to confirm everything works.
4. Add the `python api-to-dbs.py` command to your crontab to run every 1 or two minutes with something like:

`* * * * * cd /location/of/the-power-tools; python api-to-dbs.py >> api-to-dbs.log.txt 2>&1`


#### config.json details

The `"QUMULO_CLUSTERS"` section allows for multiple Qumulo clusters to be tracked.
```json
    "QUMULO_CLUSTERS":[
```

`sample-config.json` is currently only set up to save data to hourly csv files. If you want to send Qumulo data to other databases, here is the json configurations you can add to your `config.json`:

```json
  "host": "product.example.com",
  "user": "admin",
  "password": ""
  "client_ip_regex": "10.0.0.*"
```
host (required): Hostname or IP of the Qumulo cluster API server

user (required): Username of account with API access

password (required): Password for API user

client_ip_regex (optional): a regex based filter for the clients accessing the Qumulo
cluster. This allows for cleaning up metrics for only those clients that you
are interested in storing metrics.

```json
"influx": {"host": "influxdb.example.com",
            "db": "qumulo",
            "measurement": "qumulo_fs_activity"
            },
"postgres": {"host": "db.example.com",
            "db":   "qumulo_data",
            "user": "postgres",
            "pass": ""
            },
"elastic": {"host": "elasticdb.example.com",
        "index": "qumulo",
        "type": "qumulo_fs_activity"
        },
"splunk": {"host": "elasticdb.example.com",
        "token": "2ea5c4dd-dc73-4b89-af26-2ea6026d0d39",
        "event": "qumulo_fs_activity"
},
```
For the various databases, you will need to have your own DB server set up to pass the data. You will also potentially need to create a new database withing the DB server. For splunk, you will need to enable the /services/collector/event in splunk and generate a token for the configuration file.


The configuration parameters are pretty self-explanatory and described below.

```json
"IOPS_THRESHOLD": 1,
"THROUGHPUT_THRESHOLD": 10000, 
"DIRECTORY_DEPTH_LIMIT": 4,
"DIRECTORIES_ONLY": true,
"DEBUG": true
```

- "IOPS_THRESHOLD"- the minimum total IOPS value per path+client required for being saved into the database
- "THROUGHPUT_THRESHOLD" - the minimum total throughput value per path+client required for being saved into - the database
- "DIRECTORY_DEPTH_LIMIT" - the maximum directory depth to be tracked in the paths
- "DIRECTORIES_ONLY" - only story directory names. if false then we go down to file names.
- "DEBUG" - verbose logging
