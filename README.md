### Qumulo power tool requirements and setup
* python 2.7
* `git clone https://github.com/Qumulo/power-tools`
* `cd power-tools`
* `pip install -r requirements.txt`


## Walk a Qumulo file system via the API

This python script uses the Qumulo api and the UltraJson library to quickly walk a Qumulo filesystem tree in a parallel manner. The output is a single file with a list of every file and directory as well as its size. You can look in the file to see all the available file metadata.

Performance results: 
* 3,000+ files per second
* 100+ directories per second
Using Qumulo 4 node QC24 cluster with a Mac 2.3 GHz Intel i7 8-core CPU.

### Usage
`python walk-tree.py --host product --user admin --password *********`


## Upgrade a Qumulo cluster

* This script downloads Qumulo software, loads it onto a Qumulo cluster, and upgrades the Qumulo cluster via the API.
* Script must be run from a MacOSX or linux client machine. It cannot be run directly on a Qumulo cluster due to reboots.

#### Recommended usage

1. Step 1: Prepare for an upgrade by *only* downloading the qimg(s) to the Qumulo cluster
`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers latest --download-only`
2. Step 2: Upgrade the latest build
`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers latest`

#### A few other examples

Upgrade from an older build to a not-quite latest build (2.9.0)

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.9.0`

Upgrade to 2.9.0 without downloading, assuming you already have the qimg file on Qumulo and properly named

`python qupgrade.py --qhost product --quser admin --qpass secret --vers 2.9.0`
