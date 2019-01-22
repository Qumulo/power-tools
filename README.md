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
