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

This script downloads software from Box, loads it onto your Qumulo clusters, and upgrades the Qumulo cluster via the API.

### Usage
* Script must be run from a MacOSX or linux client machine. Cannot be run directly on a Qumulo cluster due to reboots.
* For dot versions (like 2.8.2.1), please simply specify 2.8.2 in the versions list.

Upgrade to version 2.8.2 from 2.8.1

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.8.2`

Upgrade from 2.8.0 to 2.8.1 and then 2.8.2

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.8.1,2.8.2`

Upgrade to 2.8.2 without downloading from Box. The qimg file must exist in /upgrade on the cluster

`python qupgrade.py --qhost product --quser admin --qpass secret --vers 2.8.2`


## Copy settings from one Qumulo cluster to another

This script copies NFS exports, SMB shares, local users/groups, vpn, and MissionQontrol settings from one Qumulo cluster to another.

### Usage

Copy settings from qumulo1 to qumulo2
`copy-settings.py --src_host qumulo1 --src_user admin --src_pass 555if234 --dest_host qumulo2 --dest_user admin --dest_pass 234sdf235`
