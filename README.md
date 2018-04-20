## Walk a Qumulo file system via the API

This python script uses the Qumulo api and the UltraJson library to quickly walk a Qumulo filesystem tree in a parallel manner. The output is a single file with a list of every file and directory as well as its size. You can look in the file to see all the available file metadata.

Performance results: 
* 40,000 files per second
* 1,000 files per second
Using Qumulo 4 node QC24 cluster with a Mac 2.3 GHz Intel i7 8-core CPU.

### Requirments
* python 2.7
* `pip install -r requirements.txt`

### Usage
`python walk-tree.py --host product --user admin --password *********`


## Upgrade a Qumulo cluster

This script downloads software from Box, loads it onto your Qumulo clusters, and upgrades the Qumulo cluster via the API.

### Requirments
* python 2.7
* `pip install qumulo_api requests`
* Script must be run from a client machine. Cannot be run directly on a Qumulo cluster due to reboots.

### Usage
Upgrade to version 2.8.2 from 2.8.1

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.8.2`

Upgrade from 2.8.0 to 2.8.1 and then 2.8.2

`python qupgrade.py --qhost product --quser admin --qpass secret --sharepass secret --vers 2.8.1,2.8.2`

Upgrade to 2.8.2 without downloading from Box. The qimg file must exist in /upgrade on the cluster

`python qupgrade.py --qhost product --quser admin --qpass secret --vers 2.8.2`