## Walk a Qumulo file system via the API

This python script uses the Qumulo api and the UltraJson library to quickly walk a Qumulo filesystem tree in a parallel manner. The output is a single file with a list of every file and directory as well as its size. You can look in the file to see all the available file metadata.

Performance results: 
* 40,000 files per second
* 1,000 files per second
Using Qumulo 4 node QC24 cluster with a Mac 2.3 GHz Intel i7 8-core CPU.

### Setup
`pip install -r requirements.txt`

### Usage
`python walk-tree.py --host product --user admin --password *********`
