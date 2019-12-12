import sys
import os

#disks = {"vtable":"/dev/sdd1","titandb":"/dev/sdc1","rocksdb":"/dev/sdb1"}
disks = {"titandb":"/dev/sdc1"}
paths = {"vtable":"/mnt/expdb","titandb":"/mnt/titan","rocksdb":"/mnt/rocksdb"}
backupPath = "/mnt/backup/"

for db,disk in disks.items():
    os.system("umount {0}".format(disk))
    os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, paths[db]))
    os.system("python run_{0}.py both".format(db))
