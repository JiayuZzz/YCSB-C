import sys
import os

disks = {"vtable":"/dev/nvme0n1p1","titan":"/dev/nvme0n1p1","rocksdb":"/dev/nvme0n1p1","vtablenolarge":"/dev/nvme0n1p1","vtablenomid":"/dev/nvme0n1p1"}
paths = {"vtable":"/mnt/expdb","titan":"/mnt/titan","rocksdb":"/mnt/rocksdb","vtablenolarge":"/mnt/expdb","vtablenomid":"/mnt/expdb"}
backupPath = "/mnt/backup/"

for db,disk in disks.items():
    os.system("umount {0}".format(disk))
    os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, paths[db]))
    if db=="rocksdb":
        os.system("python run_{0}.py load && python run_{0}_scan.py run && python run_{0}.py run".format(db))
    else:
        os.system("python run_{0}.py both && python run_{0}_scan.py run".format(db))
    os.system("rm -rf {0}".format(backupPath+"*"))
    os.system("cp -r {0} {1}".format(paths[db]+"/*", backupPath))
    os.system("python run_{0}_core.py run".format(db))