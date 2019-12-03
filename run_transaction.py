import sys
import os

disks = {"vtable":"/dev/sdd1","titan":"/dev/sdc1","rocksdb":"/dev/sdb1"}
paths = {"vtable":"/mnt/expdb","titan":"/mnt/titan","rocksdb":"/mnt/rocksdb"}
backupPath = "/mnt/backup/"

for db,disk in disks：
    os.system("umount {0}".format(disk))
    os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, paths[db]))
    if db=="rocksdb":
        os.system("python run_{0}.py load && python run_{0}_scan.py run && python run_{0}.py run".format(db))
    else:
        os.system("python run_{0}_.py both && python run_{0}_scan.py run".format(db))
    os.system("rm -rf {0}".format(backupPath+"*"))
    os.system("cp -r {0} {1}".format(paths[db]+"/*", backupPath))
    os.system("python run_{0}_core.py run && python run_{0}_zipfcore.py run".format(db))