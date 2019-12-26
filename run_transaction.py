import sys
import os

#disks = {"titandb":"/dev/sdc1","vtable":"/dev/sdd1"}
#paths = {"titandb":"/mnt/titan","vtable":"/mnt/expdb"}
disks = {"vtable":"/dev/md0","rocksdb":"/dev/md0"}
paths = {"vtablenolarge":"/mnt/expdb","vtable":"/mnt/expdb","rocksdb":"/mnt/rocksdb","titandb":"/mnt/titan","pebblesdb":"/mnt/pebbles"}

backupPath = "/mnt/backup/"

for db,disk in disks.items():
    os.system("umount {0}".format(disk))
    if disk == "/dev/md0":
        os.system("mkfs.ext4 -b 4096 -E stride=128,stripe-width=512 {0} -F".format(disk))
    else:
        os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, paths[db]))
    if db=="rocksdb" or db=="rocksdbuni":
        os.system("python run_{0}.py load && python run_{0}_scan.py run && python run_{0}.py run".format(db))
        os.system("cp -r {0} {1}".format(paths[db]+"/*","/mnt/rocksbackup/"))
    else:
        os.system("python run_{0}.py both && python run_{0}_scan.py run".format(db))
    os.system("rm -rf {0}".format(backupPath+"*"))
    os.system("cp -r {0} {1}".format(paths[db]+"/*", backupPath))
    os.system("python run_{0}_core.py run".format(db))
