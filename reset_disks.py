import sys
import os

disks = ["/dev/sdd1","/dev/sdc1","/dev/sdb1"]

for disk in disks:
    os.system("umount {0}".format(disk))
    os.system("mkfs.ext4 {0} -F".format(disk))

