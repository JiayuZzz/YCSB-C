import configparser
import os

# repair case problem in configparser package
class MyConfigParser(configparser.ConfigParser):
    def __init__(self, defaults=None):
        configparser.ConfigParser.__init__(self, defaults=defaults)
    def optionxform(self, optionstr):
        return optionstr

def remount(disk, path, israid):
    os.system("umount {0}".format(disk))
    if israid:
        os.system("mkfs.ext4 -b 4096 -E stride=128,stripe-width=512 {0} -F".format(disk))
    else:
        os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, path))

def modifyConfig(file,field,key,value):
    parser = MyConfigParser()
    parser.read(file)
    parser.set(field,key,value)
    parser.write(open(file,'w'))

def remount(disk, path, israid):
    os.system("umount {0}".format(disk))
    if israid:
        os.system("mkfs.ext4 -b 4096 -E stride=128,stripe-width=512 {0} -F".format(disk))
    else:
        os.system("mkfs.ext4 {0} -F".format(disk))
    os.system("mount {0} {1}".format(disk, path))


def load(db,dbfilename,workload,resultfile="-1",threads=8,sleep=0):
    os.system("sudo rm -rf {0}".format(dbfilename))
    if resultfile=="-1":
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {3} -P {2} -phase load".format(db,dbfilename,workload,threads))
    else:
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {4} -P {2} -phase load -sleep {5} > {3}".format(db,dbfilename,workload,resultfile,threads,sleep))

def run(db,dbfilename,workload,resultfile="-1",threads=8):
    if resultfile=="-1":
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {3} -P {2} -phase run".format(db,dbfilename,workload,threads))
    else:
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {4} -P {2} -phase run > {3}".format(db,dbfilename,workload,resultfile,threads))

def both(db,dbfilename,workload,resultfile="-1",threads=8):
    os.system("sudo rm -rf {0}".format(dbfilename))
    if resultfile=="-1":
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {3} -P {2} -phase both -sleep 0".format(db,dbfilename,workload,threads))
    else:
        os.system("./ycsbc -db {0} -dbfilename {1} -threads {2} -P {3} -phase both -sleep 0 > {4}".format(db,dbfilename,threads,workload,resultfile))
