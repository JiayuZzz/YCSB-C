import configparser
import os

def modifyConfig(file,field,key,value):
    parser = configparser.ConfigParser()
    parser.read(file)
    parser.set(field,key,value)
    parser.write(open(file,'w'))


def load(dbfilename,workload):
    os.system("./ycsbc -db leveldb -threads 1 -P {0} -skipLoad false".format(workload))

def run(dbfilename,workload):
    os.system("./ycsbc -db leveldb -threads 1 -P {0} -skipLoad true".format(workload))