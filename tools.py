import configparser
import os

# repair case problem in configparser package
class MyConfigParser(configparser.ConfigParser):
    def __init__(self, defaults=None):
        configparser.ConfigParser.__init__(self, defaults=defaults)
    def optionxform(self, optionstr):
        return optionstr


def modifyConfig(file,field,key,value):
    parser = MyConfigParser()
    parser.read(file)
    parser.set(field,key,value)
    parser.write(open(file,'w'))


def load(dbfilename,workload,resultfile="-1"):
    if resultfile=="-1":
        os.system("./ycsbc -db leveldb -dbfilename {0} -threads 1 -P {1} -skipLoad false".format(dbfilename,workload))
    else:
        os.system("./ycsbc -db leveldb -dbfilename {0} -threads 1 -P {1} -skipLoad false > {2}".format(dbfilename,workload,resultfile))

def run(dbfilename,workload,resultfile="-1"):
    if resultfile=="-1":
        os.system("./ycsbc -db leveldb -dbfilename {0} -threads 1 -P {1} -skipLoad true".format(dbfilename,workload))
    else:
        os.system("./ycsbc -db leveldb -dbfilename {0} -threads 1 -P {1} -skipLoad true > {2}".format(dbfilename,workload,resultfile))
