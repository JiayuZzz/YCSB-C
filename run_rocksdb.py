import funcs
import sys
import os

dbPath = "/mnt/leveldb/"
#dbPath = "/mnt/raidstore/"
valueSize = "512B"
dbSize = "40GB"
dbfilename = dbPath+"rocksdb"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/rocksdb_raid0"+valueSize+dbSize


configs = {
    "bloomBits":"10",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "blockCache":str(0)
}

phase = sys.argv[1]

if __name__ == '__main__':
    #set configs
    os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        resultfile = resultfile+"_load"
        funcs.load("rocksdb",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        print(resultfile)
        funcs.run("rocksdb",dbfilename,workload,resultfile)

    if phase=="both":
        resultfile1 = resultfile+"_load"
        funcs.load("rocksdb",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("rocksdb",dbfilename,workload,resultfile2)
