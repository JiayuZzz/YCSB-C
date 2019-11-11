import funcs
import sys
import os

dbPath = "/mnt/rocksdb/"
#dbPath = "/mnt/raidstore/"
valueSizes = ["ratio"]
dbSize = "300GB"
for valueSize in valueSizes:
    dbfilename = dbPath+"rocksdb"+valueSize+dbSize
    workload = "./workloads/workload"+valueSize+dbSize+".spec"
    memtable = 64
    resultfile = "./resultDir/rocksdb"+valueSize+dbSize+"memtable"+str(memtable)


    configs = {
        "bloomBits":"10",
        "seekCompaction":"false",
        "directIO":"false",
        "noCompaction":"false",
        "compression":"false",
        "blockCache":str(6*1024*1024),
        "memtable":str(memtable*1024*1024),
        "numThreads":str(8),
        "tiered":"false"
    }

    phase = sys.argv[1]

    if __name__ == '__main__':
        #set configs
        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
        if phase=="load":
            configs["noCompaction"] = "false"

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
