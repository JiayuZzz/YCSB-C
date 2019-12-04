import funcs
import sys
import os

dbPath = "/mnt/rocksdb/"
#dbPath = "/mnt/raidstore/"
backupPath = "/mnt/backup/"
valueSizes = ["zipfcorea","zipfcoreb","zipfcorec","zipfcored","zipfcoree","zipfcoref"]
dbSize = "300GB"
for valueSize in valueSizes:
    dbfilename = dbPath+"rocksdb"+"ratio"+dbSize
    backupfilename = backupPath+"rocksdb"+"ratio"+dbSize
    workload = "./workloads/workload"+valueSize+dbSize+".spec"
    memtable = 64
    resultfile = "./resultDir/rocksdb"+valueSize+dbSize+"memtable"+str(memtable)


    configs = {
        "bloomBits":"10",
        "seekCompaction":"false",
        "directIO":"false",
        "noCompaction":"false",
        "compression":"false",
        "blockCache":str(8*1024*1024),
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
            os.system("sudo rm -rf {0}".format(dbfilename))
            os.system("sudo cp -r {0} {1}".format(backupfilename, dbPath))
            resultfile = resultfile+"_run"
            print(resultfile)
            funcs.run("rocksdb",dbfilename,workload,resultfile)

        if phase=="both":
            resultfile1 = resultfile+"_both"
            funcs.both("rocksdb",dbfilename,workload,resultfile1)
