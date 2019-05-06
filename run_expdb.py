import funcs
import sys
import os

dbPath = "/mnt/expdb/"
#valueSizes = ["128B","256B","512B","1KB","2KB","3KB","4KB"]
valueSizes = ["1KB"]
dbSize = "40GB"

if __name__ == '__main__':
    for valueSize in valueSizes:

        dbfilename = dbPath+"leveldb_exp"+valueSize+dbSize
        vlogDir = dbfilename+"/vlogs"
        workload = "./workloads/workload"+valueSize+dbSize+".spec"
        gcSize = 0
        memSize = 1024
        memtable = 64
        resultfile = "./resultDir/expdb"+valueSize+dbSize+str(memSize)+"memtable"+str(memtable)
        #gcSize = 20*1024*1024*1024

        configs = {
            "bloomBits":"10",
            "seekCompaction":"false",
            "directIO":"false",
            "compression":"false",
            "blockCache":str(64*1024*1024),
            "sizeRatio":"10",
            "gcSize":str(gcSize),
            "noCompaction":"true",
            "memtable":str(memtable*1024*1024),
        }

        exps = {
            "vlogDir":vlogDir,
            "expThreads":"32",
            "memSize":str(memSize*1024*1024),
        }

        phase = sys.argv[1]

        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
        os.system("ulimit -n 50000")
        print(workload)
        print(vlogDir)
        print(dbfilename)
        if phase == "load":
            configs["gcSize"] = "0"
        for cfg in configs:
            funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
        for exp in exps:
            funcs.modifyConfig("./configDir/leveldb_config.ini","expdb",exp,exps[exp])
        if len(sys.argv) == 3:
            resultfile = sys.argv[2]

        if phase=="load":
            os.system("rm -rf {0}".format(vlogDir))
            resultfile = resultfile+"_load"
            funcs.load("expdb",dbfilename,workload,resultfile)

        if phase=="run":
            resultfile = resultfile+"_run"
            funcs.run("expdb",dbfilename,workload,resultfile)

        if phase =="both":
            os.system("rm -rf {0}".format(vlogDir))
            resultfile1 = resultfile+"_load"
            funcs.load("expdb",dbfilename,workload,resultfile1)
            resultfile2 = resultfile+"_run"
            funcs.run("expdb",dbfilename,workload,resultfile2)
