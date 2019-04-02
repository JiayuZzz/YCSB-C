import funcs
import sys
import os

dbPath = "/mnt/expdb/"
valueSize = "3KB"
dbSize = "40GB"
dbfilename = dbPath+"leveldb_exp"+valueSize+dbSize
vlogDir = dbPath+"/vlogs"
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/expdb"+valueSize+dbSize
gcSize = 0
#gcSize = 20*1024*1024*1024

configs = {
    "bloomBits":"4",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "blockCache":str(64*1024*1024),
    "sizeRatio":"10",
    "gcSize":str(gcSize)
}

exps = {
    "vlogDir":vlogDir,
    "expThreads":"32",
    "memSize":str(256*1024*1024),
}

phase = sys.argv[1]

#set configs
if __name__ == '__main__':
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
