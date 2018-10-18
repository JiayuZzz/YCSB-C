import funcs
import sys
import os

dbPath = "/mnt/expdb/"
valueSize = "1KB"
dbSize = "40GB"
dbfilename = dbPath+"leveldb_exp"+valueSize+dbSize
vlogDir = dbPath+"vlogDir"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/expdb"+valueSize+dbSize

configs = {
    "bloomBits":"4",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "blockCache":str(64*1024*1024)
}

exps = {
    "vlogDir":vlogDir,
    "expThreads":"36",
}

phase = sys.argv[1]

#set configs
if __name__ == '__main__':
    os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
    print(workload)
    print(vlogDir)
    print(dbfilename)
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
