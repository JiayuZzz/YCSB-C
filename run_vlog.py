import funcs
import sys
import os

dbPath = "/mnt/vlog/"
valueSize = "512B"
dbSize = "40GB"
dbfilename = dbPath+"leveldb_vlog"+valueSize+dbSize
vlogfilename = dbPath+"vlog"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/vlog"+valueSize+dbSize

configs = {
    "bloomBits":"4",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "blockCache":str(64*1024*1024),
    "gcSize":(20*1024*1024*1024)
}

vlogs = {
    "vlogFilename":vlogfilename,
    "scanThreads":"32",
}

phase = sys.argv[1]

#set configs
if __name__ == '__main__':
    os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
    os.system("ulimit -n 50000")
    print(workload)
    print(vlogfilename)
    print(dbfilename)
    if phase == "load":
        configs["gcSize"] = "0"
    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    for vlog in vlogs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","vlog",vlog,vlogs[vlog])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile = resultfile+"_load"
        funcs.load("vlog",dbfilename,workload,resultfile)

    if phase=="run":
        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
        resultfile = resultfile+"_run"
        funcs.run("vlog",dbfilename,workload,resultfile)

    if phase =="both":
        os.system("rm -rf {0}".format(vlogfilename))
        resultfile1 = resultfile+"_load"
        funcs.load("vlog",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("vlog",dbfilename,workload,resultfile2)
