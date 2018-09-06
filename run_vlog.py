import funcs
import sys
import os

dbPath = "/mnt/vlog/"
valueSize = "1KB"
dbSize = "100GB"
dbfilename = dbPath+"leveldb_vlog"+valueSize+dbSize
vlogfilename = dbPath+"vlog"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/vlog_"+valueSize+dbSize

configs = {
    "bloomBits":"4",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
}

vlogs = {
    "vlogFilename":vlogfilename
}

phase = sys.argv[1]

#set configs
if __name__ == '__main__':
    print(workload)
    print(vlogfilename)
    print(dbfilename)
    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    for vlog in vlogs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","vlog",vlog,vlogs[vlog])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile = resultfile+"_load"
        funcs.load("leveldbvlog",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        funcs.run("leveldbvlog",dbfilename,workload,resultfile)

    if phase =="both":
        os.system("rm -rf {0}".format(vlogfilename))
        resultfile1 = resultfile+"_load"
        funcs.load("leveldbvlog",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("leveldbvlog",dbfilename,workload,resultfile2)
