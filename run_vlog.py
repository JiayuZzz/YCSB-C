import tools
import sys
import os

phase = sys.argv[1]
dbfilename = "/mnt/datadisk/leveldb"
workload = "./workloads/workloada.spec"
resultfile = "-1"

configs = {
    "bloomBits":"4",
    "seekCompaction":"true",
    "directIO":"false",
    "compression":"false",
}

vlogs = {
    "vlogFilename":"/mnt/datadisk/vlog"
}

#set configs
if __name__ == '__main__':
    for cfg in configs:
        tools.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    for vlog in vlogs:
        tools.modifyConfig("./configDir/leveldb_config.ini","vlog",vlog,vlogs[vlog])
    resultfile = "/home/wujy/res/result"

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile = resultfile+"_load"
        tools.load("leveldbvlog",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        tools.run("leveldbvlog",dbfilename,workload,resultfile)

    if phase=="both":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile = resultfile+"_load"
        tools.load("leveldbvlog",dbfilename,workload,resultfile)
        resultfile = resultfile+"_run"
        tools.run("leveldbvlog",dbfilename,workload,resultfile)


