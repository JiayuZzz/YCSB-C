import funcs
import sys
import os

phase = sys.argv[1]
dbfilename = "/mnt/datadisk/leveldb_vlog"
workload = "./workloads/workload1KB.spec"
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
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    for vlog in vlogs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","vlog",vlog,vlogs[vlog])
    resultfile = "./resultDir/vlog_result"

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile = resultfile+"_load"
        funcs.load("leveldbvlog",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        funcs.run("leveldbvlog",dbfilename,workload,resultfile)

    if phase=="both":
        os.system("rm -rf {0}".format(vlogs["vlogFilename"]))
        resultfile1 = resultfile+"_load"
        funcs.load("leveldbvlog",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("leveldbvlog",dbfilename,workload,resultfile1)


