import tools
import sys

phase = sys.argv[1]
dbfilename = "/mnt/datadisk/leveldb"
workload = "./workloads/workloada.spec"
resultfile = "-1"

configs = {
    "bloomBits":"4",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false"
}

#set configs
if __name__ == '__main__':
    for cfg in configs:
        tools.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    resultfile = "./resultDir/result"

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load": 
        resultfile = resultfile+"_load"
        tools.load("leveldb",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        tools.run("leveldb",dbfilename,workload,resultfile)

    if phase=="both":
        resultfile = resultfile+"_load"
        tools.load("leveldb",dbfilename,workload,resultfile)
        resultfile = resultfile+"_run"
        tools.run("leveldb",dbfilename,workload,resultfile)
        
