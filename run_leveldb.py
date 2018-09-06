import funcs
import sys

dbPath = "/mnt/leveldb/"
valueSize = "1KB"
dbSize = "50GB"
dbfilename = dbPath+"leveldb"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
resultfile = "./resultDir/leveldb_"+valueSize+dbSize


configs = {
    "bloomBits":"10",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false"
}

phase = sys.argv[1]

if __name__ == '__main__':
    #set configs
    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load": 
        resultfile = resultfile+"_load"
        funcs.load("leveldb",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        print(resultfile)
        funcs.run("leveldb",dbfilename,workload,resultfile)

    if phase=="both":
        resultfile1 = resultfile+"_load"
        funcs.load("leveldb",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("leveldb",dbfilename,workload,resultfile2)
        
