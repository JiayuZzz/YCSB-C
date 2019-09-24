import funcs
import sys
import os

dbPath = "/data1/vtable/"
#dbPath = "/mnt/raidstore/"
valueSize = "1KB"
dbSize = "300GB"
dbfilename = dbPath+"titandb"+valueSize+dbSize
workload = "./workloads/workload"+valueSize+dbSize+".spec"
memtable = 256
resultfile = "./resultDir/vtitandb"+valueSize+dbSize+"memtable"+str(memtable)


configs = {
    "bloomBits":"10",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "blockCache":str(6*1024*1024),
    "memtable":str(memtable*1024*1024),
}

phase = sys.argv[1]

if __name__ == '__main__':
    #set configs
    os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load":
        resultfile = resultfile+"_load"
        funcs.load("titandb",dbfilename,workload,resultfile)

    if phase=="run":
        resultfile = resultfile+"_run"
        print(resultfile)
        funcs.run("titandb",dbfilename,workload,resultfile)

    if phase=="both":
        resultfile1 = resultfile+"_load"
        funcs.load("titandb",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("titandb",dbfilename,workload,resultfile2)
