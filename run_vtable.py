import funcs
import sys
import os

dbPath = "/mnt/expdb/"
#dbPath = "/mnt/raidstore/"
valueSizes = ["ratio"]
dbSize = "300GB"
smallThresh = 128
midThresh = 4096
for valueSize in valueSizes:
    dbfilename = dbPath+"titandb"+valueSize+dbSize
    workload = "./workloads/workload"+valueSize+dbSize+".spec"
    memtable = 64
    resultfile = "./resultDir/vtable"+valueSize+dbSize+"memtable"+str(memtable)
    sepBeforeFlush = "true"
    if sepBeforeFlush == "true":
        resultfile = resultfile + "before"
    
    
    configs = {
        "bloomBits":"10",
        "seekCompaction":"false",
        "directIO":"false",
        "compression":"false",
        "noCompaction":"true",
        "blockCache":str(6*1024*1024),
        "memtable":str(memtable*1024*1024),
        "numThreads":str(8),
        "tiered":"false",
        "levelMerge":"true",
        "rangeMerge":"true",
        "sepBeforeFlush":sepBeforeFlush,
        "midThresh":str(midThresh),
        "smallThresh":str(smallThresh)
    }
    
    phase = sys.argv[1]
    
    if __name__ == '__main__':
        #set configs
        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
    
        if phase=="load":
            configs["noCompaction"] = "false"
            
        for cfg in configs:
            funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
    
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
            resultfile1 = resultfile+"_both"
            funcs.both("titandb",dbfilename,workload,resultfile1)
    
