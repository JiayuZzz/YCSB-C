import os
import sys
import funcs
import time
from multiprocessing import Process

#dbs = ["vtable"]
disk = "/dev/sdc1"
isRaid = False
paths = {"vtablelarge":"/mnt/vtable/","vtable":"/mnt/vtable/","rocksdb":"/mnt/rocksdb/","titandb":"/mnt/titan/","pebblesdb":"/mnt/pebbles/","rocksdb_tiered":"/mnt/rocksdb/"}

backupPath = "/mnt/backup/"


memtable = 64
compactionThreads = 8
gcThreads = 4       # backgroud gc threads

msr=10              # max sorted run
gcRatio = 0.3       # gc_threshold
blockWriteSize = 0 # GB. Block foreground write after values reach this size

midThresh = 8192
smallThresh = 128


configs = {}

def resetconfig():
    global configs
    configs = {
    "bloomBits":"10",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "noCompaction":"false",
    "blockCache":str(8*1024*1024*1024),
    "memtable":str(memtable*1024*1024),
    "numThreads":str(compactionThreads),
    "gcThreads":str(gcThreads),
    "tiered":"false",
    "levelMerge":"true",
    "rangeMerge":"true",
    "sepBeforeFlush":"true",
    "midThresh":str(midThresh),
    "smallThresh":str(smallThresh),
    "maxSortedRuns":str(msr),
    "gcRatio":str(gcRatio),
    "blockWriteSize":str(blockWriteSize*1024*1024*1024),
    }


def run_exp(exp):
    dbs = ["titandb","vtable"]
    foregroundThreadses = [16] # request concurrency
    valueSizes = ["1KB"]       # default
    dbSize = "100GB"           # default
    skipLoad = False           # skip load phase?
    backup = False             # backup db to /mnt/backup after load
    printSize = False          # record total db size to result file (lsm part and values part)
    useBackup = False          # use backup db (set skipLoad to True)
    noCompaction = False       # disable compaction in run phase
    bothPhase = False          # run load and run phase continuously without break, stats in a same result file
    workloads = []
    round = 1
    waitCompaction = 0
    backupUsed = False
    if exp == 1: # use this to run read-only workloads
        dbs = ["vtable","titandb"]
        #valueSizes = ["1KB"]
        #workloads = ["read"]
        valueSizes = ["pareto1KB"]
        workloads = ["read","zipf20scan","zipf100scan","zipf1000scan","zipf10000scan"]
        round = 1
        noCompaction = True
        backup = True
        skipLoad = False
        useBackup = False
        waitCompaction = 600 # wait for compaction completed
        if skipLoad:
            foregroundThreadses = [1]
    if exp == 2: # use this to run ycsb core workloads
        dbs = ["titandb"]
        valueSizes = ["pareto1KB"]
        workloads = ["corea","coreb","corec","cored","coree","coref","zipfcorea","zipfcoreb","zipfcorec","zipfcored","zipfcoree","zipfcoref"]
        backup = False
        skipLoad = True
        useBackup = True
        round = 1
        waitCompaction = 600
    if exp == 3:   # use this to run update workload
        dbs = ["vtable","titandb"]
        valueSizes = ["pareto1KB"]
        workloads = [""] # means update
        waitCompaction = 0
        bothPhase = True
        round = 1
        printSize=True
    if exp == 4:
        dbs = ["pebblesdb"]
        valueSizes = ["8KB","16KB","4KB"]
        workloads = ["1000scan","read","zipfread","zipf1000scan",""]
        round = 1
        skipLoad = False
        backup = False
        useBackup = False
        waitCompaction = 600
    if exp == 5:
        dbs = ["rocksdb"]
        valueSizes = ["128B","192B"]
        #valueSizes = ["64B"]
        dbSize = "100GB"
        workloads = [""]
        round = 1
        skipLoad = False
        backup = True
        useBackup = False
        waitCompaction = 600
    for db in dbs:
        backupUsed = False
        for foregroundThreads in foregroundThreadses:
            for valueSize in valueSizes:
                resetconfig()
                if db == "titandb":
                    configs["sepBeforeFlush"] = "true"
                    configs["levelMerge"] = "false"
                    configs["rangeMerge"] = "false"
                if db == "vtable":
                    configs["sepBeforeFlush"] = "true"
                    configs["levelMerge"] = "true"
                    configs["rangeMerge"] = "true"
                if db == "vtablelarge":
                    configs["midThresh"] = "32000"
                if db == "rocksdb_tiered":
                    configs["tiered"] = "true"
                dbfilename = paths[db] + db + valueSize +dbSize
                backupfilename = backupPath + db + valueSize +dbSize
                workload = "./workloads/workload"+valueSize+dbSize+".spec"
                resultfile = "./resultDir/"+db+valueSize+dbSize+"memtable"+str(memtable)+"forethreads"+str(foregroundThreads)+"compactionthreads"+str(compactionThreads)+"gcThreads"+str(gcThreads)+"sortedrun"+str(msr)+"gcratio"+str(gcRatio)+"midthresh"+str(midThresh)+"blockwrite"+str(blockWriteSize)
                for cfg in configs:
                    funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
                    
                # load data
                if not skipLoad and not bothPhase:
                    sizefile = "/home/kvgroup/wujiayu/YCSB-C/resultDir/sizefiles/"+db + valueSize +dbSize+"_load"
                    p = Process(target=funcs.getsize,args=(dbfilename, sizefile,))
                    if db!="titandb" and db!="vtable" and db!="vtablelarge":
                        p.start()
                    funcs.remount(disk, paths[db], isRaid)
                    resultfile_load = resultfile + "_load"
                    funcs.load(db,dbfilename,workload,resultfile_load,foregroundThreads,waitCompaction)
                    if db!="titandb" and db!="vtable" and db!="vtablelarge":
                        p.terminate()
                    os.system("echo load >> db_size && du -sh {0} >> db_size && date >> db_size".format(dbfilename))
                    if backup:
                        os.system("rm -rf {0}".format(backupfilename))
                        os.system("cp -r {0} {1}".format(dbfilename, backupPath))
                if not backupUsed and skipLoad and useBackup and exp!=2:
                    funcs.remount(disk, paths[db], isRaid)
                    os.system("sudo cp -r {0} {1}".format(backupfilename, paths[db]))
                    backupUsed = True
                    
                # run workloads
                for wl in workloads:
                    sizefile = "/home/kvgroup/wujiayu/YCSB-C/resultDir/sizefiles/"+ db + valueSize +dbSize+"_exp"+str(exp)
                    p = Process(target=funcs.getsize,args=(dbfilename, sizefile,))  # record db size change to resultDir/sizefiles/
                    workload = "./workloads/workload"+valueSize+wl+dbSize+".spec"
                    if noCompaction:
                        configs["noCompaction"] = "true"
                    for cfg in configs:
                        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
                    for r in range(0,round):
                        if useBackup and exp==2:
                            funcs.remount(disk, paths[db], isRaid)
                            os.system("sudo cp -r {0} {1}".format(backupfilename, paths[db]))
                        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
                        resultfile_wl = resultfile+"_"+wl+"round"+str(r)
                        if bothPhase:
                            if db!="titandb" and db!="vtable" and db!="vtablelarge":
                                p.start()
                            funcs.remount(disk, paths[db], isRaid)
                            funcs.both(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                        else:
                            funcs.run(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                        if printSize:
                            os.system("echo workload{0} >> db_size && du -sh {1} >> db_size && date >> db_size".format(wl,dbfilename))
                            os.system("du -sh {0} >> {1}".format(dbfilename,resultfile_wl))
                        if bothPhase and db!="titandb" and db!="vtable" and db!="vtablelarge":
                            p.terminate()

if __name__ == '__main__':
    exp = int(sys.argv[1])
    if exp<10 and exp>0:
        run_exp(exp)
