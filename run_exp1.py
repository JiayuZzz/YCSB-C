import os
import sys
import funcs
import time
from multiprocessing import Process

#dbs = ["vtable"]
disk = "/dev/sdb1"
isRaid = False
paths = {"vtablelarge":"/mnt/vtable/","vtable":"/mnt/vtable/","rocksdb":"/mnt/rocksdb/","titandb":"/mnt/titan/","pebblesdb":"/mnt/pebbles/"}

backupPath = "/mnt/backup/"


memtable = 64
compactionThreads = 8
gcThreads = 8

msr=10
gcratio = 0.3

midThresh = 8192
smallThresh = 128


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
}

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
    }


def run_exp(exp):
    dbs = ["titandb","vtable"]
    foregroundThreadses = [16]
    valueSizes = ["1KB"]
    dbSize = "100GB"
    skipLoad = False
    bothPhase = False
    needRemount = False
    backup = False
    printSize = False
    useBackup = False
    workloads = []
    round = 1
    waitCompaction = 0
    backupUsed = False
    if exp == 1: # overall fix
        dbs = ["vtable"]
        valueSizes = ["pareto512B"]
        #workloads = ["read","zipf1000scan","zipf20scan","zipf100scan","zipf1000scan"]
        workloads = ["read","zipf1000scan"]

        round = 1
        skipLoad = True
        backup = False
        useBackup = True
        waitCompaction = 1200
        if skipLoad:
            foregroundThreadses = [16]
    if exp == 2:
        dbs = ["titandb"]
        waitCompaction = 1200
        valueSizes = ["pareto1KB"]
        backup = False
        skipLoad = True
        useBackup = True
        round = 1
        workloads = ["corea","coreb","corec","cored","coree","coref","zipfcorea","zipfcoreb","zipfcorec","zipfcored","zipfcoree","zipfcoref"]
        #workloads = ["zipfcorec"]
    if exp == 3:
        dbs = ["titandb"]
        valueSizes = ["1KB"]
        #valueSizes = ["pareto1KB"]
        waitCompaction = 0
        backup = False
        dbSize = "100GB"
        workloads = [""]
        skipLoad = False
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

                dbfilename = paths[db] + db + valueSize +dbSize
                backupfilename = backupPath + db + valueSize +dbSize
                workload = "./workloads/workload"+valueSize+dbSize+".spec"
                resultfile = "./resultDir/"+db+valueSize+dbSize+"memtable"+str(memtable)+"forethreads"+str(foregroundThreads)+"compactionthreads"+str(compactionThreads)+"gcThreads"+str(gcThreads)+"sortedrun"+str(msr)+"gcratio"+str(gcratio)+"midthresh"+str(midThresh)
                for cfg in configs:
                    funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
                if not skipLoad:
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
                for wl in workloads:
                    if exp == 4 and wl == "":
                        printSize=True
                    if exp == 4 and wl != "":
                        printSize=False
                        configs["noCompaction"] = "true"
                    sizefile = "/home/kvgroup/wujiayu/YCSB-C/resultDir/sizefiles/"+ db + valueSize +dbSize+"_exp"+str(exp)
                    p = Process(target=funcs.getsize,args=(dbfilename, sizefile,))
                    workload = "./workloads/workload"+valueSize+wl+dbSize+".spec"
                    if exp == 1 or exp == 4 or exp==5:
                        configs["noCompaction"] = "true"
                    if exp == 4 and wl == "":
                        configs["noCompaction"] = "false"
                    for cfg in configs:
                        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
                    for r in range(0,round):
                        if useBackup and exp==2:
                            funcs.remount(disk, paths[db], isRaid)
                            os.system("sudo cp -r {0} {1}".format(backupfilename, paths[db]))
                        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
                        resultfile_wl = resultfile+"_"+wl+"round"+str(r)
                        if exp == 3 and db!="titandb" and db!="vtable" and db!="vtablelarge":
                            p.start()
                        if exp == 3:
                            funcs.remount(disk, paths[db], isRaid)
                            funcs.both(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                        else:
                            funcs.run(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                        if printSize:
                            os.system("echo workload{0} >> db_size && du -sh {1} >> db_size && date >> db_size".format(wl,dbfilename))
                            os.system("du -sh {0} >> {1}".format(dbfilename,resultfile_wl))
                        if exp == 3 and db!="titandb" and db!="vtable" and db!="vtablelarge":
                            p.terminate()


if __name__ == '__main__':
    exp = int(sys.argv[1])
    if exp<10 and exp>0:
        run_exp(exp)
