import funcs
import sys
import os
import time
from multiprocessing import Process

dbs = ["titandb","vtable"]
#dbs = ["vtable"]
disk = "/dev/md0"
isRaid = True
paths = {"vtablenolarge":"/mnt/expdb/","vtable":"/mnt/expdb/","rocksdb":"/mnt/rocksdb/","titandb":"/mnt/titan/","pebblesdb/":"/mnt/pebbles/"}

backupPath = "/mnt/backup/"


memtable = 64
compactionThreads = 4
gcThreads = 4
foregroundThreads = 8

msr=10
gcratio = 0.3

midThresh = 8096
smallThresh = 64

configs = {
    "bloomBits":"10",
    "seekCompaction":"false",
    "directIO":"false",
    "compression":"false",
    "noCompaction":"false",
    "blockCache":str(8*1024*1024),
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
    valueSizes = ["1KB"]
    dbSize = "300GB"
    skipLoad = False
    bothPhase = False
    needRemount = False
    backup = False
    printSize = False
    useBackup = False
    workloads = []
    round = 1
    waitCompaction = 0
    if exp == 1: # overall fix
        workloads = ["20scan","100scan","1000scan","10000scan","zipf20scan","zipf100scan","zipf1000scan","zipf10000scan"]
        round = 5
        backup = True
        waitCompaction = 1200
    if exp == 2:
        waitCompaction = 1200
        backup = True
        round = 1
        workloads = ["corea","coreb","corec","cored","coree","coref","zipfcorea","zipfcoreb","zipfcorec","zipfcored","zipfcoree","zipfcoref"]
    if exp == 3:
        waitCompaction = 0
        backup = False
        dbSize = "100GB"
        workloads = [""]
        skipLoad = True
        printSize = True
        round = 1
    for db in dbs:
        if db == "titandb":
            configs["sepBeforeFlush"] = "true"
            configs["levelMerge"] = "false"
            configs["rangeMerge"] = "false"
        if db == "vtable":
            configs["sepBeforeFlush"] = "true"
            configs["levelMerge"] = "true"
            configs["rangeMerge"] = "true"
        for valueSize in valueSizes:
            dbfilename = paths[db] + db + valueSize +dbSize
            backupfilename = backupPath + db + valueSize +dbSize
            workload = "./workloads/workload"+valueSize+dbSize+".spec"
            resultfile = "./resultDir/"+db+valueSize+dbSize+"memtable"+str(memtable)+"forethreads"+str(foregroundThreads)+"compactionthreads"+str(compactionThreads)+"gcThreads"+str(gcThreads)+"sortedrun"+str(msr)+"gcratio"+str(gcratio)
            for cfg in configs:
                funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])
            if not skipLoad:
                sizefile = "/home/wujy/workspace/YCSB-C/resultDir/sizefiles/"+db + valueSize +dbSize+"_load"
                p = Process(target=funcs.getsize,args=(dbfilename, sizefile,))
                if db!="titandb" and db!="vtable" and db!="vtablenolarge":
                    p.start()
                funcs.remount(disk, paths[db], isRaid)
                resultfile_load = resultfile + "_load"
                funcs.load(db,dbfilename,workload,resultfile_load,foregroundThreads,waitCompaction)
                if db!="titandb" and db!="vtable" and db!="vtablenolarge":
                    p.terminate()
                os.system("echo load >> db_size && du -sh {0} >> db_size && date >> db_size".format(dbfilename))
                if backup:
                    if db == "rocksdb":
                        os.system("rm -rf /mnt/rocksbackup/"+db+"*")
                        os.system("cp -r {0} /mnt/rocksbackup".format(dbfilename))
                    else:
                        os.system("rm -rf {0}".format(backupPath+db+"*"))
                        os.system("cp -r {0} {1}".format(dbfilename, backupPath))
            for wl in workloads:
                sizefile = "/home/wujy/workspace/YCSB-C/resultDir/sizefiles/"+db + valueSize +dbSize+"_exp"+str(exp)
                p = Process(target=funcs.getsize,args=(dbfilename, sizefile,))
                workload = "./workloads/workload"+valueSize+wl+dbSize+".spec"
                for r in range(0,round):
                    if useBackup:
                        funcs.remount(disk, paths[db], isRaid)
                        os.system("sudo cp -r {0} {1}".format(backupfilename, paths[db]))
                    resultfile_wl = resultfile+"_"+wl+"round"+str(r)
                    if exp == 3 and db!="titandb" and db!="vtable" and db!="vtablenolarge":
                        p.start()
                    if exp == 3:
                        funcs.remount(disk, paths[db], isRaid)
                        funcs.both(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                    else:
                        funcs.run(db,dbfilename,workload, resultfile_wl,foregroundThreads)
                    if printSize:
                        os.system("echo workload{0} >> db_size && du -sh {1} >> db_size && date >> db_size".format(wl,dbfilename))
                    if exp == 3 and db!="titandb" and db!="vtable" and db!="vtablenolarge":
                        p.terminate()


if __name__ == '__main__':
    exp = int(sys.argv[1])
    if exp<10 and exp>0:
        run_exp(exp)
