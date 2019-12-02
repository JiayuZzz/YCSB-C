import funcs
import sys
import os

dbPath = "/mnt/pebbles/"
backupPath = "/mnt/backup/"
#dbPath = "/mnt/HDD/"
#valueSizes = ["8KB","6KB","4KB","2KB","1KB","512B","128B"]
valueSizes = ["corea","coreb","corec","cored","coree","coref"]
#valueSizes = ["coree"]
dbSize = "300GB"
for valueSize in valueSizes:
    workload = "./workloads/workload"+valueSize+dbSize+".spec"
    memtable = 64
    threads = 16
    dbfilename = dbPath+"pebblesdb"+"ratio"+dbSize
    backupfilename = backupPath+"pebblesdb"+"ratio"+dbSize
    resultfile = "./resultDir/pebblesdb"+valueSize+dbSize+"memtable"+str(memtable)
    print(dbfilename)

    configs = {
        "bloomBits":"10",
        "seekCompaction":"false",
        "compression":"false",
        "blockCache":str(6*1024*1024*1024),
        "memtable":str(memtable*1024*1024),
        "noCompaction":"false",
        "numThreads":str(threads),
    }

    phase = sys.argv[1]

    os.system("sync && echo 3 > /proc/sys/vm/drop_caches")

    if phase=="load":
        configs["noCompaction"]="false"

    for cfg in configs:
        funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load": 
        resultfile = resultfile+"_load"
        funcs.load("leveldb",dbfilename,workload,resultfile)

    if phase=="run":
        os.system("sudo rm -rf {0}".format(dbfilename))
        os.system("sudo cp -r {0} {1}".format(backupfilename, dbPath))
        resultfile = resultfile+"_run"
        print(resultfile)
        funcs.run("leveldb",dbfilename,workload,resultfile)

    if phase=="both":
        resultfile1 = resultfile+"_load"
        funcs.load("leveldb",dbfilename,workload,resultfile1)
        resultfile2 = resultfile+"_run"
        funcs.run("leveldb",dbfilename,workload,resultfile2)
        
