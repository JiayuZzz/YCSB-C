import funcs
import sys
import os

dbPath = "/mnt/rocksdb/"
#dbPath = "/mnt/raidstore/"
disk = "/dev/md0"
valueSizes = ["4KB","8KB","16KB","512B"]
dbSize = "300GB"
for valueSize in valueSizes:
    thread = 8
    dbfilename = dbPath+"rocksdb"+valueSize+dbSize
    workload = "./workloads/workload"+valueSize+dbSize+".spec"
    memtable = 64
    resultfile = "./resultDir/rocksdb"+valueSize+dbSize+"memtable"+str(memtable)+"thread"+str(thread)


    configs = {
        "bloomBits":"10",
        "seekCompaction":"false",
        "directIO":"false",
        "noCompaction":"false",
        "compression":"false",
        "blockCache":str(8*1024*1024),
        "memtable":str(memtable*1024*1024),
        "numThreads":str(thread),
        "tiered":"false"
    }

    phase = sys.argv[1]

    if __name__ == '__main__':
        #set configs
        os.system("sync && echo 3 > /proc/sys/vm/drop_caches")
        if phase=="load":
            configs["noCompaction"] = "false"

        for cfg in configs:
            funcs.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

        if len(sys.argv) == 3:
            resultfile = sys.argv[2]

        if phase=="load":
            resultfile = resultfile+"_load"
            os.system("umount {0}".format(disk))
            os.system("mkfs.ext4 -b 4096 -E stride=128,stripe-width=512 {0} -F".format(disk))
            os.system("mount {0} {1}".format(disk, dbPath))
            funcs.load("rocksdb",dbfilename,workload,resultfile)
            os.system("du -sh {0} >> db_size && date >> db_size".format(dbfilename))

        if phase=="run":
            for i in range(1,5):
                resultfile = resultfile+"_run"+"round"+str(i)
                print(resultfile)
                funcs.run("rocksdb",dbfilename,workload,resultfile)

        if phase=="both":
            os.system("umount {0}".format(disk))
            os.system("mkfs.ext4 -b 4096 -E stride=128,stripe-width=512 {0} -F".format(disk))
            os.system("mount {0} {1}".format(disk, dbPath))
            resultfile1 = resultfile+"_both"
            funcs.both("rocksdb",dbfilename,workload,resultfile1)
            os.system("du -sh {0} >> db_size && date >> db_size".format(dbfilename))
