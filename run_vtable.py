import funcs
import sys
import os

dbPath = "/mnt/expdb/"
disk = "/dev/md0"
#dbPath = "/mnt/raidstore/"
valueSizes = ["4KB"]
maxSortedRuns = [10]
dbSize = "300GB"
smallThresh = 64
midThresh = 8192
for msr in maxSortedRuns:
    for valueSize in valueSizes:
        dbfilename = dbPath+"titandb"+valueSize+dbSize
        workload = "./workloads/workload"+valueSize+dbSize+".spec"
        memtable = 64
        threads = 8
        gcThreads = 4
        resultfile = "./resultDir/vtable"+valueSize+dbSize+"memtable"+str(memtable)+"threads"+str(threads)+"gc"+str(gcThreads)+"sortedrun"+str(msr)
        sepBeforeFlush = "true"
        if sepBeforeFlush == "true":
            resultfile = resultfile + "before"


        configs = {
            "bloomBits":"10",
            "seekCompaction":"false",
            "directIO":"false",
            "compression":"false",
            "noCompaction":"false",
            "blockCache":str(8*1024*1024),
            "memtable":str(memtable*1024*1024),
            "numThreads":str(threads),
            "gcThreads":str(gcThreads),
            "tiered":"false",
            "levelMerge":"true",
            "rangeMerge":"true",
            "sepBeforeFlush":sepBeforeFlush,
            "midThresh":str(midThresh),
            "smallThresh":str(smallThresh),
            "maxSortedRuns":str(msr),
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
                funcs.load("titandb",dbfilename,workload,resultfile,8,0)
                os.system("du -sh {0} >> db_size && date >> db_size".format(dbfilename))

            if phase=="run":
                resultfile = resultfile+"_run"
                print(resultfile)
                funcs.run("titandb",dbfilename,workload,resultfile)

            if phase=="both":
                resultfile1 = resultfile+"_both"
                funcs.both("titandb",dbfilename,workload,resultfile1,8,0)
                os.system("du -sh {0} >> db_size && date >> db_size".format(dbfilename))
                    

                    

