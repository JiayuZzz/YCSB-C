import tools
import sys

phase = sys.argv[1]
dbfilename = "./test"
workload = "./workloads/workloada.spec"

configs = {
    "bloomBits":"4",
    "seekCompaction":"true",
    "directIO":"false",
    "dbName0":"./test",
    "dbName1":"./nihao",
    "diskNum":"2"
}

#set configs
if __name__ == '__main__':
    for cfg in configs:
        tools.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if phase=="load": 
        tools.load(dbfilename,workload)

    if phase=="run":
        tools.run(dbfilename,workload)
