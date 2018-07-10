import tools
import sys

phase = sys.argv[1]
dbfilename = "./test"
workload = "./workloads/workloada.spec"
resultfile = "-1"

configs = {
    "bloomBits":"4",
    "seekCompaction":"true",
    "directIO":"false",
    "compression":"false",
    "dbName0":"./test",
    "dbName1":"./test1",
    "diskNum":"2"
}

#set configs
if __name__ == '__main__':
    for cfg in configs:
        tools.modifyConfig("./configDir/leveldb_config.ini","config",cfg,configs[cfg])

    if len(sys.argv) == 3:
        resultfile = sys.argv[2]

    if phase=="load": 
        tools.load(dbfilename,workload,resultfile)

    if phase=="run":
        tools.run(dbfilename,workload,resultfile)