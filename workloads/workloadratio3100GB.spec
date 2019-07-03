# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

#10GB 1KB value

recordcount=71014670
#recordcount=30000000
#operationcount=284058682
operationcount=10
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0
scanproportion=1
insertproportion=0

fieldlength=4072
field_len_dist=ratio
requestdistribution=uniform
scanlengthdistribution=constant
maxscanlength=500000

largevalue=0.1
midvalue=0.5
smallvalue=0.4
largesize=16384
midsize=2048
smallsize=100

