# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

#10GB 1KB value

recordcount=102261126
operationcount=0
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
field_len_dist=pareto
pareto_k=0.91
pareto_theta=0
pareto_sigma=226

readproportion=0
updateproportion=1.0
scanproportion=0
insertproportion=0

fieldlength=1024
requestdistribution=zipfian
scanlengthdistribution=constant
maxscanlength=2000


