#/bin/bash

workload="./workloads/workloada.spec"

./ycsbc -db leveldb -dbfilename test -threads 1 -P $workload -skipLoad true
