#/bin/bash

workload="./workloads/workloada.spec"

./ycsbc -db leveldb -threads 1 -P $workload -skipLoad false
