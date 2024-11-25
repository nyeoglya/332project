#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

master_path="/home/green/master"
worker_path="/home/green/worker"
files_path="/home/green/files"

echo "Activate master..."

cd $master_path
master 10

for worker in "${workers[@]}"; do
	echo "Activate $worker..."
	ssh green@$worker "cd $worker_path && worker 2.2.2.254 -I $files_path/input0 $files_path/input1 $files_path/input2 $files_path/input3 $files_path/input4 $files_path/input5 $files_path/input6 $files_path/input7 $files_path/input8 $files_path/input9 -O $files_path/output"
done

echo "Activate master & 10 workers"
