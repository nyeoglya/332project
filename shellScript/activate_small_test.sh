#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")

worker_num=10
input_dir="/home/green/dataset/small_input0"
output_dir="/home/green/dataset/small_output"

echo "Activate master..."

master $worker_num

for worker in "${workers[@]}"; do
	echo "Activate $worker..."
	ssh green@$worker "worker 2.2.2.254 -I $input_dir -O $output_dir"
done

echo "Activate master & 10 workers"
