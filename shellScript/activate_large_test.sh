#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" )

worker_num=10
input_dir="/home/dataset/large"
output_dir="/home/green/dataset/large_output"
master_ip="2.2.2.254:50050"

/home/green/332project/shellScript/kill_process.sh

for worker in "${workers[@]}"; do
	echo "Activate $worker..."
	ssh green@$worker "cd /home/green && rm -rf $output_dir && rm -rf /home/green/dataset/partitioned && rm -rf /home/green/dataset/merging && mkdir -p $output_dir && ./worker $master_ip -I $input_dir -O $output_dir" &
	sleep 5s
done

echo "Activate master & 10 workers"
