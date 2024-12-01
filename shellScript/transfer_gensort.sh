#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")

file_path="/home/green/332project/shellScript/gensort-linux-1.5.tar.gz"
worker_path="/home/green/gensort_source"

for worker in "${workers[@]}"; do
	ssh green@$worker "mkdir -p $worker_path"
	echo "Transferring file to $worker..."
	scp $file_path green@$worker:$worker_path/

	echo "Extracting file on $worker..."
	ssh green@$worker "cd $worker_path && tar -xzf gensort-linux-1.5.tar.gz && mv $worker_path/64/gensort /home/green/ && mv $worker_path/64/valsort /home/green/ && cd /home/green && rm -rf $worker_path"

	echo "File transferred and extracted on $worker."
done

echo "All files have been transferred and extracted on all workers."
