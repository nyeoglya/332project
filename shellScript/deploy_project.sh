#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")
# 워커와 마스터.jar 파일은 /home/green 에 존재해야함
worker_path="/home/green/worker.jar"
shell_path="/home/green/332project/shellScript"
remote_path="/home/green"

echo "Setting master shell on master..."
mv $shell_path/master /home/green/
chmod +x master
export PATH=$PATH:/home/green

for worker in "${workers[@]}"; do
	echo "Transferring worker.jar file to $worker..."
	scp $worker_path green@$worker:$remote_path/

	echo "Transferring worker shell to $worker..."
  scp $shell_path/worker green@$worker:$remote_path/

	echo "Setting worker shell on $worker..."
	ssh username@$worker "chmod +x worker && export PATH=$PATH:$remote_path"

	echo "worker.jar file transferred and set on $worker."
done

echo "All tasks have been completed on all workers."

