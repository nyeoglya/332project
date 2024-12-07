#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" )

project_folder="/home/green/jdk"
project_file="/home/green/jdk.tar"
worker_file="/home/green/worker.jar"
shell_folder="/home/green/332project/shellScript"
root="/home/green"

echo "Setting master shell on master..."
cp $shell_folder/master $root
chmod +x $root/master
export PATH=$PATH:$root
cd $root
tar -xvf $project_file
chmod 777 $project_folder/bin/java

for worker in "${workers[@]}"; do
	echo "Transferring project to $worker..."
	ssh green@$worker "rm -f $project_file && rm -f $worker_file && rm -f $shell_folder/worker"
	scp $project_file green@$worker:$root
	scp $worker_file green@$worker:$root
	scp $shell_folder/worker green@$worker:$root
	ssh green@$worker "tar -xvf $project_file && chmod +x $root/worker && chmod 777 $project_folder/bin/java && rm -rf $project_file && export PATH=$PATH:$root"

	echo "Project file transferred on $worker."
done

echo "All tasks have been completed on all workers."

