#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" )

project_folder="/home/green/332project"
project_file="/home/green/worker.jar"
shell_folder="/home/green/332project/shellScript"
root="/home/green"

echo "Setting master shell on master..."
cp $shell_folder/master $root
chmod +x $root/master
export PATH=$PATH:$root

cd $root
tar -czvf $project_file 332project

for worker in "${workers[@]}"; do
	echo "Transferring project to $worker..."
	scp $project_file green@$worker:$project_file
	ssh green@$worker "cp $shell_folder/worker $root && chmod +x $root/worker && export PATH=$PATH:$root"

	echo "Project file transferred on $worker."
done

echo "All tasks have been completed on all workers."

