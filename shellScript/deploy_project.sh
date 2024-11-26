#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

project_folder="/home/green/332project"
project_file="/home/green/project.tar.gz"
shell_folder="/home/green/332project/shellScript"
root="/home/green"

echo "Setting master shell on master..."
mv $shell_folder/master $root
chmod +x master
export PATH=$PATH:$root
$project_folder/sbt assembly

tar -czvf $project_file $project_folder

for worker in "${workers[@]}"; do
	echo "Transferring project to $worker..."
	scp $project_folder green@$worker:$root
	ssh green@$worker "tar -xzvf $project_file && mv $shell_folder/worker $root && chmod +x worker && export PATH=$PATH:$root && $project_folder/sbt assembly"

	echo "Project file transferred on $worker."
done

echo "All tasks have been completed on all workers."

