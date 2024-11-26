#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

root="/home/green"
dir_path="/home/green/files"
worker_size=10
dir_size=10
file_size=100
entity_size=320000



for worker_num in $(seq 0 $(($worker_size - 1))); do
	worker=${workers[$worker_num]}
	ssh green@$worker "rm -rf $dir_path && mkdir -p $dir_path/output"
	
	echo "Generate input files on $worker..."

	for dir_num in $(seq 0 $(($dir_size - 1))); do
		ssh green@$worker "mkdir -p $dir_path/input$dir_num"
		for file_num in $(seq 0 $(($file_size - 1))); do
			num=$(expr $worker_num \* $dir_size \* $file_size + $dir_num \* $file_size + $file_num)
			offset=$(expr $num \* $entity_size)
			ssh green@$worker "cd $dir_path/input$dir_num && $root/gensort -a -b$offset $entity_size input${num}.txt"
 		done
	done
	
	echo "Input files generated on $worker."
done

echo "All tasks have been completed on all workers."

