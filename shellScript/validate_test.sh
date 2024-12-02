#!bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")

test_name="$1"
worker_size="$2"
dir_size="$3"
file_size="$4"
entity_Size="$5"

root="/home/green"
dir_path="/home/green/dataset"
name="$dir_path/$test_name"

for worker_num in $(seq 0 $(($worker_size - 1))); do
	worker=${workers[$worker
