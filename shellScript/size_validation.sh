#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")
home_folder="/home/green"

remote_input_folder="$home_folder/dataset/big_input"
remote_output_folder="$home_folder/dataset/big_output"

master_local_folder="$home_folder/size_validation"
output_file="$home_folder/size_validation_merged.txt"

set -e
rm -r "$master_local_folder" || true
mkdir -p "$master_local_folder"

for worker in "${workers[@]}"; do
    echo ">> Processing $worker..."

    # calculate unsorted files size & send
    # ssh "$worker" "du -sb $remote_input_folder > $remote_input_folder/size-$worker.txt"
    # scp "$worker:$remote_input_folder/size-$worker.txt" "$master_local_folder/"

    # calculate sorted files size & send
    ssh "$worker" "du -sb $remote_output_folder > $remote_output_folder/size-$worker.txt"
    scp "$worker:$remote_output_folder/size-$worker.txt" "$master_local_folder/"
    
    echo " "
done

# 총합 초기화
total_sorted_size=0

# 폴더 내 모든 파일을 순회하며 값 계산
for file in "$master_local_folder"/*; do
  if [ -f "$file" ]; then
    size=$(awk '{print $1}' "$file")
    total_sorted_size=$((total_size + size))
  fi
done

echo ">> total size: $total_sorted_size"
echo ">> complete size validation."
