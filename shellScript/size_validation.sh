#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")
name="$1"
[ -z "$name" ] && { echo ">> error: No name provided." >&2; exit 1; }
home_folder="/home/green"

remote_input_folder="/home/dataset/${name}"
remote_output_folder="$home_folder/dataset/big_output"
remote_save_folder="$home_folder/validation/${name}"

master_unsorted_folder="$home_folder/validation/unsorted_data"
master_sorted_folder="$home_folder/validation/sorted_data"
output_file="$home_folder/validation/size_validation_merged.txt"

set -e
rm -r "$master_unsorted_folder" || true
rm -r "$master_sorted_folder" || true
mkdir -p "$master_unsorted_folder"
mkdir -p "$master_sorted_folder"

for worker in "${workers[@]}"; do
    echo ">> Processing $worker..."

    # calculate input files size & send
    ssh "$worker" "rm -r \"$remote_save_folder\" || true"
    ssh "$worker" "mkdir -p \"$remote_save_folder\""

    ssh "$worker" "du -sb $remote_input_folder > $remote_save_folder/input_size_\"$worker\".txt"
    scp "$worker:$remote_save_folder/input_size_\"$worker\".txt" "$master_unsorted_folder/"

    # calculate output files size & send
    ssh "$worker" "du -sb $remote_output_folder > $remote_save_folder/output_size_\"$worker\".txt"
    scp "$worker:$remote_save_folder/output_size_\"$worker\".txt" "$master_sorted_folder/"
    
    echo " "
done

# init variables
total_sorted_size=0
total_unsorted_size=0

for file in "$master_unsorted_folder"/*; do
    if [ -f "$file" ]; then
        size=$(awk '{print $1}' "$file")
        total_unsorted_size=$((total_unsorted_size + size))
    fi
done

for file in "$master_sorted_folder"/*; do
    if [ -f "$file" ]; then
        size=$(awk '{print $1}' "$file")
        total_sorted_size=$((total_sorted_size + size))
    fi
done

echo ">> total sorted size: $total_sorted_size"
echo ">> total unsorted size: $total_unsorted_size"

echo "total sorted size: $total_sorted_size" >> "$output_file"
echo "total unsorted size: $total_unsorted_size" >> "$output_file"

echo ">> complete size validation."
