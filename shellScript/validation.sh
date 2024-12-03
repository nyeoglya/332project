#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")
home_folder="/home/green"
remote_folder="$home_folder/dataset/large_output"
master_local_folder="$home_folder/validation"
output_file="$home_folder/validation/merged_file"

set -e
mkdir -p "$master_local_folder"

for worker in "${workers[@]}"; do
    echo "> Processing $worker..."

    # valsort & send results
    ssh "$worker" "
        for FILE in $remote_folder/*; do 
            ./valsort \"\$FILE\" > \"$home_folder/${worker}_valsort_result.txt\"
        done
    "

    # scp "$worker:$home_folder/${worker}_valsort_result.txt" "$master_local_folder/"
    
    # extract head & tail from files
    ssh "$worker" "
        for FILE in $remote_folder/*; do
            BASENAME=\$(basename \"\$FILE\")
            head -n 1 \"\$FILE\" > \"$remote_folder/headtail_\$BASENAME.txt\"
            tail -n 1 \"\$FILE\" >> \"$remote_folder/headtail_\$BASENAME.txt\"
        done
    "

    # send head & tail to master
    scp "$worker:$remote_folder/headtail_*" "$master_local_folder/"
    
    echo "> successfully move headtail from $worker."
done

# merge files
find "$master_local_folder" -type f | sort | while read -r FILE; do
  echo "> merge $master_local_folder..."
  cat "$FILE" >> "$output_file"
  echo -e "\n" >> "$output_file" # add \n between files
done

valsort $output_file > $home_folder/result.txt # final valsort result file

echo "Complete work from worker. Results were saved in $master_local_folder."
