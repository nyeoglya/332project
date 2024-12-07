#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111")
name="$1"
[ -z "$name" ] && { echo ">> error: No name provided." >&2; exit 1; }
home_folder="/home/green"

remote_folder="$home_folder/dataset/${name}_output"
remote_save_folder="$home_folder/validation/${name}"

master_local_folder="$home_folder/validation/headtail"
output_file="$home_folder/validation/validation_merged_file.txt"

set -e
rm -r "$master_local_folder" || true
mkdir -p "$master_local_folder"

for worker in "${workers[@]}"; do
    echo ">> Processing $worker..."

    ssh "$worker" "find \"$remote_folder\" -name 'headtail_*' -exec rm {} \;"

    # valsort & send results
    ssh "$worker" "
        remote_home=\"$home_folder\"
        for FILE in $remote_folder/*; do
            ./valsort \"\$FILE\"
        done
    "
    
    ssh "$worker" "rm -r \"$remove_save_folder\" || true"
    ssh "$worker" "mkdir -p \"$remote_save_folder\""

    # extract head & tail from files
    ssh "$worker" "
        for FILE in $remote_folder/*; do
            BASENAME=\$(basename \"\$FILE\")
            head -n 1 \"\$FILE\" > \"$remote_save_folder/headtail_\$BASENAME.txt\"
            tail -n 1 \"\$FILE\" >> \"$remote_save_folder/headtail_\$BASENAME.txt\"
        done
    "

    # send head & tail to master
    scp "$worker:$remote_save_folder/headtail_*" "$master_local_folder/"
    
    echo " "
done

> "$output_file"
for file in $(ls "$master_local_folder" | sort); do
    if [ -f "$master_local_folder/$file" ]; then
        cat "$master_local_folder/$file" >> "$output_file"
    fi
done

"$home_folder/valsort" "$output_file" # final valsort result file

echo ">> complete validation."
