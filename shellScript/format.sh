#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

root="/home/green"

echo "Formatting master..."
rm -rf $root/dataset
mkdir -p $root/dataset
rm -f $root/master
rm -f $root/worker

for worker in "${workers[@]}"; do
	echo "Formatting $worker..."
	ssh green@$worker "rm -rf $root/* && mkdir -p $root/dataset"
done


echo "Formatting done."
