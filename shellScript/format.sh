#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

for worker in "${workers[@]}"; do
  echo "Formatting master..."
  rm -rf files
  mkdir -p files
	echo "Formatting $worker..."
	ssh green@$worker "rm -rf files && rm -f gensort && rm -f valsort && rm -f project.jar"
done


echo "Formatting done."
