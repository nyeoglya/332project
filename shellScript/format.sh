#!/bin/bash

workers=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110")

for worker in "${workers[@]}"; do
  echo "Formatting master..."
  rm -rf dataset
  mkdir -p dataset
  rm -f master
  rm -f worker
	echo "Formatting $worker..."
	ssh green@$worker "rm -rf dataset && rm -f gensort && rm -f valsort && rm -rf 332project && rm -f master && rm -f worker && mkdir -p dataset"
done


echo "Formatting done."
