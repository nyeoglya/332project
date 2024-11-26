#!/bin/bash

shell_path="/home/green/332project/shellScript"
$shell_path/transfer_test.sh "small" 10 1 2 320000
$shell_path/transfer_test.sh "big" 10 1 10 320000
$shell_path/transfer_test.sh "large" 10 1 1000 320000


