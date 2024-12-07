#!/bin/bash

path="/home/green/332project/shellScript"

chmod +x $path/activate_test.sh
chmod +x $path/format.sh
chmod +x $path/transfer_gensort.sh
chmod +x $path/deploy_project.sh
chmod +x $path/transfer_test.sh
chmod +x $path/transfer_tests.sh
chmod +x $path/validation.sh
chmod +x $path/size_validation.sh
chmod +x $path/kill_process.sh

$path/format.sh
$path/transfer_gensort.sh
$path/deploy_project.sh
$path/transfer_tests.sh
