#!/bin/bash
#
# Usage runwf.sh 10 /FoxData/c3.8xlarge
#     10 is the total number of runs
#     /FoxData/c3.8xlarge is the root path to all workflows
# 
cd ~/FoxWF
for (( i=1; i<=$1; i++ ))
do
  java FoxSubmit Test-$i $2/Test-$i
done
