#!/bin/bash
#
# Usage: copy_data.sh 20
# 20 is the total number of copies to be make
#
cp ~/TestData/FoxWF_Montage_6.0_Example.tar.gz .
tar zxvf FoxWF_Montage_6.0_Example.tar.gz
mv FoxWF_Montage_6.0_Example Test-0
rm Test-0/timeout.xml
for (( i=1; i<=$1; i++ ))
do
  cp -r Test-0 Test-$i
done


