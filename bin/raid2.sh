#!/bin/bash
umount /dev/xvdb
umount /dev/xvdc
mdadm --create --verbose /dev/md0 --level=stripe --raid-devices=2 /dev/xvdb /dev/xvdc
mkfs.ext4 /dev/md0

rm -Rf /mfshdd
mkdir /mfshdd
chown -R mfs:mfs /mfshdd
mount /dev/md0 /mfshdd
chown -R mfs:mfs /mfshdd

echo "MFSCHUNKSERVER_ENABLE=true" | tee --append /etc/default/moosefs-ce-chunkserver
service moosefs-ce-chunkserver start
mfsmount /data -H mfsmaster
