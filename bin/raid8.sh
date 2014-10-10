#!/bin/bash
mdadm --create --verbose /dev/md0 --level=stripe --raid-devices=8 /dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde /dev/xvdf /dev/xvdg /dev/xvdh /dev/xvdi
mkfs.ext4 /dev/md0

rm -Rf /mfshdd
mkdir /mfshdd
chown -R mfs:mfs /mfshdd
mount /dev/md0 /mfshdd
