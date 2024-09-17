#!/usr/bin/env/ bash

a=$(date +'%d')
b=23

if [ $a -eq $b ];
then
	python3 /mnt/d/Chicago_traffic_data/fatalities.py ;
else
	echo "Today is not month start, so this file wont run";
fi
