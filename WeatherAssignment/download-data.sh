#!/bin/bash

echo "Started downloading data"

mkdir -p /tmp/spark-assignment
cd /tmp/spark-assignment

#download weather data
wget -c  ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2017.csv.gz

#Verify downloaded file or not
ls -alth .

#Extract downloaded data
gunzip 2017.csv.gz

#verify whether data extracted successfully for not
ls -alth .

#Note : you can see some sample record on screen
head -n 5 2017.csv

