#!/bin/bash

echo "Started running spark assignment processing"

#Download attached assignment zip and copy to /tmp/ folder

mkdir -p /tmp/
git clone https://github.com/mandar2174/SparkAssignment.git
cd  WeatherAssignment

#move to WeatherAssignment folder
cd /tmp/WeatherAssignment
chmod -r 755 /tmp/WeatherAssignment

#download weather data 
bash download-data.sh

#create weatherraw and WeatherCurated table(Note :Run this command via HIVE user by [su hive])
bash create-table.sh

#run spark job to process weather data (Note : Run this command via Hive user by [su hive])
bash run-spark-job.sh
