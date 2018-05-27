#!/bin/bash

#Main program for spark assignment
mkdir -p /tmp/
git clone https://github.com/mandar2174/SparkAssignment.git
cd WeatherAssignment

#download weather data 
bash download-data.sh

#create weatherraw and WeatherCurated table
bash create-table.sh

#run spark job to process weather data 
bash run-spark-job.sh
