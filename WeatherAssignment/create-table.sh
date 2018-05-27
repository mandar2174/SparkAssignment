#!/bin/bash

echo "Creating hive table"

#copy data from local to hive(Note : Copy data from hive user because we are creating table using hive so that hive can access data)
#su hive

hdfs dfs -mkdir /tmp/weatherdata/
hdfs dfs -put /tmp/WeatherAssignment/spark-assignment/2017.csv /tmp/weatherdata/
hdfs dfs -ls /tmp/weatherdata/

#create hive table on top data
hive -f create-table.hql

#provide hive user permission to read data from hdfs
hdfs dfs -chmod -R 755  /tmp/weatherdata/


