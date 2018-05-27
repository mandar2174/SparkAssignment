#!/bin/bash

echo "Creating hive table"

#copy data from local to hdfs(Note : Copy data from hive user because we are creating table using hive so that hive can access data)
su hive

hdfs dfs -mkdir /tmp/weatherdata1/
hdfs dfs -put /tmp/2017.csv /tmp/weatherdata1/
hdfs dfs -ls /tmp/weatherdata1/

#create hive table on top data

hive -f create-table.hql

