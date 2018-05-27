#!/bin/bash

echo "Run spark program"

export SPARK_MAJOR_VERSION=2

#Before running program make sure you have change hive.metastore.uris parameter as per HDP Cluster
spark-submit --master yarn --deploy-mode client spark-job.py

