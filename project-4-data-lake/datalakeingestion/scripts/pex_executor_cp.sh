#!/bin/bash
# Author: Ernani Britto
# Version: 2021-04-27
# Wrapper to execute a PEX archive via an EMR Step
# Step type: Custom JAR
# JAR location: s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar
# Arguments:
#      s3://.../pex-executer.sh
#      s3://.../some-etl-job.pex
#      HADOOP_HOME=/usr/lib/hadoop
#      SPARK_HOME=/usr/lib/spark
aws s3 cp $1 .
chmod +x $(basename -- $1);
shift;
eval "$@"