#!/bin/bash

pip3 install --upgrade google-cloud-pubsub google-cloud-bigquery
sleep 1
nohup python3 ./pubsub2bq.py $1 $2 $3 $4 > /dev/null 2>&1 &
