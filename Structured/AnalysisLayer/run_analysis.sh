#!/bin/bash

VM="training@10.50.35.9"
SSH="ssh -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"
SCP="scp -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"

echo "=== 1) Run local PySpark analysis ==="
python3 analysis.py

echo "=== 2) Upload final_analysis.csv to VM ==="
$SCP ./final_analysis.csv ${VM}:~/

echo "=== 3) Put into HDFS ==="
$SSH $VM "hdfs dfs -mkdir -p /user/cloudera/analysis"
$SSH $VM "hdfs dfs -put -f ~/final_analysis.csv /user/cloudera/analysis/"

echo "=== 4) Create Hive Table ==="
$SSH $VM "hive -e \"
CREATE EXTERNAL TABLE IF NOT EXISTS analysis (
    region_code STRING,
    time_id STRING,
    price_change_rate DOUBLE,
    rent_change_rate DOUBLE,
    sales_growth_rate DOUBLE,
    youth_inflow_rate DOUBLE,
    senior_outflow_rate DOUBLE,
    GPI_score DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/user/cloudera/analysis';
\""

echo "All done!"
