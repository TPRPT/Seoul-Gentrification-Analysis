#!/bin/bash

VM="training@10.50.35.9"
HDFS_BASE="/user/cloudera/raw"
CLEAN_DIR="./cleaned_data"

ssh_base="ssh -o HostKeyAlgorithms=+ssh-rsa"

echo "=== Uploading data to HDFS ==="

upload_cleaned() {
  dataset=$1

  echo ""
  echo "=== DATASET: $dataset ==="

  # Ensure HDFS folder exists
  $ssh_base $VM "hdfs dfs -mkdir -p ${HDFS_BASE}/${dataset}"

  for file in ${CLEAN_DIR}/${dataset}/*.csv; do
    fname=$(basename "$file")
    echo " → Uploading $fname"

    cat "$file" | $ssh_base $VM \
      "hdfs dfs -put -f - ${HDFS_BASE}/${dataset}/${fname}"
  done
}

upload_cleaned "real_estate"
upload_cleaned "rent"
upload_cleaned "sales"
upload_cleaned "population"

echo ""
echo "Data upload complete (HDFS /raw replaced with clean versions!)"
