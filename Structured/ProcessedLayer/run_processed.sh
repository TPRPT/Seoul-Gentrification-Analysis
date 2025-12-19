#!/bin/bash
# ===================================================================
# run_processed.sh (PowerShell-safe + Spark 1.3)
# ===================================================================

VM_USER="training"
VM_HOST="10.50.35.9"

SSH_BASE="ssh -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"
SCP_BASE="scp -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"

REMOTE_DIR="/home/training/etl_run"
SPARK="/usr/bin/spark-submit"

# ---------------------------------------------------------------
# ETL Python scripts 목록
# ---------------------------------------------------------------
SCRIPTS=(
  "real_estate_processed.py"
  "rent_processed.py"
  "sales_processed.py"
  "population_processed.py"
)

# ---------------------------------------------------------------
# Keyword CSV 파일  ← 반드시 canonical code 버전!
# ---------------------------------------------------------------
KEYWORD_FILE="keywords_code.csv"


echo "[1] Create remote folder"
$SSH_BASE ${VM_USER}@${VM_HOST} "mkdir -p ${REMOTE_DIR}"

echo "[1-1] Ensure HDFS processed root exists"
$SSH_BASE ${VM_USER}@${VM_HOST} "hdfs dfs -mkdir -p /user/cloudera/processed"


echo "[2] Upload ETL scripts"
for s in "${SCRIPTS[@]}"; do
  echo " → Upload: $s"
  $SCP_BASE "./$s" ${VM_USER}@${VM_HOST}:${REMOTE_DIR}/
done

echo "[2-1] Upload keyword CSV (UTF-8)"
if [ -f "$KEYWORD_FILE" ]; then
  echo " → Upload keyword file: $KEYWORD_FILE"
  $SCP_BASE "./$KEYWORD_FILE" ${VM_USER}@${VM_HOST}:${REMOTE_DIR}/
else
  echo " ⚠ WARNING: $KEYWORD_FILE not found locally!"
fi


echo "[3] Run Spark ETL"
for s in "${SCRIPTS[@]}"; do
  echo " → Running: $s"
  $SSH_BASE ${VM_USER}@${VM_HOST} "$SPARK ${REMOTE_DIR}/${s}"
done


echo "[4] Cleanup remote scripts (optional)"
$SSH_BASE ${VM_USER}@${VM_HOST} "rm -f ${REMOTE_DIR}/*.py"


echo "Processed ETL complete"
