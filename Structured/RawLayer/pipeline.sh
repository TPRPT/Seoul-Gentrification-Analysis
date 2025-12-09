#!/bin/bash
# ==============================================================
# pipeline.sh
# Daily automated structured-data pipeline (Full ETL + SGI)
# --------------------------------------------------------------
# Executes:
#   1) OpenAPI Daily Collection (data.sh)
#   2) Optional Historical Upload (raw_upload_data.sh)
#   3) Spark ETL for all datasets (run_processed.sh)
#   4) Hive external table refresh (hive_processed.sh)
#   5) SGI analysis (analysis.py)
# ==============================================================

BASE_DIR="/home/training/DataPipeline"
RAW_LAYER="${BASE_DIR}/RawLayer"
PROCESSED_LAYER="${BASE_DIR}/ProcessedLayer"
ANALYSIS_LAYER="${BASE_DIR}/AnalysisLayer"

LOG_DIR="${BASE_DIR}/logs"
mkdir -p $LOG_DIR

LOG_FILE="${LOG_DIR}/pipeline_$(date +'%Y%m%d_%H%M').log"

echo "==========================================================" | tee -a $LOG_FILE
echo "[PIPELINE START] $(date)" | tee -a $LOG_FILE
echo "==========================================================" | tee -a $LOG_FILE


# ---------------------------------------------------------------
# Step 0. Ensure HDFS Safe Mode is OFF
# ---------------------------------------------------------------
echo "[0] Checking HDFS safemode..." | tee -a $LOG_FILE
hdfs dfsadmin -safemode leave >> $LOG_FILE 2>&1


# ---------------------------------------------------------------
# Step 1. Run Daily OpenAPI Collector
# ---------------------------------------------------------------
echo "[1] Running data.sh (OpenAPI daily collector)..." | tee -a $LOG_FILE
bash ${RAW_LAYER}/data.sh >> $LOG_FILE 2>&1

if [ $? -ne 0 ]; then
    echo "[ERROR] data.sh failed. Terminating pipeline." | tee -a $LOG_FILE
    exit 1
fi


# ---------------------------------------------------------------
# Step 2. Upload Raw CSV to HDFS (Daily or Historical)
# ---------------------------------------------------------------
echo "[2] Running raw_upload_data.sh (Upload CSV to HDFS RAW)..." | tee -a $LOG_FILE
bash ${RAW_LAYER}/raw_upload_data.sh >> $LOG_FILE 2>&1

if [ $? -ne 0 ]; then
    echo "[ERROR] raw_upload_data.sh failed. Terminating pipeline." | tee -a $LOG_FILE
    exit 1
fi


# ---------------------------------------------------------------
# Step 3. Run Spark ETL for All Datasets
# ---------------------------------------------------------------
echo "[3] Running run_processed.sh (Spark ETL)..." | tee -a $LOG_FILE
bash ${PROCESSED_LAYER}/run_processed.sh >> $LOG_FILE 2>&1

if [ $? -ne 0 ]; then
    echo "[ERROR] run_processed.sh failed. Terminating pipeline." | tee -a $LOG_FILE
    exit 1
fi


# ---------------------------------------------------------------
# Step 4. Refresh Hive External Tables
# ---------------------------------------------------------------
echo "[4] Running hive_processed.sh (Hive table creation)..." | tee -a $LOG_FILE
bash ${PROCESSED_LAYER}/hive_processed.sh >> $LOG_FILE 2>&1

if [ $? -ne 0 ]; then
    echo "[ERROR] hive_processed.sh failed. Terminating pipeline." | tee -a $LOG_FILE
    exit 1
fi


# ---------------------------------------------------------------
# Step 5. Run SGI Analysis (PySpark)
# ---------------------------------------------------------------
echo "[5] Running SGI analysis (analysis.py)..." | tee -a $LOG_FILE
spark-submit ${ANALYSIS_LAYER}/analysis.py >> $LOG_FILE 2>&1

if [ $? -ne 0 ]; then
    echo "[ERROR] SGI analysis failed. Terminating pipeline." | tee -a $LOG_FILE
    exit 1
fi


# ---------------------------------------------------------------
# Completed
# ---------------------------------------------------------------
echo "==========================================================" | tee -a $LOG_FILE
echo "[PIPELINE COMPLETE] $(date)" | tee -a $LOG_FILE
echo "Logs saved to: $LOG_FILE" | tee -a $LOG_FILE
echo "==========================================================" | tee -a $LOG_FILE

exit 0
