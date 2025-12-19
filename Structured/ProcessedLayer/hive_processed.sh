#!/bin/bash
# ===================================================================
# hive_processed.sh (PowerShell-safe version)
# ===================================================================

VM_USER="training"
VM_HOST="10.50.35.9"

SSH_BASE="ssh -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"

HIVE_CMD="beeline -u jdbc:hive2://localhost:10000/default -n cloudera -p cloudera"


# ===================================================================
# 1. REAL ESTATE TABLE
# ===================================================================
echo "[1] Create real_estate_processed table"

$SSH_BASE ${VM_USER}@${VM_HOST} "
${HIVE_CMD} << 'EOF'

DROP TABLE IF EXISTS real_estate_processed;

CREATE EXTERNAL TABLE real_estate_processed (
    region_code STRING,
    year INT,
    month INT,
    trade_price DOUBLE,
    arch_area DOUBLE,
    land_area DOUBLE,
    arch_year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/processed/real_estate';

EOF
"


# ===================================================================
# 2. RENT TABLE
# ===================================================================
echo "[2] Create rent_processed table"

$SSH_BASE ${VM_USER}@${VM_HOST} "
${HIVE_CMD} << 'EOF'

DROP TABLE IF EXISTS rent_processed;

CREATE EXTERNAL TABLE rent_processed (
    region_code STRING,
    year INT,
    month INT,
    deposit DOUBLE,
    monthly_rent DOUBLE,
    prev_deposit DOUBLE,
    prev_monthly_rent DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/processed/rent';

EOF
"


# ===================================================================
# 3. SALES TABLE
# ===================================================================
echo "[3] Create sales_processed table"

$SSH_BASE ${VM_USER}@${VM_HOST} "
${HIVE_CMD} << 'EOF'

DROP TABLE IF EXISTS sales_processed;

CREATE EXTERNAL TABLE sales_processed (
    region_code STRING,
    year INT,
    quarter INT,
    total_sales DOUBLE,
    weekday_sales DOUBLE,
    weekend_sales DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/processed/sales';

EOF
"


# ===================================================================
# 4. POPULATION TABLE
# ===================================================================
echo "[4] Create population_processed table"

$SSH_BASE ${VM_USER}@${VM_HOST} "
${HIVE_CMD} << 'EOF'

DROP TABLE IF EXISTS population_processed;

CREATE EXTERNAL TABLE population_processed (
    region_code STRING,
    year INT,
    month INT,
    total_pop DOUBLE,
    pop_age_20_39 DOUBLE,
    pop_age_60plus DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/processed/population';

EOF
"

echo "Hive processed tables created successfully!"