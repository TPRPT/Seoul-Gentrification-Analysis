#!/bin/bash
# =========================================================
# data_collect_json.sh
# 서울 열린데이터포털 JSON API (2020~2025)
# 부동산 실거래가, 전월세가, 상권매출 데이터 자동 수집 파이프라인
# =========================================================

API_KEY="YOUR_API_KEY"
LOCAL_BASE="/home/training/Desktop/raw"
HDFS_BASE="/user/cloudera/raw"

YEARS=(2020 2021 2022 2023 2024 2025)
STEP=1000
END_LIMIT=20000

# jq 확인 (사전 설치 필요)
if ! command -v jq &> /dev/null; then
  echo "[INFO] jq not found. Installing..."
  sudo yum install -y epel-release
  sudo yum install -y jq
fi

mkdir -p ${LOCAL_BASE}/{real_estate,rent,sales}

# =========================================================
# ① 부동산 실거래가 tbLnOpendataRtmsV
# =========================================================
echo "[INFO] Collecting Real Estate data..."
for year in "${YEARS[@]}"; do
  OUTFILE="${LOCAL_BASE}/real_estate/real_estate_${year}.csv"
  : > "$OUTFILE"
  for ((start=1; start<=END_LIMIT; start+=STEP)); do
    end=$((start+STEP-1))
    URL="http://openapi.seoul.go.kr:8088/${API_KEY}/json/tbLnOpendataRtmsV/${start}/${end}/${year}"
    echo "  → ${year} [${start}-${end}]"
    curl -s "$URL" | jq -r '
      .tbLnOpendataRtmsV.row? // [] |
      .[] | [
        .RCPT_YR, .CGG_CD, .CGG_NM, .STDG_CD, .STDG_NM,
        .LOTNO_SE, .LOTNO_SE_NM, .MNO, .SNO, .BLDG_NM,
        .CTRT_DAY, .THING_AMT, .ARCH_AREA, .LAND_AREA, .FLR, .BLDG_USG
      ] | @csv
    ' >> "$OUTFILE"
  done
done

# =========================================================
# ② 전월세가 tbLnOpendataRentV
# =========================================================
echo "[INFO] Collecting Rent data..."
for year in "${YEARS[@]}"; do
  OUTFILE="${LOCAL_BASE}/rent/rent_${year}.csv"
  : > "$OUTFILE"
  for ((start=1; start<=END_LIMIT; start+=STEP)); do
    end=$((start+STEP-1))
    URL="http://openapi.seoul.go.kr:8088/${API_KEY}/json/tbLnOpendataRentV/${start}/${end}/${year}"
    echo "  → ${year} [${start}-${end}]"
    curl -s "$URL" | jq -r '
      .tbLnOpendataRentV.row? // [] |
      .[] | [
        .RCPT_YR, .CGG_CD, .CGG_NM, .STDG_CD, .STDG_NM,
        .LOTNO_SE, .MNO, .SNO, .FLR, .CTRT_DAY,
        .RENT_SE, .GRFE, .RTFE, .BLDG_USG
      ] | @csv
    ' >> "$OUTFILE"
  done
done

# =========================================================
# ③ 상권 매출 VwsmTrdarSelngQq
# =========================================================
echo "[INFO] Collecting Sales data..."
QUARTERS=(20201 20202 20203 20204 20211 20212 20213 20214 20221 20222 20223 20224 20231 20232 20233 20234 20241 20242 20243 20244 20251 20252)
for quarter in "${QUARTERS[@]}"; do
  OUTFILE="${LOCAL_BASE}/sales/sales_${quarter}.csv"
  : > "$OUTFILE"
  for ((start=1; start<=END_LIMIT; start+=STEP)); do
    end=$((start+STEP-1))
    URL="http://openapi.seoul.go.kr:8088/${API_KEY}/json/VwsmTrdarSelngQq/${start}/${end}/${quarter}"
    echo "  → ${quarter} [${start}-${end}]"
    curl -s "$URL" | jq -r '
      .VwsmTrdarSelngQq.row? // [] |
      .[] | [
        .STDR_YYQU_CD, .TRDAR_CD, .TRDAR_CD_NM,
        .THSMON_SELNG_AMT, .ML_SELNG_AMT, .FML_SELNG_AMT
      ] | @csv
    ' >> "$OUTFILE"
  done
done

# =========================================================
# ④ HDFS 업로드 + Hive 테이블 등록
# =========================================================
echo "[INFO] Uploading CSVs to HDFS..."
hdfs dfs -mkdir -p ${HDFS_BASE}/{real_estate,rent,sales}
hdfs dfs -put -f ${LOCAL_BASE}/real_estate/*.csv ${HDFS_BASE}/real_estate/
hdfs dfs -put -f ${LOCAL_BASE}/rent/*.csv ${HDFS_BASE}/rent/
hdfs dfs -put -f ${LOCAL_BASE}/sales/*.csv ${HDFS_BASE}/sales/

echo "[INFO] Registering Hive external tables..."
beeline -u "jdbc:hive2://localhost:10000/default" -n cloudera -p cloudera -e "
DROP TABLE IF EXISTS real_estate_raw;
CREATE EXTERNAL TABLE real_estate_raw (
  RCPT_YR STRING, CGG_CD STRING, CGG_NM STRING, STDG_CD STRING, STDG_NM STRING,
  LOTNO_SE STRING, LOTNO_SE_NM STRING, MNO STRING, SNO STRING, BLDG_NM STRING,
  CTRT_DAY STRING, THING_AMT STRING, ARCH_AREA STRING, LAND_AREA STRING, FLR STRING, BLDG_USG STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/real_estate/';

DROP TABLE IF EXISTS rent_raw;
CREATE EXTERNAL TABLE rent_raw (
  RCPT_YR STRING, CGG_CD STRING, CGG_NM STRING, STDG_CD STRING, STDG_NM STRING,
  LOTNO_SE STRING, MNO STRING, SNO STRING, FLR STRING, CTRT_DAY STRING,
  RENT_SE STRING, GRFE STRING, RTFE STRING, BLDG_USG STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/rent/';

DROP TABLE IF EXISTS sales_raw;
CREATE EXTERNAL TABLE sales_raw (
  STDR_YYQU_CD STRING, TRDAR_CD STRING, TRDAR_CD_NM STRING,
  THSMON_SELNG_AMT STRING, ML_SELNG_AMT STRING, FML_SELNG_AMT STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/sales/';
"

echo "[DONE] Real Estate, Rent, Sales datasets (2020–2025) collected and loaded into Hive successfully."
