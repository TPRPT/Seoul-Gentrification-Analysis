# -*- coding: utf-8 -*-
# =====================================================================
# real_estate_processed.py  
# Spark 1.3 + Clean CSV + 법정동 5자리 필터링 + CSV 저장
# =====================================================================

from pyspark import SparkContext
import re

sc = SparkContext(appName="real_estate_processed_final")
logger = sc._jvm.org.apache.log4j.LogManager.getRootLogger()
logger.setLevel(sc._jvm.org.apache.log4j.Level.ERROR)


# =====================================================================
# 1) Helpers
# =====================================================================

def clean_str(x):
    if x is None:
        return None
    x = x.strip().replace('"', '')
    return x if x != "" else None

def clean_float(x):
    try:
        s = clean_str(x)
        return float(s) if s not in (None, "") else None
    except:
        return None

def clean_int(x):
    try:
        s = clean_str(x)
        return int(s) if s not in (None, "") else None
    except:
        return None

def parse_date(x):
    """ CTRT_DAY = YYYYMMDD → (year, month) """
    s = clean_str(x)
    if s is None:
        return None, None
    s = re.sub(r"\D", "", s)
    if len(s) < 6:
        return None, None
    return int(s[:4]), int(s[4:6])


# =====================================================================
# 2) 법정동 코드 필터 (5자리)
# =====================================================================

TARGET_CODE_MAP = {
    "Gongneung": ["11300"],
    "Seongsu":   ["11400", "11500"],   # 성수1가/성수2가
    "Sinsa":     ["10900"],
    "Yeonnam":   ["12400"],
    "Euljiro": [
        "10400","10500","15500","15000","15100","14800","14900"
    ],
    "Ikseon": ["13300"],
    "Hannam": ["13100"]
}

ALL_CODES = sum(TARGET_CODE_MAP.values(), [])


# =====================================================================
# 3) Load clean CSV
# =====================================================================

RAW_PATH = "hdfs:///user/cloudera/raw/real_estate/*.csv"
raw = sc.textFile(RAW_PATH)

header = raw.first()
columns = header.split(",")
data = raw.filter(lambda x: x != header)


# =====================================================================
# 4) Parse each row
# =====================================================================

def parse_row(line):
    parts = line.split(",")
    if len(parts) != len(columns):
        return None

    row = dict(zip(columns, parts))

    # 원본 지역코드 (법정동 5자리 or 10자리 형태)
    region_raw = clean_str(row.get("STDG_CD"))
    if region_raw is None:
        return None

    # ---- 법정동 5자리 prefix ----
    region5 = region_raw[:5]

    # 관심 구역만 필터링
    if region5 not in ALL_CODES:
        return None

    year, month = parse_date(row.get("CTRT_DAY"))

    return {
        "region_code": region5,
        "year": year,
        "month": month,
        "trade_price": clean_float(row.get("THING_AMT")),
        "arch_area": clean_float(row.get("ARCH_AREA")),
        "land_area": clean_float(row.get("LAND_AREA")),
        "arch_year": clean_int(row.get("ARCH_YR"))
    }


parsed = data.map(parse_row).filter(lambda x: x is not None)


# =====================================================================
# 5) CSV 변환
# =====================================================================

def row_to_csv(r):
    def s(v): return "" if v is None else str(v)
    return ",".join([
        r["region_code"],
        s(r["year"]),
        s(r["month"]),
        s(r["trade_price"]),
        s(r["arch_area"]),
        s(r["land_area"]),
        s(r["arch_year"])
    ])


# =====================================================================
# 6) Save to HDFS (single folder)
# =====================================================================

OUT_PATH = "hdfs:///user/cloudera/processed/real_estate"

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
p = sc._jvm.org.apache.hadoop.fs.Path(OUT_PATH)

if fs.exists(p):
    fs.delete(p, True)

parsed.map(row_to_csv).saveAsTextFile(OUT_PATH)

print("=== Real Estate Processed: FINAL (5-digit legal dong filter) Completed ===")
