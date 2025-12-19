# -*- coding: utf-8 -*-
# =====================================================================
# rent_processed.py  
# Spark 1.3 + Clean CSV + 법정동 5자리 필터 + CSV 저장 (keywords_code 사용 X)
# =====================================================================

from pyspark import SparkContext
import re

sc = SparkContext(appName="rent_processed_final")
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
# 2) 법정동 5자리 코드 필터
# =====================================================================

TARGET_CODE_MAP = {
    "Gongneung": ["11300"],
    "Seongsu":   ["11400", "11500"],
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
# 3) Load clean rent CSV
# =====================================================================

RAW_PATH = "hdfs:///user/cloudera/raw/rent/*.csv"

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

    # 원본 행정동 코드 (대개 10자리)
    region10 = clean_str(row.get("STDG_CD"))
    if region10 is None or len(region10) < 5:
        return None

    # rent는 5자리 법정동 prefix 기준
    region5 = region10[:5]

    if region5 not in ALL_CODES:
        return None

    year, month = parse_date(row.get("CTRT_DAY"))

    return {
        "region_code": region5,   # 5자리 법정동 코드
        "year": year,
        "month": month,
        "deposit": clean_float(row.get("GRFE")),
        "monthly_rent": clean_float(row.get("RTFE")),
        "prev_deposit": clean_float(row.get("BFR_GRFE")),
        "prev_monthly_rent": clean_float(row.get("BFR_RTFE"))
    }


parsed = data.map(parse_row).filter(lambda x: x is not None)


# =====================================================================
# 5) Convert to CSV
# =====================================================================

def to_csv(r):
    def s(v): return "" if v is not None else ""
    return ",".join([
        r["region_code"],
        str(r["year"]),
        str(r["month"]),
        str(r["deposit"]),
        str(r["monthly_rent"]),
        str(r["prev_deposit"]),
        str(r["prev_monthly_rent"])
    ])


# =====================================================================
# 6) Save to HDFS (single unified output)
# =====================================================================

OUT_PATH = "hdfs:///user/cloudera/processed/rent"

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
p = sc._jvm.org.apache.hadoop.fs.Path(OUT_PATH)

if fs.exists(p):
    fs.delete(p, True)

parsed.map(to_csv).saveAsTextFile(OUT_PATH)

print("=== Rent Processed: FINAL (5-digit legal dong filter) Completed ===")
