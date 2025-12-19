# -*- coding: utf-8 -*-
# =====================================================================
# sales_processed.py  
# Spark 1.3 safe + Keyword CSV flexible loader + region_code only + CSV Save
# =====================================================================

from pyspark import SparkContext
import csv
import re
import os

sc = SparkContext(appName="sales_processed_final_csv")
logger = sc._jvm.org.apache.log4j.LogManager.getRootLogger()
logger.setLevel(sc._jvm.org.apache.log4j.Level.ERROR)

# =====================================================================
# TARGET region_code (final 5 digits)
# =====================================================================
REGION_CODE_MAP = {
    "Gongneung": "11300",
    "Seongsu":   "11400",
    "Sinsa":     "10900",
    "Yeonnam":   "12400",
    "Euljiro":   "10400",
    "Ikseon":    "13300",
    "Hannam":    "13100"
}

# =====================================================================
# 1) Load Keyword CSV safely (2 or 3 columns)
# =====================================================================

KEYWORD_FILE = "/home/training/etl_run/keywords_sales.csv"

keyword_map = {}   # dong → list of (keyword, region_code)

with open(KEYWORD_FILE, "r") as f:
    reader = csv.reader(f)

    header = next(reader, None)  # skip header if exists

    for row in reader:
        if not row or len(row) < 2:
            continue
        
        dong = row[0].strip()
        keyword_utf8 = row[1].strip()

        # region_code 결정 우선순위: CSV third column > default mapping
        if len(row) >= 3 and row[2].strip() != "":
            region_code = row[2].strip()
        else:
            region_code = REGION_CODE_MAP.get(dong)

        if region_code is None:
            continue

        # python2 UTF-8 safe decode
        try:
            keyword = keyword_utf8.decode("utf-8")
        except:
            keyword = keyword_utf8

        keyword_map.setdefault(dong, []).append((keyword, region_code))

# =====================================================================
# 2) Helpers
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


def parse_quarter(code):
    """ 20231 → (2023, 1) """
    s = clean_str(code)
    if s is None:
        return None, None
    s = re.sub(r"\D", "", s)
    if len(s) < 5:
        return None, None
    return int(s[:4]), int(s[4])

# =====================================================================
# 3) Load clean CSV
# =====================================================================

RAW_PATH = "hdfs:///user/cloudera/raw/sales/*.csv"
raw = sc.textFile(RAW_PATH)

header = raw.first()
columns = header.split(",")

data = raw.filter(lambda x: x != header)

# =====================================================================
# 4) Parse sales row
# =====================================================================

def parse_row(line):
    parts = line.split(",")
    if len(parts) != len(columns):
        return None

    row = dict(zip(columns, parts))

    nm = clean_str(row.get("TRDAR_CD_NM"))
    if nm is None:
        return None

    try:
        nm = nm.decode("utf-8")
    except:
        pass

    year, quarter = parse_quarter(row.get("STDR_YYQU_CD"))

    return {
        "name": nm,
        "year": year,
        "quarter": quarter,
        "total_sales": clean_float(row.get("THSMON_SELNG_AMT")),
        "weekday_sales": clean_float(row.get("MDWK_SELNG_AMT")),
        "weekend_sales": clean_float(row.get("WKEND_SELNG_AMT"))
    }

parsed = data.map(parse_row).filter(lambda x: x is not None)

# =====================================================================
# 5) Keyword → region_code 매핑
# =====================================================================

def apply_keywords(r):
    out = []
    nm = r["name"]

    for dong, kw_list in keyword_map.items():
        for kw, region_code in kw_list:
            if kw in nm:   # substring match
                out.append({
                    "region_code": region_code,
                    "year": r["year"],
                    "quarter": r["quarter"],
                    "total_sales": r["total_sales"],
                    "weekday_sales": r["weekday_sales"],
                    "weekend_sales": r["weekend_sales"]
                })
    return out

expanded = parsed.flatMap(apply_keywords)

# =====================================================================
# 6) Save CSV
# =====================================================================

def to_csv(r):
    def s(v): return "" if v is None else str(v)
    return ",".join([
        r["region_code"],
        s(r["year"]),
        s(r["quarter"]),
        s(r["total_sales"]),
        s(r["weekday_sales"]),
        s(r["weekend_sales"])
    ])

OUT_BASE = "hdfs:///user/cloudera/processed/sales"

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
p = sc._jvm.org.apache.hadoop.fs.Path(OUT_BASE)

if fs.exists(p):
    fs.delete(p, True)

expanded.map(to_csv).saveAsTextFile(OUT_BASE)

print("=== Sales Processed: FINAL SAFE VERSION Completed ===")
