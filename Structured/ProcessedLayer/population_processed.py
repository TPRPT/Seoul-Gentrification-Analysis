# -*- coding: utf-8 -*-
# =====================================================================
# population_processed.py  
# Spark 1.3 Safe + 8-digit admin code filter + Age Sum + CSV Save
# =====================================================================

from pyspark import SparkContext
import re

sc = SparkContext(appName="population_processed_final")
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
    """ STDR_DE_ID = YYYYMMDD → (year, month) """
    s = clean_str(x)
    if s is None:
        return None, None
    s = re.sub(r"\D", "", s)
    if len(s) < 6:
        return None, None
    return int(s[:4]), int(s[4:6])


# =====================================================================
# 2) TARGET 8-digit administrative codes (population 전용)
# =====================================================================

TARGET_CODE_MAP = { 
    "Gongneung": ["11350103"],
    "Seongsu": ["11200650", "11200660", "11200670", "11200690"],
    "Sinsa": ["11680510"],
    "Yeonnam": ["11440710"],
    "Euljiro": ["11140605"],
    "Ikseon": ["11110615"],
    "Hannam": ["11170685"]
}

# Flatten code list
ALL_CODES = sum(TARGET_CODE_MAP.values(), [])


# =====================================================================
# 3) Load clean CSV
# =====================================================================

RAW_PATH = "hdfs:///user/cloudera/raw/population/*.csv"
raw = sc.textFile(RAW_PATH)

header = raw.first()
columns = header.split(",")
data = raw.filter(lambda x: x != header)


# =====================================================================
# 4) Age group detection
# =====================================================================

def is_youth(col):
    """ 20~39세 """
    return (
        ("MALE" in col or "FEMALE" in col)
        and any(age in col for age in 
            ["20","21","22","23","24","25","26","27","28","29",
             "30","31","32","33","34","35","36","37","38","39"])
    )

def is_senior(col):
    """ 60세 이상 """
    return (
        ("MALE" in col or "FEMALE" in col)
        and any(age in col for age in 
            ["60","61","62","63","64","65","66","67","68","69","70"])
    )


# =====================================================================
# 5) Parse row
# =====================================================================

def parse_row(line):
    parts = line.split(",")
    if len(parts) != len(columns):
        return None
    
    row = dict(zip(columns, parts))

    # ---- 8자리 행정동 필터 ----
    region8 = clean_str(row.get("ADSTRD_CODE_SE"))
    if region8 not in ALL_CODES:
        return None

    # ---- 날짜 ----
    year, month = parse_date(row.get("STDR_DE_ID"))
    total_pop = clean_float(row.get("TOT_LVPOP_CO"))

    youth = 0.0
    senior = 0.0

    for col, val in row.items():
        if is_youth(col):
            v = clean_float(val)
            if v is not None:
                youth += v
        if is_senior(col):
            v = clean_float(val)
            if v is not None:
                senior += v

    return {
        "region_code": region8,  # ★ 8자리 그대로 유지 (법정동 아님)
        "year": year,
        "month": month,
        "total_pop": total_pop,
        "pop_age_20_39": youth,
        "pop_age_60plus": senior
    }


parsed = data.map(parse_row).filter(lambda x: x is not None)


# =====================================================================
# 6) Convert row → CSV
# =====================================================================

def to_csv(r):
    def s(v): return "" if v is None else str(v)
    return ",".join([
        r["region_code"],
        s(r["year"]),
        s(r["month"]),
        s(r["total_pop"]),
        s(r["pop_age_20_39"]),
        s(r["pop_age_60plus"])
    ])


# =====================================================================
# 7) Save to HDFS
# =====================================================================

OUT_PATH = "hdfs:///user/cloudera/processed/population"

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
p = sc._jvm.org.apache.hadoop.fs.Path(OUT_PATH)

if fs.exists(p):
    fs.delete(p, True)

parsed.map(to_csv).saveAsTextFile(OUT_PATH)

print("=== Population Processed: FINAL (8-digit admin code) Completed ===")
