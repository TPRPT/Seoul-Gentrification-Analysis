# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re

sqlContext = SQLContext(sc)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# 1. Load CSV
rdd = sc.textFile("hdfs:///user/cloudera/raw/real_estate/real_estate_seongsu.csv")
header = rdd.first()
data = rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

# 2. Safe float
def safe_float(x):
    try:
        return float(x.strip().replace(",", "")) if x and x.strip() != "" else None
    except:
        return None

# 3. Schema
schema = StructType([
    StructField("RCPT_YR", StringType(), True),
    StructField("CGG_CD", StringType(), True),
    StructField("CGG_NM", StringType(), True),
    StructField("STDG_CD", StringType(), True),
    StructField("STDG_NM", StringType(), True),
    StructField("LOTNO_SE", StringType(), True),
    StructField("LOTNO_SE_NM", StringType(), True),
    StructField("MNO", StringType(), True),
    StructField("SNO", StringType(), True),
    StructField("BLDG_NM", StringType(), True),
    StructField("CTRT_DAY", StringType(), True),
    StructField("THING_AMT", DoubleType(), True),
    StructField("ARCH_AREA", DoubleType(), True),
    StructField("LAND_AREA", DoubleType(), True),
    StructField("FLR", StringType(), True),
    StructField("RGHT_SE", StringType(), True),
    StructField("RTRCN_DAY", StringType(), True),
    StructField("ARCH_YR", StringType(), True),
    StructField("BLDG_USG", StringType(), True),
    StructField("DCLR_SE", StringType(), True),
    StructField("OPBIZ_RESTAGNT_SGG_NM", StringType(), True)
])

# 4. RDD → DataFrame
rows = data.map(lambda f: (
    f[0] if len(f) > 0 else None, f[1] if len(f) > 1 else None,
    f[2] if len(f) > 2 else None, f[3] if len(f) > 3 else None,
    f[4] if len(f) > 4 else None, f[5] if len(f) > 5 else None,
    f[6] if len(f) > 6 else None, f[7] if len(f) > 7 else None,
    f[8] if len(f) > 8 else None, f[9] if len(f) > 9 else None,
    f[10] if len(f) > 10 else None, safe_float(f[11]) if len(f) > 11 else None,
    safe_float(f[12]) if len(f) > 12 else None, safe_float(f[13]) if len(f) > 13 else None,
    f[14] if len(f) > 14 else None, f[15] if len(f) > 15 else None,
    f[16] if len(f) > 16 else None, f[17] if len(f) > 17 else None,
    f[18] if len(f) > 18 else None, f[19] if len(f) > 19 else None,
    f[20] if len(f) > 20 else None
))
df_real_estate = sqlContext.createDataFrame(rows, schema=schema)

# 5. YEAR / MONTH 추출
def extract_year(date_str):
    if not date_str:
        return None
    digits = re.sub(r'\D', '', date_str)
    return int(digits[:4]) if len(digits) >= 4 else None

def extract_month(date_str):
    if not date_str:
        return None
    digits = re.sub(r'\D', '', date_str)
    return int(digits[4:6]) if len(digits) >= 6 else None

year_udf = udf(extract_year, IntegerType())
month_udf = udf(extract_month, IntegerType())

df_proc_temp = df_real_estate.withColumn("YEAR", year_udf(df_real_estate.CTRT_DAY))
df_proc_temp = df_proc_temp.withColumn("MONTH", month_udf(df_real_estate.CTRT_DAY))

# 6. Processed Schema
df_real_estate_proc = df_proc_temp.map(
    lambda r: (
        r.CGG_CD,
        r.STDG_CD,
        r.YEAR,
        r.MONTH,
        r.THING_AMT,
        r.ARCH_AREA,
        r.LAND_AREA,
        r.FLR,
        r.RGHT_SE,
        "real_estate"
    )
).toDF([
    "CGG_CD", "STDG_CD", "YEAR", "MONTH",
    "THING_AMT", "ARCH_AREA", "LAND_AREA",
    "FLR", "RGHT_SE", "DATA_SRC"
])


# 7. 출력
print("=== Real Estate PROCESSED Schema ===")
df_real_estate_proc.printSchema()

print("=== Real Estate Sample Data ===")
df_real_estate_proc.show(5)


# 8. 저장
output_path = "hdfs:///user/cloudera/processed/real_estate"
df_real_estate_proc.saveAsParquetFile(output_path)
print("[저장 완료]", output_path)