# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re

sqlContext = SQLContext(sc)
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# ① CSV 로드
rdd = sc.textFile("hdfs:///user/cloudera/raw/sales/sales_seongsu.csv")
header = rdd.first()
data = rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

# ② 안전한 float 변환 함수
def safe_float(x):
    try:
        return float(x.strip().replace(",", "")) if x and x.strip() != "" else None
    except:
        return None

# ③ 스키마 정의
schema = StructType([
    StructField("STDR_YYQU_CD", StringType(), True),
    StructField("TRDAR_SE_CD", StringType(), True),
    StructField("TRDAR_SE_CD_NM", StringType(), True),
    StructField("TRDAR_CD", StringType(), True),
    StructField("TRDAR_CD_NM", StringType(), True),
    StructField("SVC_INDUTY_CD", StringType(), True),
    StructField("SVC_INDUTY_CD_NM", StringType(), True),
    StructField("THSMON_SELNG_AMT", DoubleType(), True),
    StructField("THSMON_SELNG_CO", DoubleType(), True),
    StructField("MDWK_SELNG_AMT", DoubleType(), True),
    StructField("WKEND_SELNG_AMT", DoubleType(), True),
    StructField("TMZON_00_06_SELNG_AMT", DoubleType(), True),
    StructField("TMZON_21_24_SELNG_AMT", DoubleType(), True),
    StructField("AGRDE_20_SELNG_AMT", DoubleType(), True),
    StructField("AGRDE_30_SELNG_AMT", DoubleType(), True)
])

# ④ RDD → DataFrame 변환
rows = data.map(lambda f: (
    f[0], f[1], f[2], f[3], f[4], f[5], f[6],
    safe_float(f[7]), safe_float(f[8]), safe_float(f[9]), safe_float(f[10]),
    safe_float(f[23]), safe_float(f[24]), safe_float(f[27]), safe_float(f[28])
))
df_sales = sqlContext.createDataFrame(rows, schema=schema)

# ⑤ YEAR, QUARTER 분리
def parse_year(v):
    return int(v[:4]) if v and len(v) >= 4 else None

def parse_quarter(v):
    return int(v[4:]) if v and len(v) > 4 else None

year_udf = udf(parse_year, IntegerType())
quarter_udf = udf(parse_quarter, IntegerType())

df_sales = df_sales.withColumn("YEAR", year_udf(df_sales.STDR_YYQU_CD))
df_sales = df_sales.withColumn("QUARTER", quarter_udf(df_sales.STDR_YYQU_CD))

# ⑥ Processed Schema 생성
df_sales_proc = df_sales.map(
    lambda r: (
        r.TRDAR_CD,
        r.TRDAR_CD_NM,
        r.SVC_INDUTY_CD,
        r.SVC_INDUTY_CD_NM,
        r.YEAR,
        r.QUARTER,
        r.THSMON_SELNG_AMT,
        r.THSMON_SELNG_CO,
        r.WKEND_SELNG_AMT,
        r.TMZON_21_24_SELNG_AMT,
        r.TMZON_00_06_SELNG_AMT,
        r.AGRDE_20_SELNG_AMT,
        r.AGRDE_30_SELNG_AMT,
        "sales"
    )
).toDF([
    "TRDAR_CD", "TRDAR_CD_NM", "SVC_INDUTY_CD", "SVC_INDUTY_CD_NM",
    "YEAR", "QUARTER",
    "THSMON_SELNG_AMT", "THSMON_SELNG_CO",
    "WKEND_SELNG_AMT",
    "TMZON_21_24_SELNG_AMT", "TMZON_00_06_SELNG_AMT",
    "AGRDE_20_SELNG_AMT", "AGRDE_30_SELNG_AMT",
    "DATA_SRC"
])

# ⑦ 출력
print("=== Sales PROCESSED Schema ===")
df_sales_proc.printSchema()

print("=== Sales Sample Data ===")
df_sales_proc.show(5)

# 8. 저장
output_path = "hdfs:///user/cloudera/processed/sales"
df_sales_proc.saveAsParquetFile(output_path)
print("[저장 완료]", output_path)