# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext, Row

sqlContext = SQLContext(sc)
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def safe_float(x):
    try:
        return float(x.strip().replace(",", "")) if x and x.strip() != "" else None
    except Exception:
        return None

# ------------------------------------------------------------------
# 🏠 1. REAL ESTATE (부동산 실거래가)
# ------------------------------------------------------------------
rdd1 = sc.textFile("hdfs:///user/cloudera/raw/real_estate/real_estate_seongsu.csv")
header1 = rdd1.first()
data1 = rdd1.filter(lambda x: x != header1).map(lambda x: x.split(","))

df_real_estate = data1.map(lambda f: Row(
    SGG_CD = f[0] if len(f) > 0 else None,
    BJDONG_CD = f[1] if len(f) > 1 else None,
    BLDG_NM = f[2] if len(f) > 2 else None,
    OBJ_AMT = safe_float(f[3]) if len(f) > 3 else None,
    DEAL_YMD = f[4] if len(f) > 4 else None
)).toDF()

print("\n=== 🏠 Real Estate RAW Schema ===")
df_real_estate.printSchema()
df_real_estate.show(5)


# ------------------------------------------------------------------
# 🏢 2. RENT (전월세)
# ------------------------------------------------------------------
rdd2 = sc.textFile("hdfs:///user/cloudera/raw/rent/rent_seongsu.csv")
header2 = rdd2.first()
data2 = rdd2.filter(lambda x: x != header2).map(lambda x: x.split(","))

df_rent = data2.map(lambda f: Row(
    SGG_CD = f[0] if len(f) > 0 else None,
    BJDONG_CD = f[1] if len(f) > 1 else None,
    BLDG_NM = f[2] if len(f) > 2 else None,
    DEPOSIT = safe_float(f[3]) if len(f) > 3 else None,
    DEAL_YMD = f[4] if len(f) > 4 else None
)).toDF()

print("\n=== 🏢 Rent RAW Schema ===")
df_rent.printSchema()
df_rent.show(5)


# ------------------------------------------------------------------
# 🛒 3. SALES (상권 매출)
# ------------------------------------------------------------------
rdd3 = sc.textFile("hdfs:///user/cloudera/raw/sales/sales_seongsu.csv")
header3 = rdd3.first()
data3 = rdd3.filter(lambda x: x != header3).map(lambda x: x.split(","))

df_sales = data3.map(lambda f: Row(
    TRDAR_CD = f[0] if len(f) > 0 else None,
    TRDAR_NM = f[1] if len(f) > 1 else None,
    SVC_INDUTY_CD = f[2] if len(f) > 2 else None,
    TOT_SALES = safe_float(f[3]) if len(f) > 3 else None,
    YEAR = f[4] if len(f) > 4 else None
)).toDF()

print("\n=== 🛒 Sales RAW Schema ===")
df_sales.printSchema()
df_sales.show(5)


# ------------------------------------------------------------------
# 👥 4. POPULATION (생활인구)
# ------------------------------------------------------------------
rdd4 = sc.textFile("hdfs:///user/cloudera/raw/population/population_seongsu_all.csv")
header4 = rdd4.first()
data4 = rdd4.filter(lambda x: x != header4).map(lambda x: x.split(","))

df_pop = data4.map(lambda f: Row(
    BASE_YM = f[0] if len(f) > 0 else None,
    SGG_CD = f[1] if len(f) > 1 else None,
    BJDONG_CD = f[2] if len(f) > 2 else None,
    TOT_POP = safe_float(f[3]) if len(f) > 3 else None,
    MALE_POP = safe_float(f[4]) if len(f) > 4 else None
)).toDF()

print("\n=== 👥 Population RAW Schema ===")
df_pop.printSchema()
df_pop.show(5)

