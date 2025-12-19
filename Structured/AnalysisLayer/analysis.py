# pip install pyspark==4.0.1

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, lag, explode, sum as spark_sum,
    concat_ws, lpad, when, coalesce, udf,
    mean as spark_mean, stddev as spark_std,
    min as spark_min, max as spark_max
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
import os

# ============================================
# 0. Spark Session (로컬)
# ============================================
spark = (
    SparkSession.builder
    .appName("gentrification_analysis")
    .master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

print("Spark Ready!")

# ============================================
# 1. CSV 로드
# ============================================

BASE = "/your/local/path/csv_export"  # <<< 변경하세요

real = spark.read.csv(f"{BASE}/real_estate/real_estate.csv", header=True, inferSchema=True)
rent = spark.read.csv(f"{BASE}/rent/rent.csv", header=True, inferSchema=True)
sales = spark.read.csv(f"{BASE}/sales/sales.csv", header=True, inferSchema=True)
pop = spark.read.csv(f"{BASE}/population/population.csv", header=True, inferSchema=True)

print("Loaded CSVs WITH HEADER")

# ============================================
# 2. 법정동 / 행정동 코드 매핑
# ============================================

LEGAL_MAP = {
    "11300": "11300",
    "11400": "11400",
    "11500": "11400",
    "10900": "10900",
    "12400": "12400",
    "10400": "10400",
    "10500": "10400",
    "15500": "10400",
    "15000": "10400",
    "15100": "10400",
    "14800": "10400",
    "14900": "10400",
    "13300": "13300",
    "13100": "13100"
}

ADMIN_MAP = {
    "11350103": "11300",
    "11200650": "11400",
    "11200660": "11400",
    "11200670": "11400",
    "11200690": "11400",
    "11680510": "10900",
    "11440710": "12400",
    "11140605": "10400",
    "11110615": "13300",
    "11170685": "13100"
}

map_legal = udf(lambda x: LEGAL_MAP.get(str(x)), StringType())
map_admin = udf(lambda x: ADMIN_MAP.get(str(x)), StringType())

real = real.withColumn("region_code", map_legal("region_code"))
rent = rent.withColumn("region_code", map_legal("region_code"))
sales = sales.withColumn("region_code", map_legal("region_code"))
pop = pop.withColumn("region_code", map_admin("region_code"))

real = real.dropna(subset=["region_code"])
rent = rent.dropna(subset=["region_code"])
sales = sales.dropna(subset=["region_code"])
pop = pop.dropna(subset=["region_code"])

print("Normalized region codes")

# ============================================
# 3. Raw Indicators 생성
# ============================================

quarter_map = {
    1: ["01","02","03"],
    2: ["04","05","06"],
    3: ["07","08","09"],
    4: ["10","11","12"]
}

quarter_udf = udf(lambda q: quarter_map.get(int(q)) if q else None,
                  ArrayType(StringType()))

sales_m = (
    sales.withColumn("months", quarter_udf(col("quarter")))
         .withColumn("month", explode(col("months")))
         .withColumn("total_sales", col("total_sales") / lit(3))
         .select("region_code", "year", "month", "total_sales")
)

def add_time_id(df):
    return df.withColumn(
        "time_id",
        concat_ws("", col("year").cast("string"), lpad(col("month").cast("string"), 2, "0"))
    )

real_t = add_time_id(real)
rent_t = add_time_id(rent)
sales_t = add_time_id(sales_m)
pop_t = add_time_id(pop)

# 2021~2024 + pop에 있는 2025
months_full = [f"{y}{m:02d}" for y in range(2021, 2025) for m in range(1, 12+1)]
months_2025 = [row.time_id for row in pop_t.filter(col("year") == 2025).select("time_id").distinct().collect()]
months_full.extend(months_2025)

months_df = spark.createDataFrame([(m,) for m in months_full], ["time_id"])

REGION_CODE_ORDER = ["11300","11400","10900","12400","10400","13300","13100"]
region_df = spark.createDataFrame([(c,) for c in REGION_CODE_ORDER], ["region_code"])

full_grid = region_df.crossJoin(months_df)

real_g = real_t.groupBy("region_code","time_id").agg(avg("trade_price").alias("trade_price"))
rent_g = rent_t.groupBy("region_code","time_id").agg(
    avg("deposit").alias("deposit"), avg("monthly_rent").alias("monthly_rent")
)
sales_g = sales_t.groupBy("region_code","time_id").agg(spark_sum("total_sales").alias("total_sales"))
pop_g = pop_t.groupBy("region_code","time_id").agg(
    avg("total_pop").alias("total_pop"),
    avg("pop_age_20_39").alias("pop_age_20_39"),
    avg("pop_age_60plus").alias("pop_age_60plus")
)

merged = (
    full_grid
    .join(real_g, ["region_code","time_id"], "left")
    .join(rent_g, ["region_code","time_id"], "left")
    .join(sales_g, ["region_code","time_id"], "left")
    .join(pop_g, ["region_code","time_id"], "left")
)

# ========= MoM 변화율 =========

w = Window.partitionBy("region_code").orderBy("time_id")

merged = merged.withColumn("prev_trade", lag("trade_price").over(w))
merged = merged.withColumn("price_change_rate",
    when(col("prev_trade").isNull(), lit(0))
    .otherwise((col("trade_price") - col("prev_trade")) / col("prev_trade"))
)

merged = merged.withColumn("rent_all", col("deposit") + col("monthly_rent") * 100)
merged = merged.withColumn("prev_rent", lag("rent_all").over(w))
merged = merged.withColumn("rent_change_rate",
    when(col("prev_rent").isNull(), lit(0))
    .otherwise((col("rent_all") - col("prev_rent")) / col("prev_rent"))
)

merged = merged.withColumn("prev_sales", lag("total_sales").over(w))
merged = merged.withColumn("sales_growth_rate",
    when(col("prev_sales").isNull(), lit(0))
    .otherwise((col("total_sales") - col("prev_sales")) / col("prev_sales"))
)

merged = merged.withColumn("prev20", lag("pop_age_20_39").over(w))
merged = merged.withColumn("youth_inflow_rate",
    when(col("prev20").isNull(), lit(0))
    .otherwise((col("pop_age_20_39") - col("prev20")) / col("prev20"))
)

merged = merged.withColumn("prev60", lag("pop_age_60plus").over(w))
merged = merged.withColumn("senior_outflow_rate",
    when(col("prev60").isNull(), lit(0))
    .otherwise((col("prev60") - col("pop_age_60plus")) / col("prev60"))
)

merged.cache()

print("Indicators ready")

# ============================================
# 4. SGI 계산 (Z-score → 0~100)
# ============================================

merged_sgi = merged.withColumn(
    "SGI_raw",
    coalesce(col("price_change_rate"), lit(0)) +
    coalesce(col("rent_change_rate"), lit(0)) +
    coalesce(col("sales_growth_rate"), lit(0)) +
    coalesce(col("youth_inflow_rate"), lit(0)) +
    coalesce(col("senior_outflow_rate"), lit(0))
)

stats = merged_sgi.agg(
    spark_mean("SGI_raw").alias("mean"),
    spark_std("SGI_raw").alias("std")
).collect()[0]

mu = stats["mean"]
sigma = stats["std"] if stats["std"] not in (None, 0) else 1.0

merged_sgi = merged_sgi.withColumn("SGI_z", (col("SGI_raw") - lit(mu)) / lit(sigma))

z_stats = merged_sgi.agg(
    spark_min("SGI_z").alias("z_min"),
    spark_max("SGI_z").alias("z_max")
).collect()[0]

z_min = z_stats["z_min"]
z_max = z_stats["z_max"] if z_stats["z_max"] != z_min else z_min + 1.0

merged_sgi = merged_sgi.withColumn(
    "SGI_score",
    100 * (col("SGI_z") - lit(z_min)) / (lit(z_max) - lit(z_min))
)

# ============================================
# 5. 결과 CSV 저장
# ============================================

FINAL_COLS = [
    "region_code",
    "time_id",
    "price_change_rate",
    "rent_change_rate",
    "sales_growth_rate",
    "youth_inflow_rate",
    "senior_outflow_rate",
    "SGI_score"
]

OUTPUT_DIR = "./final_output_zscore"
os.makedirs(OUTPUT_DIR, exist_ok=True)

for code in REGION_CODE_ORDER:
    df_code = (
        merged_sgi.filter(col("region_code") == code)
                  .orderBy("time_id")
                  .select(FINAL_COLS)
    )

    df_code.coalesce(1).write.csv(f"{OUTPUT_DIR}/{code}", header=True, mode="overwrite")
    print(f"✔ Saved: {OUTPUT_DIR}/{code}")

print("\nSGI 계산 완료!")
