# ==========================================
# ① 환경 설정
# ==========================================
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

sqlContext = SQLContext(sc)

# 로그 최소화
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# ==========================================
# ② CSV 로드
# ==========================================
file_path = "hdfs:///user/cloudera/raw/population/population_seongsu_all.csv"

rdd = sc.textFile(file_path)
header = rdd.first()
data = rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

# 안전한 숫자 변환 함수
def safe_float(x):
    try:
        return float(x.strip().replace(",", "")) if x and x.strip() != "" else None
    except:
        return None

# ==========================================
# ③ Raw Schema 정의
# ==========================================
schema = StructType([
    StructField("STDR_DE_ID", StringType(), True),       # 기준일
    StructField("TMZON_PD_SE", StringType(), True),      # 시간대구분
    StructField("ADSTRD_CODE_SE", StringType(), True),   # 행정동 코드
    StructField("TOT_LVPOP_CO", DoubleType(), True)      # 총생활인구수
])

rows = data.map(lambda f: (
    f[0] if len(f) > 0 else None, 
    f[1] if len(f) > 1 else None, 
    f[2] if len(f) > 2 else None, 
    safe_float(f[3]) if len(f) > 3 else None
))

df_population = sqlContext.createDataFrame(rows, schema=schema)

# ==========================================
# ④ YEAR, MONTH 추출
# ==========================================
def parse_year(v):
    return int(v[:4]) if v and len(v) >= 4 else None

def parse_month(v):
    return int(v[4:6]) if v and len(v) >= 6 else None

year_udf = udf(parse_year, IntegerType())
month_udf = udf(parse_month, IntegerType())

df_population = df_population.withColumn("YEAR", year_udf(df_population.STDR_DE_ID))
df_population = df_population.withColumn("MONTH", month_udf(df_population.STDR_DE_ID))

# ==========================================
# ⑤ Processed Schema 정의 (실존 컬럼만)
# ==========================================
proc_schema = StructType([
    StructField("ADSTRD_CODE_SE", StringType(), True),
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("TOT_LVPOP_CO", DoubleType(), True),
    StructField("DATA_SRC", StringType(), True)
])

# ==========================================
# ⑥ RDD 변환 → Processed DataFrame 생성
# ==========================================
df_population_rdd = df_population.rdd.map(lambda r: (
    r.ADSTRD_CODE_SE,   # 행정동 코드
    r.YEAR,             # 연도
    r.MONTH,            # 월
    r.TOT_LVPOP_CO,     # 총생활인구수
    "population"        # 데이터 출처
))

df_population_proc = sqlContext.createDataFrame(df_population_rdd, schema=proc_schema)

# ==========================================
# ⑦ 출력 확인
# ==========================================
print("=== Population PROCESSED Schema ===")
df_population_proc.printSchema()

print("=== Population Sample Data ===")
df_population_proc.show(5)

# 8. 저장
output_path = "hdfs:///user/cloudera/processed/population"
df_population_proc.saveAsParquetFile(output_path)
print("[저장 완료]", output_path)