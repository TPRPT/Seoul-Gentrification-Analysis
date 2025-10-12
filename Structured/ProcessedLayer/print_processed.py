# ==========================================
# 1. 환경 설정
# ==========================================
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# 로그 최소화
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# ==========================================
# 2. Parquet 로드
# ==========================================
output_path = "hdfs:///user/cloudera/processed/real_estate"

# Spark 1.3 이하에서는 parquetFile()을 사용
df_check = sqlContext.parquetFile(output_path)

# ==========================================
# 3. 결과 확인
# ==========================================
print("=== Real Estate PROCESSED Schema ===")
df_check.printSchema()

print("=== Real Estate Sample Data ===")
df_check.show(10)
