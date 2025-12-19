# =====================================================
# print_parquet.py  (Spark 1.3 Compatible)
# 하위 파티션 자동 스캔 후 Schema + Sample 출력
# =====================================================

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="print_parquet")
sqlContext = SQLContext(sc)

# 로그 최소화
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# -----------------------------------------------------
# 입력 경로
# -----------------------------------------------------
if len(sys.argv) < 2:
    print("Usage: spark-submit print_parquet.py <hdfs_path>")
    sys.exit(1)

base_path = sys.argv[1]

# -----------------------------------------------------
# HDFS 디렉토리 리스트 함수
# Spark/HDFS 내부 ls → RDD로 처리
# -----------------------------------------------------
def hdfs_ls(path):
    try:
        jvm = sc._gateway.jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(hdfs_path):
            return None

        files = fs.listStatus(hdfs_path)
        return [str(f.getPath()) for f in files]
    except Exception as e:
        print("HDFS list error:", e)
        return None


print("\n=== SCAN:", base_path, "===\n")

dirs = hdfs_ls(base_path)
if dirs is None:
    print("❌ Path does not exist in HDFS.")
    sys.exit(1)

# -----------------------------------------------------
# 하위 디렉토리만 필터링
# -----------------------------------------------------
subdirs = [d for d in dirs if not d.endswith(".parquet") and not d.endswith("_SUCCESS")]

if len(subdirs) == 0:
    print("❌ No subdirectories found (no parquet partitions).")
    sys.exit(1)

print("📁 Found partitions:")
for d in subdirs:
    print("  -", d)

print("\n=========================================")
print(" START READING PARQUET PARTITIONS")
print("=========================================\n")

# -----------------------------------------------------
# 각 파티션 읽기 + 출력
# -----------------------------------------------------
for part in subdirs:
    print("\n-------------------------------")
    print("📌 Partition:", part)
    print("-------------------------------")

    try:
        df = sqlContext.parquetFile(part)
        print("\nSchema:")
        df.printSchema()

        print("\nSample Rows:")
        df.show(10)

        print("\nCount:", df.count())

    except Exception as e:
        print("❌ Failed to read partition:", part)
        print("   Error:", e)

print("\n=== ALL DONE ===")
