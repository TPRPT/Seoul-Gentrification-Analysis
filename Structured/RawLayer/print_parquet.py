# ==========================================
# print_parquet.py (Spark 1.3 compatible, FIXED PATH ISSUE)
# ==========================================
from pyspark import SparkContext
from pyspark.sql import SQLContext
import subprocess
import sys

# ------------------------------------------
# SparkContext & SQLContext 생성
# ------------------------------------------
sc = SparkContext(appName="PrintParquet")
sqlContext = SQLContext(sc)

# ------------------------------------------
# 입력 경로 정규화
# ------------------------------------------
def normalize(path):
    """Normalize any HDFS path into usable hdfs dfs CLI path."""
    if path.startswith("hdfs:///"):
        return path.replace("hdfs:///", "/")
    if path.startswith("hdfs://localhost:8020"):
        return path.replace("hdfs://localhost:8020", "")
    return path

# ==========================================
# HDFS DIRECTORY LIST
# ==========================================
def hdfs_ls(path):
    try:
        out = subprocess.check_output(["hdfs", "dfs", "-ls", path]).decode("utf-8").strip().split("\n")
    except:
        return []

    results = []
    for line in out:
        parts = line.split()
        if len(parts) < 8:
            continue
        results.append(parts[-1])
    return results

# ==========================================
# SAFE PARQUET LOADER
# ==========================================
def load_parquet_safely(path):
    try:
        df = sqlContext.parquetFile(path)
        return df
    except Exception as e:
        print("⚠️  FAILED:", path)
        print("   ERROR:", e)
        return None

# ==========================================
# UNION ALL
# ==========================================
def union_all(dfs):
    if not dfs:
        return None
    base = dfs[0]
    for df in dfs[1:]:
        base = base.unionAll(df)
    return base

# ==========================================
# MAIN
# ==========================================
if len(sys.argv) != 2:
    print("Usage: spark-submit print_parquet.py <hdfs_path>")
    sys.exit(1)

RAW_INPUT = sys.argv[1]
HDFS_PATH = normalize(RAW_INPUT)

print("\n=== INPUT PATH:", RAW_INPUT, "===")
print("=== NORMALIZED PATH:", HDFS_PATH, "===")

dfs = []
subdirs = hdfs_ls(HDFS_PATH)

if subdirs:
    print("Found subdirectories:")
    for d in subdirs:
        print(" →", d)
        df = load_parquet_safely(d)
        if df:
            dfs.append(df)
else:
    df = load_parquet_safely(HDFS_PATH)
    if df:
        dfs.append(df)

if not dfs:
    print("❌ No readable parquet data found.")
    sys.exit(1)

final_df = union_all(dfs)

# ==========================================
# PRINT RESULTS
# ==========================================
print("\n=== SCHEMA ===")
final_df.printSchema()

print("\n=== COUNT ===")
print(final_df.count())

print("\n=== SAMPLE ROWS ===")
final_df.show(20)

print("\n=== DONE ===\n")
