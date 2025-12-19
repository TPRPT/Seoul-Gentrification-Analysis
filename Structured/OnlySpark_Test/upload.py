# -*- coding: utf-8 -*-
import os

# CSV 파일 이름과 HDFS 폴더 매핑
datasets = {
    "real_estate": "real_estate_seongsu.csv",
    "rent": "rent_seongsu.csv",
    "sales": "sales_seongsu.csv",
    "population": "population_seongsu.csv"
}

local_path = "~/Downloads"

for name, csv in datasets.items():
    print("\nUploading " + name + " dataset...")
    
    # 디렉터리 생성
    os.system("hdfs dfs -mkdir -p /user/cloudera/raw/" + name + "/")
    
    # 파일 업로드
    upload_cmd = "hdfs dfs -put -f " + local_path + "/" + csv + " /user/cloudera/raw/" + name + "/"
    result = os.system(upload_cmd)
    
    if result == 0:
        print("Upload success → /user/cloudera/raw/" + name + "/")
    else:
        print("Upload failed: " + csv)
    
# 업로드 확인
print("\nChecking uploaded files...")
os.system("hdfs dfs -ls /user/cloudera/raw/*/")
