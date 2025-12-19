# **Seoul-Gentrification-Analysis**
서울 열린데이터포털의 정형 데이터와 블로그 기반 비정형 텍스트 데이터를 결합하여  
서울 7개 지역의 **Gentrification Potential Index (GPI)** 를 산출하는  
Hive + HDFS + Spark 기반 도시 데이터 분석 파이프라인입니다.

---

# 🔷 Project Overview
본 프로젝트는 두 가지 데이터 레이어를 통합합니다.

| Layer | Description | Tech Stack |
|-------|-------------|------------|
| 🧱 **Structured Layer** | 부동산·전월세·상권매출·생활인구 데이터 정제 및 분석(SGI) | HDFS, Hive, Spark(1.x/3.x) |
| 🌐 **Unstructured Layer** | 블로그 텍스트 기반 지역 이미지·감성 분석(UGI) | Python, Selenium, PySpark, NLP |

최종적으로 **SGI(정형) + UGI(비정형)** 를 합산한 **GPI** 를 생성합니다.

---

# 🔷 Structured Pipeline (정형 데이터)

```
Raw Layer → Processed Layer (Spark ETL) → Analysis Layer (SGI)
```

### **Raw Layer**
- `data.sh` : OpenAPI → CSV 일일 수집  
- `raw_upload_data.sh` : 과거 CSV(Historical) 수동 업로드  

### **Processed Layer**
- Spark ETL (`*_processed.py`)  
- 날짜 파싱, 지역코드 표준화, 핵심 변수 추출  
- `run_processed.sh` 로 ETL 일괄 실행  
- `hive_processed.sh` 로 Hive 테이블 생성  

### **Analysis Layer**
- `analysis.py` : 5개 지표 계산  
  - price / rent / sales / youth-inflow / senior-outflow  
- Z-score + Min–Max → **SGI(0–100)** 산출  

---

# 🔷 Unstructured Pipeline (비정형 텍스트)

비정형 파트는 Silver·Gold 코드 기반으로 다음만 수행합니다:

```
Raw Text → Silver (정제) → Gold (감성·키워드·토픽) → UGI 분석
```

- Silver : 텍스트 클리닝 & 기본 필터링  
- Gold : 감성 분석 · 키워드 추출 · 임베딩 기반 특징 생성  
- 월 단위 집계 후 **UGI(0–100)** 산출  

---

# 🔷 Execution (How to Run)

### **Daily Pipeline**
```bash
bash RawLayer/data.sh
bash ProcessedLayer/run_processed.sh
bash ProcessedLayer/hive_processed.sh
spark-submit AnalysisLayer/analysis.py
```

### **Historical Load**
```bash
bash RawLayer/raw_upload_data.sh
bash ProcessedLayer/run_processed.sh
```

---

# 🔷 Automation (Cron Example)
매일 00:10 실행:
```
10 0 * * * bash /home/training/DataPipeline/pipeline.sh
```

---

# 🔷 Output Structure
```
/processed/<dataset>/
/final_output_zscore/<region_code>/SGI.csv   # Structured
/gold/<dong>/UGI.csv                        # Unstructured
GPI.csv                                      # Final Index
```

---

# 🔷 Environment
| Component | Version |
|----------|---------|
| Hadoop | 2.6.0-cdh5.4.3 |
| Hive | 1.1.0 |
| Spark | 1.3.0 (ETL), 3.x (Analysis) |
| Python | 3.x |

---
