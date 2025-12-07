# Seoul-Gentrification-Analysis
[ITM/Big Data Practice] 정형(공공데이터) + 비정형(블로그 텍스트) 융합형 젠트리피케이션(Gentrification) 지수 분석

서울 열린데이터포털(Open Data Plaza)의 정형 데이터와  
블로그 비정형 데이터 소스를 함께 수집·분석하여  
서울 내 **젠트리피케이션(Gentrification) 지수(GP Index)** 를 산출하는  
**Hive + Spark 기반 도시 데이터 융합 분석 파이프라인**입니다.

---

## 프로젝트 개요

본 프로젝트는 서울의 도시 변화를 수치적으로 추적하기 위해  
다음 두 가지 데이터 계층을 통합합니다.

| 계층 | 설명 | 주요 기술 |
|------|------|------------|
| 🧱 **Structured Layer** | 서울 열린데이터포털의 공공데이터 (부동산 실거래, 전월세, 상권매출 등) | `Hive`, `HDFS`, `Spark SQL`, `Beeline` |
| 🌐 **Unstructured Layer** | SNS/블로그/뉴스 텍스트 기반 지역 이미지·감성 데이터 | `Python`, `Tweepy`, `BeautifulSoup`, `Selenium`, `KoNLPy`, `Spark NLP` |

이 두 계층의 분석 결과를 융합하여  
**지역 상권 변화율, 임대료 상승률, 감성 점수** 등을 종합한  
**Gentrification Potential (GP) Index**를 산출합니다.

---

## 데이터 구성

### 🧱 Structured Data (정형 데이터)
| 테이블명 | 주요 컬럼 | 설명 |
|-----------|-----------|------|
| `real_estate_raw` | `RCPT_YR`, `STDG_CD`, `THING_AMT` | 부동산 실거래가 |
| `rent_raw` | `RCPT_YR`, `STDG_CD`, `GRFE`, `RTFE` | 전월세 계약 정보 |
| `sales_raw` | `STDR_YYQU_CD`, `TRDAR_CD_NM`, `THSMON_SELNG_AMT` | 상권 매출 정보 |
| `population_raw` | `TMZON_PD_SE`, `TOT_LVPOP_CO`, `MALE_F*`, `FEMALE_F*` | 생활 인구 정보 |

### 🌐 Unstructured Data (비정형 데이터)
| 데이터 출처 | 수집 방식 | 주요 분석 내용 |
|--------------|-----------|----------------|
| **Twitter / X** | `tweepy` API | 지역명 기반 트윗 감성 점수 (긍·부정) |
| **Naver Blog** | `BeautifulSoup`, `Selenium` | 상권 키워드별 언급량, 긍정어 빈도 |
| **Naver News** | `News API` + `KoNLPy` | 지역 관련 뉴스 헤드라인 키워드 네트워크 |

---

## 실행 환경 (제공 받은 VM 환경에서 진행)

| 항목 | 버전 / 환경 |
|------|--------------|
| OS | CentOS 6 (Cloudera Quickstart VM) |
| Hadoop | 2.6.0-cdh5.4.3 |
| Hive | 1.1.0-cdh5.4.3 |
| Spark | 1.3.0 |
| Python | 3.x (for unstructured analysis) |

---
