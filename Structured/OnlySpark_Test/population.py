# -*- coding: utf-8 -*-
import requests, pandas as pd, time, datetime

API_KEY = "YOUR_API_KEY"
BASE_URL = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/SPOP_LOCAL_RESD_DONG"

STEP = 1000
END_LIMIT = 20000
TARGET_YEARS = [2023, 2024, 2025]  # ✅ 수집할 연도 범위
SEONGSU_CODES = {"11110530", "11110540", "11110550", "11110560"}  # ✅ 성수동 행정동코드

all_rows = []
print("📡 서울생활인구 API (연 단위 → 통합 CSV) 요청 중...")

for year in TARGET_YEARS:
    print(f"\n📆 {year}년 데이터 수집 시작...")

    for month in range(1, 13):
        # 월 마지막 일 계산
        try:
            days_in_month = (datetime.date(year if month < 12 else year + 1,
                                           (month % 12) + 1, 1) - datetime.timedelta(days=1)).day
        except ValueError:
            days_in_month = 30

        # 매월 마지막 날짜 기준으로만 수집 (대표일자)
        date = f"{year}{str(month).zfill(2)}{str(days_in_month).zfill(2)}"

        for start in range(1, END_LIMIT, STEP):
            end = start + STEP - 1
            url = f"{BASE_URL}/{start}/{end}/{date}"
            res = requests.get(url)

            if res.status_code != 200:
                print(f"❌ {date} {start}~{end} 요청 실패: {res.status_code}")
                break

            try:
                data = res.json().get("SPOP_LOCAL_RESD_DONG", {}).get("row", [])
            except Exception as e:
                print(f"⚠️ JSON 파싱 실패 ({date}): {e}")
                break

            if not data:
                # 데이터 없는 달이면 다음 달로 넘어감
                break

            all_rows.extend(data)
            print(f"✅ {date} {start}~{end} 수집 완료 ({len(data)}건, 누적 {len(all_rows)})")
            time.sleep(0.2)

print("\n📦 전체 수집 완료, DataFrame 변환 중...")
df = pd.DataFrame(all_rows).drop_duplicates().reset_index(drop=True)
print(f"📊 전체 데이터 건수: {len(df)}")

# ✅ 행정동 코드 기준 성수동 필터링
if "ADSTRD_CODE_SE" in df.columns:
    df_seongsu = df[df["ADSTRD_CODE_SE"].astype(str).isin(SEONGSU_CODES)]
    print(f"🏙️ 성수동 데이터 건수: {len(df_seongsu)}")
else:
    print("⚠️ 'ADSTRD_CODE_SE' 컬럼이 존재하지 않습니다. 전체 데이터 저장으로 진행합니다.")
    df_seongsu = df

# ✅ CSV 하나로 저장
filename = "population_seongsu_all.csv"
df_seongsu.to_csv(filename, index=False, encoding="utf-8-sig")
print(f"💾 {filename} 저장 완료 ({len(df_seongsu)}건) ✅")
