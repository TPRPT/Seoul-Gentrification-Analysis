# -*- coding: utf-8 -*-
import requests, pandas as pd, time

API_KEY = "YOUR_API_KEY"
BASE_URL = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/tbLnOpendataRtmsV"

STEP = 1000
END_LIMIT = 20000
years = ["2022", "2023", "2024"]  # ✅ 연도 확장

all_rows = []
print("📡 서울시 부동산 실거래가 API 요청 중...")

for y in years:
    for start in range(1, END_LIMIT, STEP):
        end = start + STEP - 1
        url = f"{BASE_URL}/{start}/{end}/{y}"
        res = requests.get(url)
        if res.status_code != 200:
            print(f"❌ {y} {start}~{end} 요청 실패: {res.status_code}")
            break
        data = res.json().get("tbLnOpendataRtmsV", {}).get("row", [])
        if not data:
            print(f"⚠️ {y} {start}~{end} 데이터 없음 (마지막 페이지)")
            break
        all_rows.extend(data)
        print(f"✅ {y} {start}~{end} 수집 완료 ({len(data)}건)")
        time.sleep(0.3)

df = pd.DataFrame(all_rows).drop_duplicates().reset_index(drop=True)
mask = df.apply(lambda r: any("성수" in str(x) for x in r.astype(str)), axis=1)
df_seongsu = df[mask]
df_seongsu.to_csv("real_estate_seongsu.csv", index=False, encoding="utf-8-sig")

print(f"📊 총 {len(df)}건 중 성수동 {len(df_seongsu)}건 저장 완료 ✅")
