# -*- coding: utf-8 -*-
import csv
from pathlib import Path
import re

# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def normalize_header(h):
    if not h:
        return ""
    h = h.replace("\ufeff", "")
    h = h.replace('""', '"')
    h = h.replace('"', '')
    return h.strip()

def clean_str(x):
    if x is None:
        return ""
    return str(x).replace("\ufeff", "").strip()

def safe_float(x):
    if x is None:
        return None
    x = clean_str(x).replace(",", "")
    if x == "" or x in ["-", "N/A", "NA"]:
        return None
    if re.match(r"^-?\d+(\.\d+)?$", x):
        try:
            return float(x)
        except:
            return None
    return None

# --------------------------------------------------
# FULL COLUMN MAP (한국어 → 서울시 공식 영문 컬럼명)
# --------------------------------------------------

COL_MAP = {
    "기준_년분기_코드": "STDR_YYQU_CD",
    "상권_구분_코드": "TRDAR_SE_CD",
    "상권_구분_코드_명": "TRDAR_SE_CD_NM",
    "상권_코드": "TRDAR_CD",
    "상권_코드_명": "TRDAR_CD_NM",
    "서비스_업종_코드": "SVC_INDUTY_CD",
    "서비스_업종_코드_명": "SVC_INDUTY_CD_NM",

    "당월_매출_금액": "THSMON_SELNG_AMT",
    "당월_매출_건수": "THSMON_SELNG_CO",

    "주중_매출_금액": "MDWK_SELNG_AMT",
    "주중_매출_건수": "MDWK_SELNG_CO",

    "주말_매출_금액": "WKEND_SELNG_AMT",
    "주말_매출_건수": "WKEND_SELNG_CO",

    "월요일_매출_금액": "MON_SELNG_AMT",
    "화요일_매출_금액": "TUES_SELNG_AMT",
    "수요일_매출_금액": "WED_SELNG_AMT",
    "목요일_매출_금액": "THUR_SELNG_AMT",
    "금요일_매출_금액": "FRI_SELNG_AMT",
    "토요일_매출_금액": "SAT_SELNG_AMT",
    "일요일_매출_금액": "SUN_SELNG_AMT",

    "월요일_매출_건수": "MON_SELNG_CO",
    "화요일_매출_건수": "TUES_SELNG_CO",
    "수요일_매출_건수": "WED_SELNG_CO",
    "목요일_매출_건수": "THUR_SELNG_CO",
    "금요일_매출_건수": "FRI_SELNG_CO",
    "토요일_매출_건수": "SAT_SELNG_CO",
    "일요일_매출_건수": "SUN_SELNG_CO",

    # 시간대별 매출 금액
    "시간대_00~06_매출_금액": "TMZON_00_06_SELNG_AMT",
    "시간대_06~11_매출_금액": "TMZON_06_11_SELNG_AMT",
    "시간대_11~14_매출_금액": "TMZON_11_14_SELNG_AMT",
    "시간대_14~17_매출_금액": "TMZON_14_17_SELNG_AMT",
    "시간대_17~21_매출_금액": "TMZON_17_21_SELNG_AMT",
    "시간대_21~24_매출_금액": "TMZON_21_24_SELNG_AMT",

    # 시간대별 매출 건수
    "시간대_건수~06_매출_건수": "TMZON_00_06_SELNG_CO",
    "시간대_건수~11_매출_건수": "TMZON_06_11_SELNG_CO",
    "시간대_건수~14_매출_건수": "TMZON_11_14_SELNG_CO",
    "시간대_건수~17_매출_건수": "TMZON_14_17_SELNG_CO",
    "시간대_건수~21_매출_건수": "TMZON_17_21_SELNG_CO",
    "시간대_건수~24_매출_건수": "TMZON_21_24_SELNG_CO",

    # 성별 매출
    "남성_매출_금액": "ML_SELNG_AMT",
    "여성_매출_금액": "FML_SELNG_AMT",
    "남성_매출_건수": "ML_SELNG_CO",
    "여성_매출_건수": "FML_SELNG_CO",

    # 연령대 매출 금액
    "연령대_10_매출_금액": "AGRDE_10_SELNG_AMT",
    "연령대_20_매출_금액": "AGRDE_20_SELNG_AMT",
    "연령대_30_매출_금액": "AGRDE_30_SELNG_AMT",
    "연령대_40_매출_금액": "AGRDE_40_SELNG_AMT",
    "연령대_50_매출_금액": "AGRDE_50_SELNG_AMT",
    "연령대_60_이상_매출_금액": "AGRDE_60_ABOVE_SELNG_AMT",

    # 연령대 매출 건수
    "연령대_10_매출_건수": "AGRDE_10_SELNG_CO",
    "연령대_20_매출_건수": "AGRDE_20_SELNG_CO",
    "연령대_30_매출_건수": "AGRDE_30_SELNG_CO",
    "연령대_40_매출_건수": "AGRDE_40_SELNG_CO",
    "연령대_50_매출_건수": "AGRDE_50_SELNG_CO",
    "연령대_60_이상_매출_건수": "AGRDE_60_ABOVE_SELNG_CO",
}

# --------------------------------------------------
# Identify numeric columns
# --------------------------------------------------

def is_numeric_column(col_name):
    return (
        ("금액" in col_name)
        or ("건수" in col_name)
        or ("매출" in col_name)
    )

# --------------------------------------------------
# Main
# --------------------------------------------------

def clean_sales_folder():
    raw_dir = Path("raw_data/sales")
    out_dir = Path("cleaned_data/sales")
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2021, 2026):
        input_file = raw_dir / f"sales_{year}.csv"
        if not input_file.exists():
            print(f"SKIP: {input_file}")
            continue

        output_file = out_dir / f"sales_{year}_cleaned.csv"
        print(f"→ Processing {input_file}")

        with open(input_file, "r", encoding="utf-8-sig") as fin:
            raw_header = fin.readline().strip()
            header_list = [normalize_header(h) for h in raw_header.split(",")]

            # convert all headers to English equivalent
            eng_headers = [COL_MAP.get(h, h) for h in header_list]

            reader = csv.DictReader(fin, fieldnames=header_list)

            with open(output_file, "w", encoding="utf-8", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=eng_headers)
                writer.writeheader()

                for row in reader:
                    new_row = {}
                    for orig_col, val in row.items():
                        eng_col = COL_MAP.get(orig_col, orig_col)
                        cleaned = clean_str(val)

                        if is_numeric_column(orig_col):
                            new_row[eng_col] = safe_float(cleaned)
                        else:
                            new_row[eng_col] = cleaned

                    writer.writerow(new_row)

        print(f"[OK] Saved → {output_file}")

# --------------------------------------------------
# Run
# --------------------------------------------------

if __name__ == "__main__":
    clean_sales_folder()
