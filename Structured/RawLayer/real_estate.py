# -*- coding: utf-8 -*-
import csv
from pathlib import Path
import re

# ==== Utilities ===== #

def normalize_header(h):
    if h is None:
        return ""
    h = h.replace("\ufeff", "")  # remove BOM
    h = h.replace('"', '').replace("''", '')
    return h.strip()

def clean_str(x):
    if x is None:
        return ""
    if isinstance(x, list):
        x = ",".join(map(str, x))
    return str(x).strip()

def safe_float(x):
    if not x:
        return None
    x = clean_str(x).replace(",", "")
    try:
        return float(x)
    except:
        return None

# ========== Column Mapping ========== #

COL_MAP = {
    "접수연도": "RCPT_YR",
    "자치구코드": "CGG_CD",
    "자치구명": "CGG_NM",
    "법정동코드": "STDG_CD",
    "법정동명": "STDG_NM",
    "지번구분": "LOTNO_SE",
    "지번구분명": "LOTNO_SE_NM",
    "본번": "MNO",
    "부번": "SNO",
    "건물명": "BLDG_NM",
    "계약일": "CTRT_DAY",
    "물건금액(만원)": "THING_AMT",
    "건물면적(㎡)": "ARCH_AREA",
    "토지면적(㎡)": "LAND_AREA",
    "층": "FLR",
    "권리구분": "RGHT_SE",
    "취소일": "RTRCN_DAY",
    "건축년도": "ARCH_YR",
    "건물용도": "BLDG_USG",
    "신고구분": "DCLR_SE",
    "신고한 개업공인중개사 시군구명": "OPBIZ_RESTAGNT_SGG_NM"
}

FLOAT_COLS = ["THING_AMT", "ARCH_AREA", "LAND_AREA"]


# ========== Main Cleaner ========== #

def clean_real_estate_folder():
    raw_dir = Path("raw_data/real_estate")
    out_dir = Path("cleaned_data/real_estate")
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2021, 2026):
        input_file = raw_dir / f"real_estate_{year}.csv"
        if not input_file.exists():
            print(f"SKIP: {input_file}")
            continue

        output_file = out_dir / f"real_estate_{year}_cleaned.csv"
        print(f"→ Processing {input_file}")

        # BOM-safe reading
        with open(input_file, "r", encoding="utf-8-sig") as fin:

            # 1) Read + Clean Header
            raw_header = fin.readline().strip()
            header_list = [normalize_header(h) for h in raw_header.split(",")]

            # 2) DictReader using normalized headers
            reader = csv.DictReader(fin, fieldnames=header_list)

            # 3) Convert headers to English
            eng_headers = [COL_MAP.get(h, h) for h in header_list]

            with open(output_file, "w", encoding="utf-8", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=eng_headers)
                writer.writeheader()

                for row in reader:
                    new_row = {}
                    for k, v in row.items():
                        eng = COL_MAP.get(k, k)
                        value = clean_str(v)

                        if eng in FLOAT_COLS:
                            new_row[eng] = safe_float(value)
                        else:
                            new_row[eng] = value

                    writer.writerow(new_row)

        print(f"[OK] Saved → {output_file}")


if __name__ == "__main__":
    clean_real_estate_folder()
