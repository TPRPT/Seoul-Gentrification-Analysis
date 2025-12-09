# -*- coding: utf-8 -*-
import csv
from pathlib import Path


def normalize_header(h):
    if h is None:
        return ""
    h = h.replace("\ufeff", "")
    h = h.replace('"', '').replace("''", '')
    return h.strip()

def clean_str(x):
    if x is None:
        return ""
    return str(x).strip()

def safe_float(x):
    if not x:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return None


COL_MAP = {
    "접수년도": "RCPT_YR",
    "자치구코드": "CGG_CD",
    "자치구명": "CGG_NM",
    "법정동코드": "STDG_CD",
    "법정동명": "STDG_NM",
    "지번구분코드": "LOTNO_SE",
    "지번구분": "LOTNO_SE_NM",
    "본번": "MNO",
    "부번": "SNO",
    "층": "FLR",
    "계약일": "CTRT_DAY",
    "전월세구분": "RENT_SE",
    "임대면적": "RENT_AREA",
    "보증금(만원)": "GRFE",
    "임대료(만원)": "RTFE",
    "건물명": "BLDG_NM",
    "건축년도": "ARCH_YR",
    "건물용도": "BLDG_USG",
    "계약기간": "CTRT_PRD",
    "신규계약구분": "NEW_UPDT_YN",
    "갱신청구권사용": "CTRT_UPDT_USE_YN",
    "종전보증금": "BFR_GRFE",
    "종전임대료": "BFR_RTFE"
}

FLOAT_COLS = ["RENT_AREA", "GRFE", "RTFE", "BFR_GRFE", "BFR_RTFE"]


def clean_rent_folder():
    raw_dir = Path("raw_data/rent")
    out_dir = Path("cleaned_data/rent")
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2021, 2026):
        input_file = raw_dir / f"rent_{year}.csv"
        if not input_file.exists():
            print(f"SKIP: {input_file}")
            continue

        output_file = out_dir / f"rent_{year}_cleaned.csv"
        print(f"→ Processing {input_file}")

        with open(input_file, "r", encoding="utf-8-sig") as fin:
            raw_header = fin.readline().strip()
            header_list = [normalize_header(h) for h in raw_header.split(",")]

            reader = csv.DictReader(fin, fieldnames=header_list)
            eng_headers = [COL_MAP.get(h, h) for h in header_list]

            with open(output_file, "w", encoding="utf-8", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=eng_headers)
                writer.writeheader()

                for row in reader:
                    new_row = {}
                    for k, v in row.items():
                        eng = COL_MAP.get(k, k)
                        val = clean_str(v)

                        if eng in FLOAT_COLS:
                            new_row[eng] = safe_float(val)
                        else:
                            new_row[eng] = val

                    writer.writerow(new_row)

        print(f"[OK] Saved → {output_file}")


if __name__ == "__main__":
    clean_rent_folder()
