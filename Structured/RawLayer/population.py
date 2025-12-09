# -*- coding: utf-8 -*-
import csv
from pathlib import Path
import re

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def normalize_header(h):
    """Remove BOM, Excel double quotes, whitespace"""
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
    if x in ["", "-", "N/A", "NA"]:
        return None
    if re.match(r"^-?\d+(\.\d+)?$", x):
        try:
            return float(x)
        except:
            return None
    return None

# -------------------------------------------------------
# English key definitions (fixed columns)
# -------------------------------------------------------

COL_BASE = {
    "기준일ID": "STDR_DE_ID",
    "시간대구분": "TMZON_PD_SE",
    "행정동코드": "ADSTRD_CODE_SE",
    "총생활인구수": "TOT_LVPOP_CO",
}

# -------------------------------------------------------
# Dynamic MALE/FEMALE Age column conversion
# -------------------------------------------------------

def convert_gender_age(col):
    """
    Convert:
       남자20세부터24세생활인구수 → MALE_F20_24
       여자65세부터69세생활인구수 → FEMALE_F65_69
       남자70세이상생활인구수     → MALE_F70_ABOVE
    """
    if not col:           # ★ None 또는 빈 문자열 처리
        return None

    # 남자 / 여자
    if col.startswith("남자"):
        gender = "MALE"
        age_part = col[2:]
    elif col.startswith("여자"):
        gender = "FEMALE"
        age_part = col[2:]
    else:
        return None  # ★ 성별·연령 컬럼이 아닌 경우

    # 70세 이상
    m = re.match(r"(\d+)세이상생활인구수", age_part)
    if m:
        return f"{gender}_F{m.group(1)}_ABOVE"

    # 일반 구간
    m = re.match(r"(\d+)세부터(\d+)세생활인구수", age_part)
    if m:
        return f"{gender}_F{m.group(1)}_{m.group(2)}"

    return None


# -------------------------------------------------------
# Main cleaning function
# -------------------------------------------------------

def clean_population_folder():
    raw_dir = Path("raw_data/population")
    out_dir = Path("cleaned_data/population")
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2021, 2026):

        input_file = raw_dir / f"population_{year}.csv"
        if not input_file.exists():
            print(f"SKIP: {input_file}")
            continue

        output_file = out_dir / f"population_{year}_cleaned.csv"
        print(f"→ Processing {input_file}")

        with open(input_file, "r", encoding="utf-8-sig") as fin:
            raw_header = fin.readline().strip()

            # ① 헤더 normalize
            header_list = [normalize_header(h) for h in raw_header.split(",")]

            # ② 값이 "" 또는 None인 header 제거
            header_list = [h for h in header_list if h not in ("", None)]

            # ③ 한국어 → 영어 매핑
            eng_headers = []
            for h in header_list:
                if h in COL_BASE:
                    eng_headers.append(COL_BASE[h])
                else:
                    age_col = convert_gender_age(h)
                    eng_headers.append(age_col if age_col else h)

            reader = csv.DictReader(fin, fieldnames=header_list)

            with open(output_file, "w", encoding="utf-8", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=eng_headers)
                writer.writeheader()

                for row in reader:
                    new_row = {}

                    for orig, val in row.items():

                        if not orig:       # ★ None key 방지
                            continue

                        # determine english column name
                        if orig in COL_BASE:
                            key = COL_BASE[orig]
                        else:
                            key = convert_gender_age(orig) or orig

                        # float 변환
                        if key == "TOT_LVPOP_CO":
                            new_row[key] = safe_float(val)
                        elif key.startswith(("MALE_F", "FEMALE_F")):
                            new_row[key] = safe_float(val)
                        else:
                            new_row[key] = clean_str(val)

                    writer.writerow(new_row)

        print(f"[OK] Saved → {output_file}")


if __name__ == "__main__":
    clean_population_folder()
