#!/bin/bash
# ===================================================================
# download_processed.sh — Export Hive processed tables as CSV (with header)
# ===================================================================

VM="training@10.50.35.9"
SSH_OPTS="-o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa"

datasets=("real_estate" "rent" "sales" "population")

mkdir -p csv_export

# ===================================================================
# 함수: Hive CLI로 CSV 만들기 (헤더 포함)
# ===================================================================
export_csv () {
    local tbl=$1
    local hive_tbl="${tbl}_processed"
    local vm_dir="csv_export/${tbl}"
    local vm_csv="${vm_dir}/${tbl}.csv"

    echo ""
    echo "=== EXPORT CSV WITH HEADER (Hive CLI): ${tbl} ==="

    ssh $SSH_OPTS $VM "
        rm -rf ${vm_dir};
        mkdir -p ${vm_dir};

        # 1) 헤더 생성
        hive -e 'DESCRIBE ${hive_tbl};' | awk '{print \$1}' | paste -sd ',' - > ${vm_csv}

        # 2) 데이터 추가
        hive -e 'SELECT * FROM ${hive_tbl};' \
            | sed 's/\\t/,/g' >> ${vm_csv}
    "

    # LOCAL로 다운로드
    mkdir -p csv_export/${tbl}
    scp $SSH_OPTS ${VM}:${vm_csv} csv_export/${tbl}/

    echo "👉 Saved locally: csv_export/${tbl}/${tbl}.csv"
}

# ===================================================================
# 실행 LOOP
# ===================================================================
for d in "${datasets[@]}"; do
    export_csv $d
done

echo ""
echo "ALL CSV EXPORTED SUCCESSFULLY (WITH HEADER)!"
