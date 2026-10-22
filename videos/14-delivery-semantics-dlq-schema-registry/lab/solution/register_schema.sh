#!/usr/bin/env bash
# Đăng ký (hoặc chỉ kiểm tra tương thích, CHECK=1) một file .avsc lên Schema Registry.
#   bash register_schema.sh <file.avsc> [subject=pos.transactions-value] [registry=http://localhost:8081]
#   CHECK=1 bash register_schema.sh <file.avsc>      # → {"is_compatible": true|false}
set -euo pipefail
FILE=${1:?file.avsc}
SUBJECT=${2:-pos.transactions-value}
REGISTRY=${3:-http://localhost:8081}

# Registry nhận {"schema": "<schema dưới dạng CHUỖI json>"} — nên phải json.dumps hai lần.
BODY=$(python3 -c 'import json,sys; print(json.dumps({"schema": json.dumps(json.load(open(sys.argv[1])))}))' "$FILE")

if [[ "${CHECK:-0}" == "1" ]]; then
  URL="$REGISTRY/compatibility/subjects/$SUBJECT/versions/latest"
else
  URL="$REGISTRY/subjects/$SUBJECT/versions"
fi
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$BODY" "$URL"
echo
