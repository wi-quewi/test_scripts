#!/usr/bin/env bash
#
# Import ES index data from Huawei Cloud OBS into local Elasticsearch.
#
# Steps per index:
#   1. Download {index}_mapping.json.gz and {index}_data.ndjson.gz from OBS
#   2. Decompress .gz files
#   3. Create index with mapping in target ES
#   4. Convert NDJSON to Bulk API format and import in batches
#
# Prerequisites: curl, openssl, jq, gunzip
#
# Usage:
#   export OBS_AK="your_access_key"
#   export OBS_SK="your_secret_key"
#   bash import_from_obs.sh
#
set -euo pipefail

# --- Configuration ---
TARGET_ES="http://localhost:9200"
OBS_ENDPOINT="obs.cn-hongkong-6001.sgcis.hksarg"
OBS_BUCKET="wiener-pro-factbot-obs"
OBS_PREFIX="ES-Index"

INDICES=(
  "copilot_glossary_person_20260123"
  "copilot_glossary_person_acting_en"
  "copilot_glossary_person_acting_tc"
  "copilot_glossary_company_en"
  "copilot_glossary_company_tc"
  "wiki_en_passages_20260115"
  "wiki_zh_passages_20260115"
)

BULK_BATCH_SIZE=2000
WORK_DIR="/tmp/es_import_$$"
mkdir -p "$WORK_DIR"

# --- Validate credentials ---
if [ -z "${OBS_AK:-}" ] || [ -z "${OBS_SK:-}" ]; then
  echo "ERROR: Please set OBS_AK and OBS_SK environment variables."
  echo ""
  echo "  export OBS_AK=\"your_access_key\""
  echo "  export OBS_SK=\"your_secret_key\""
  echo "  bash $0"
  exit 1
fi

# --- OBS download function (V2 signature) ---
obs_download() {
  local object_key="$1"
  local output_file="$2"

  local date_header
  date_header="$(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')"

  local string_to_sign="GET\n\n\n${date_header}\n/${OBS_BUCKET}/${object_key}"
  local signature
  signature=$(printf '%b' "$string_to_sign" | openssl dgst -sha1 -hmac "$OBS_SK" -binary | base64)

  local http_code
  http_code=$(curl -s -w "%{http_code}" -o "$output_file" \
    -H "Date: ${date_header}" \
    -H "Authorization: AWS ${OBS_AK}:${signature}" \
    "https://${OBS_BUCKET}.${OBS_ENDPOINT}/${object_key}")

  if [ "$http_code" -eq 200 ]; then
    return 0
  else
    echo "  [FAIL] Download failed (HTTP $http_code): $object_key"
    cat "$output_file" 2>/dev/null | head -5
    return 1
  fi
}

# --- Create index from mapping ---
create_index() {
  local index="$1"
  local mapping_file="$2"

  # Check if index already exists
  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET_ES/$index")
  if [ "$status" -eq 200 ]; then
    echo "  Index '$index' already exists, skipping creation."
    return 0
  fi

  # Extract settings and mappings from the export
  local body
  body=$(jq -c ".[\"$index\"] | {settings: .settings.index | {number_of_shards, number_of_replicas, analysis}, mappings: .mappings}" "$mapping_file")

  local http_code
  http_code=$(curl -s -o /tmp/es_create_resp.json -w "%{http_code}" \
    -X PUT "$TARGET_ES/$index" \
    -H "Content-Type: application/json" \
    -d "$body")

  if [ "$http_code" -eq 200 ]; then
    echo "  [OK] Index '$index' created."
  else
    echo "  [FAIL] Failed to create index (HTTP $http_code):"
    cat /tmp/es_create_resp.json | jq . 2>/dev/null || cat /tmp/es_create_resp.json
    return 1
  fi
}

# --- Bulk import data ---
bulk_import() {
  local index="$1"
  local data_file="$2"

  local total_docs
  total_docs=$(wc -l < "$data_file" | tr -d ' ')
  echo "  Total documents: $total_docs"

  if [ "$total_docs" -eq 0 ]; then
    echo "  No documents to import."
    return 0
  fi

  # Preprocess entire file in one jq pass: convert NDJSON to ES bulk format
  local bulk_file="$WORK_DIR/bulk_all.ndjson"
  echo "  Converting to bulk format..."
  jq -c --arg idx "$index" '{index:{_index:$idx,_id:._id}} , ._source' "$data_file" > "$bulk_file"

  # Split into batch files (2 lines per doc in bulk format)
  local lines_per_chunk=$((BULK_BATCH_SIZE * 2))
  local chunk_prefix="$WORK_DIR/bulk_chunk_"
  split -l "$lines_per_chunk" "$bulk_file" "$chunk_prefix"
  rm -f "$bulk_file"

  local imported=0
  local failed=0
  local start_time
  start_time=$(date +%s)

  for chunk in "${chunk_prefix}"*; do
    local chunk_docs=$(( $(wc -l < "$chunk" | tr -d ' ') / 2 ))

    local resp
    resp=$(curl -s -X POST "$TARGET_ES/_bulk" \
      -H "Content-Type: application/x-ndjson" \
      --data-binary "@$chunk")
    rm -f "$chunk"

    local errors
    errors=$(echo "$resp" | jq -r '.errors')
    if [ "$errors" = "true" ]; then
      local err_count
      err_count=$(echo "$resp" | jq '[.items[] | select(.index.error)] | length')
      failed=$((failed + err_count))
      imported=$((imported + chunk_docs - err_count))
    else
      imported=$((imported + chunk_docs))
    fi

    local now elapsed rate
    now=$(date +%s)
    elapsed=$((now - start_time))
    rate=$(( (imported + failed) / (elapsed > 0 ? elapsed : 1) ))
    local remaining=$(( (total_docs - imported - failed) / (rate > 0 ? rate : 1) ))
    printf "\r  Progress: %s/%s (%d%%) | %d docs/s | ETA: %ds | errors: %d   " \
      "$((imported + failed))" "$total_docs" \
      "$(( (imported + failed) * 100 / (total_docs > 0 ? total_docs : 1) ))" \
      "$rate" "$remaining" "$failed"
  done

  echo ""
  echo "  Imported: $imported, Failed: $failed"
}

# --- Main ---
echo "Target ES:  $TARGET_ES"
echo "OBS Bucket: $OBS_BUCKET"
echo "OBS Prefix: $OBS_PREFIX/"
echo "Work Dir:   $WORK_DIR"
echo ""

for i in "${!INDICES[@]}"; do
  INDEX="${INDICES[$i]}"
  NUM=$((i + 1))
  echo "===== [$NUM/${#INDICES[@]}] $INDEX ====="

  MAPPING_GZ="$WORK_DIR/${INDEX}_mapping.json.gz"
  DATA_GZ="$WORK_DIR/${INDEX}_data.ndjson.gz"
  MAPPING_FILE="$WORK_DIR/${INDEX}_mapping.json"
  DATA_FILE="$WORK_DIR/${INDEX}_data.ndjson"

  # Step 1: Download .gz files from OBS
  echo "  Downloading mapping..."
  if ! obs_download "$OBS_PREFIX/${INDEX}_mapping.json.gz" "$MAPPING_GZ"; then
    echo "  SKIP: cannot download mapping."
    echo ""
    continue
  fi
  echo "  [OK] Mapping downloaded."

  echo "  Downloading data (this may take a while for large indices)..."
  if ! obs_download "$OBS_PREFIX/${INDEX}_data.ndjson.gz" "$DATA_GZ"; then
    echo "  SKIP: cannot download data."
    echo ""
    continue
  fi
  local_size=$(du -h "$DATA_GZ" | cut -f1)
  echo "  [OK] Data downloaded ($local_size compressed)."

  # Step 2: Decompress
  echo "  Decompressing..."
  gunzip -f "$MAPPING_GZ"
  gunzip -f "$DATA_GZ"
  local_size=$(du -h "$DATA_FILE" | cut -f1)
  echo "  [OK] Decompressed ($local_size)."

  # Step 3: Create index
  echo "  Creating index..."
  if ! create_index "$INDEX" "$MAPPING_FILE"; then
    echo "  SKIP: cannot create index."
    echo ""
    continue
  fi

  # Step 4: Bulk import
  echo "  Importing data..."
  bulk_import "$INDEX" "$DATA_FILE"

  # Clean up downloaded files to save disk space
  rm -f "$MAPPING_FILE" "$DATA_FILE"
  echo "  Cleaned up temp files."
  echo ""
done

rm -rf "$WORK_DIR"
echo "All done!"
