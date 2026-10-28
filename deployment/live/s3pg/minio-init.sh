#!/bin/sh
# Copyright 2026 The Tessera authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Idempotently prepares MinIO for a Tessera s3pg log:
#  - creates the log bucket,
#  - allows anonymous (public) GetObject on it, so clients read the log straight from MinIO,
#  - creates a dedicated access key for the Tessera server, limited to that bucket.
set -eu
# Note: the MinIO image is minimal, so only shell builtins and mc are used here.

: "${MINIO_ENDPOINT:=http://minio:9000}"
: "${TESSERA_BUCKET:?}"
: "${TESSERA_S3_ACCESS_KEY:?}"
: "${TESSERA_S3_SECRET:?}"

mc alias set tessera "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb --ignore-existing "tessera/${TESSERA_BUCKET}"

# Public read of objects only (no anonymous listing or writes).
printf '%s\n' \
  '{"Version": "2012-10-17", "Statement": [{' \
  '  "Effect": "Allow", "Principal": {"AWS": ["*"]},' \
  '  "Action": ["s3:GetObject"],' \
  "  \"Resource\": [\"arn:aws:s3:::${TESSERA_BUCKET}/*\"]" \
  '}]}' > /tmp/public-read.json
mc anonymous set-json /tmp/public-read.json "tessera/${TESSERA_BUCKET}"

# Everything the Tessera server needs: read/write/delete objects (the garbage collector
# lists and deletes obsolete partial tiles and entry bundles), and nothing else.
printf '%s\n' \
  '{"Version": "2012-10-17", "Statement": [' \
  "  {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\"], \"Resource\": [\"arn:aws:s3:::${TESSERA_BUCKET}\"]}," \
  "  {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"], \"Resource\": [\"arn:aws:s3:::${TESSERA_BUCKET}/*\"]}" \
  ']}' > /tmp/tessera-rw.json
mc admin policy create tessera "tessera-${TESSERA_BUCKET}-rw" /tmp/tessera-rw.json
mc admin user add tessera "${TESSERA_S3_ACCESS_KEY}" "${TESSERA_S3_SECRET}"
mc admin policy attach tessera "tessera-${TESSERA_BUCKET}-rw" --user "${TESSERA_S3_ACCESS_KEY}"
echo "MinIO bucket ${TESSERA_BUCKET} ready"
