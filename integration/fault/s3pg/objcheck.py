#!/usr/bin/env python3
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
"""objcheck: object-level checks of a tlog-tiles bucket (read directly from S3, never via a proxy).

  objcheck.py BUCKET check [--since EPOCH]
      For every partial tile/bundle X.p/W whose full version X exists (or with a wider partial),
      verify that the wider object starts with the narrower one (tiles: W*32 bytes; bundles: the
      first W length-prefixed entries). A mismatch means two different trees were written to the
      bucket (divergent timelines) and a client that prefers the full object would read data
      inconsistent with a checkpoint. With --since, also lists objects modified after EPOCH.
  objcheck.py BUCKET prune SIZE [--quarantine QBUCKET] [--dry-run]
      Operator recovery helper (README "Recovering from a lost commit", step 1): delete every
      tile/bundle (full or partial) that describes indices beyond tree size SIZE (prints what it
      deletes). With --quarantine, each object is first copied to QBUCKET (created if missing).
  objcheck.py BUCKET create-bucket | delete-bucket
      Test helpers: create the bucket with an anonymous-read policy / empty and delete it.
  objcheck.py BUCKET marker [--delete]
      Reports (JSON) whether the persistent tripped marker `s3pg-coordination-tripped` exists, with
      its content; --delete removes it (last step of the recovery runbook) and verifies it is gone.
"""
import json, re, sys
import boto3, botocore

b, op = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test",
                  region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))


def list_all(bucket):
    res = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        res += page.get("Contents", [])
    return res


if op == "create-bucket":
    s3.create_bucket(Bucket=b)
    s3.put_bucket_policy(Bucket=b, Policy=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::%s/*" % b]}]}))
    sys.exit(0)
if op == "delete-bucket":
    try:
        objs = list_all(b)
    except s3.exceptions.NoSuchBucket:
        sys.exit(0)
    for i in range(0, len(objs), 1000):
        s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": o["Key"]} for o in objs[i:i + 1000]], "Quiet": True})
    s3.delete_bucket(Bucket=b)
    sys.exit(0)
if op == "marker":
    # The persistent tripped marker (storage/s3pg/guard.go, trippedMarkerPath; no BucketPrefix).
    mk = "s3pg-coordination-tripped"
    try:
        r = s3.get_object(Bucket=b, Key=mk)
        body = r["Body"].read().decode(errors="replace")
        res = {"exists": True, "last_modified": r["LastModified"].timestamp(), "content": body.strip()[:300]}
    except s3.exceptions.NoSuchKey:
        res = {"exists": False}
    if "--delete" in sys.argv and res["exists"]:
        s3.delete_object(Bucket=b, Key=mk)
        try:
            s3.head_object(Bucket=b, Key=mk)
            res["deleted"] = False
        except botocore.exceptions.ClientError:
            res["deleted"] = True
    print(json.dumps(res))
    sys.exit(0)

objs = list_all(b)
keys = {o["Key"]: o for o in objs}
pat = re.compile(r"^tile/(entries|\d+)/((?:x\d{3}/)*\d{3})(?:\.p/(\d+))?$")


def parse(k):
    m = pat.match(k)
    if not m:
        return None
    lvl = -1 if m.group(1) == "entries" else int(m.group(1))
    idx = int(m.group(2).replace("x", "").replace("/", ""))
    w = int(m.group(3)) if m.group(3) else 256
    return lvl, idx, w


def get(k):
    try:
        return s3.get_object(Bucket=b, Key=k)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None  # deleted (e.g. garbage collected) since the listing


def bundle_prefix(data, n):
    off = 0
    for _ in range(n):
        ln = int.from_bytes(data[off:off + 2], "big")
        off += 2 + ln
    return data[:off]


if op == "check":
    since = float(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[3] == "--since" else None
    groups = {}
    for k in keys:
        p = parse(k)
        if p:
            groups.setdefault((p[0], p[1]), []).append((p[2], k))
    mismatches = []
    compared = 0
    for (lvl, idx), vs in groups.items():
        vs.sort()
        cache = {}
        for i in range(len(vs) - 1):
            w, k = vs[i]
            for w2, k2 in vs[i + 1:]:
                a = cache[k] if k in cache else cache.setdefault(k, get(k))
                c = cache[k2] if k2 in cache else cache.setdefault(k2, get(k2))
                if a is None or c is None:
                    continue
                pa = a if lvl >= 0 else a
                pc = c[:w * 32] if lvl >= 0 else bundle_prefix(c, w)
                compared += 1
                if pa != pc:
                    mismatches.append("%s is not a prefix of %s" % (k, k2))
    res = {"objects": len(objs), "compared_pairs": compared, "prefix_mismatches": len(mismatches), "examples": mismatches[:10]}
    if since is not None:
        newer = sorted(k for k, o in keys.items() if o["LastModified"].timestamp() > since)
        res["modified_since"] = len(newer)
        res["modified_since_examples"] = newer[:15]
    print(json.dumps(res))
    sys.exit(1 if mismatches else 0)
elif op == "prune":
    size = int(sys.argv[3])
    qb = sys.argv[sys.argv.index("--quarantine") + 1] if "--quarantine" in sys.argv else None
    dry = "--dry-run" in sys.argv
    dels = []
    for k in keys:
        p = parse(k)
        if not p:
            continue
        lvl, idx, w = p
        # Leaves covered by this object: entries -> [idx*256, idx*256+w); level L tile -> up to (idx*256+w)*256^L.
        end = (idx * 256 + w) * (256 ** lvl if lvl >= 0 else 1)
        if end > size:
            dels.append(k)
    if dry:
        print(json.dumps({"would_prune": len(dels), "keys": sorted(dels)}))
        sys.exit(0)
    if qb and dels:
        try:
            s3.head_bucket(Bucket=qb)
        except botocore.exceptions.ClientError:
            s3.create_bucket(Bucket=qb)
        for k in dels:
            s3.copy_object(Bucket=qb, Key=k, CopySource={"Bucket": b, "Key": k})
        missing = set(dels) - {o["Key"] for o in list_all(qb)}
        if missing:
            print(json.dumps({"error": "quarantine copy incomplete", "missing": sorted(missing)[:20]}))
            sys.exit(1)
    for i in range(0, len(dels), 1000):
        s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in dels[i:i + 1000]], "Quiet": True})
    print(json.dumps({"pruned": len(dels), "quarantined_in": qb, "examples": sorted(dels)[:20]}))
