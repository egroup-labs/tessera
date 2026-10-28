-- Copyright 2026 The Tessera authors. All Rights Reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- PostgreSQL schema for the Tessera s3pg antispam index (schema compatibility version 1).
--
-- Tables are created in the first schema of the current search_path. The antispam tables may
-- share a schema with the s3pg log tables, or live in a schema (or database) of their own.
-- This file is idempotent and is applied automatically at start-up unless
-- AntispamOpts.SkipSchemaInit is set, in which case it should be applied by your migration tooling.
--
-- Requires PostgreSQL 13 or newer.

-- AntispamMeta holds the schema compatibility version of the data stored in these tables.
CREATE TABLE IF NOT EXISTS AntispamMeta (
	id                   INTEGER NOT NULL PRIMARY KEY,
	compatibilityVersion BIGINT  NOT NULL
);

-- AntispamIDSeq maps entry identity hashes to the index of the first entry in the log with that identity.
CREATE TABLE IF NOT EXISTS AntispamIDSeq (
	h   BYTEA  NOT NULL PRIMARY KEY,
	idx BIGINT NOT NULL CHECK (idx >= 0)
);

-- AntispamFollowCoord holds the index of the next log entry to be added to AntispamIDSeq.
CREATE TABLE IF NOT EXISTS AntispamFollowCoord (
	id      INTEGER NOT NULL PRIMARY KEY,
	nextIdx BIGINT  NOT NULL CHECK (nextIdx >= 0)
) WITH (fillfactor = 50);

-- Initial rows. These only take effect on a fresh schema, so an existing antispam index is never reset.
-- The compatibilityVersion seeded here must equal SchemaCompatibilityVersion.
INSERT INTO AntispamMeta (id, compatibilityVersion) VALUES (0, 1) ON CONFLICT (id) DO NOTHING;
INSERT INTO AntispamFollowCoord (id, nextIdx) VALUES (0, 0) ON CONFLICT (id) DO NOTHING;
