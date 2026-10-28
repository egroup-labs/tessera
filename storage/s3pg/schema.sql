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

-- PostgreSQL schema for the Tessera s3pg storage driver (schema compatibility version 1).
--
-- Tables are created in the first schema of the current search_path (the driver sets it to
-- Config.Schema while applying this file, if configured); use one PostgreSQL
-- schema per log. This file is idempotent and is applied automatically at start-up unless
-- Config.SkipSchemaInit is set, in which case it should be applied by your migration tooling.
--
-- Requires PostgreSQL 13 or newer.

-- Tessera holds the schema compatibility version of the data stored in this schema.
CREATE TABLE IF NOT EXISTS Tessera (
	id                   INTEGER NOT NULL PRIMARY KEY,
	compatibilityVersion BIGINT  NOT NULL
);

-- SeqCoord holds the next available sequence number (i.e. log index).
CREATE TABLE IF NOT EXISTS SeqCoord (
	id   INTEGER NOT NULL PRIMARY KEY,
	next BIGINT  NOT NULL CHECK (next >= 0)
) WITH (fillfactor = 50);

-- Seq is a queue of batches of sequenced, but not yet integrated, entries.
-- Rows are inserted by sequencing and deleted by integration, so it is vacuumed aggressively.
-- It must never be UNLOGGED: indices for these entries have already been returned to clients.
CREATE TABLE IF NOT EXISTS Seq (
	id  INTEGER NOT NULL,
	seq BIGINT  NOT NULL CHECK (seq >= 0),
	v   BYTEA,
	PRIMARY KEY (id, seq)
) WITH (
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 1000,
	autovacuum_vacuum_insert_scale_factor = 0,
	autovacuum_vacuum_insert_threshold = 1000
);

-- IntCoord holds the size and root hash of the integrated tree.
CREATE TABLE IF NOT EXISTS IntCoord (
	id       INTEGER NOT NULL PRIMARY KEY,
	seq      BIGINT  NOT NULL CHECK (seq >= 0),
	rootHash BYTEA   NOT NULL
) WITH (fillfactor = 50);

-- PubCoord coordinates checkpoint publication.
CREATE TABLE IF NOT EXISTS PubCoord (
	id          INTEGER NOT NULL PRIMARY KEY,
	publishedAt BIGINT  NOT NULL CHECK (publishedAt >= 0),
	size        BIGINT  CHECK (size >= 0)
) WITH (fillfactor = 50);

-- GCCoord tracks garbage collection progress.
CREATE TABLE IF NOT EXISTS GCCoord (
	id       INTEGER NOT NULL PRIMARY KEY,
	fromSize BIGINT  NOT NULL CHECK (fromSize >= 0)
) WITH (fillfactor = 50);

-- Initial rows. These only take effect on a fresh schema, so an existing log is never reset.
INSERT INTO Tessera (id, compatibilityVersion) VALUES (0, 1) ON CONFLICT (id) DO NOTHING;
INSERT INTO SeqCoord (id, next) VALUES (0, 0) ON CONFLICT (id) DO NOTHING;
-- The root hash of an empty RFC 6962 tree is SHA-256("").
INSERT INTO IntCoord (id, seq, rootHash)
	VALUES (0, 0, decode('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'hex'))
	ON CONFLICT (id) DO NOTHING;
INSERT INTO PubCoord (id, publishedAt, size) VALUES (0, 0, 0) ON CONFLICT (id) DO NOTHING;
INSERT INTO GCCoord (id, fromSize) VALUES (0, 0) ON CONFLICT (id) DO NOTHING;
