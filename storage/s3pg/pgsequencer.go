// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3pg

// This file contains the PostgreSQL port of the mySQLSequencer found in storage/aws/aws.go.
// Method bodies deliberately mirror the upstream implementation so that upstream changes can
// be ported mechanically; the differences are:
//   - pgx stdlib driver and $n placeholders; DDL and initial rows live in schema.sql, applied in
//     one transaction under pg_advisory_xact_lock (see initDB),
//   - start-up verifies the initial rows of all coordination tables, not just the version,
//   - the pool can be supplied externally (Config.DB) and is closed only if we opened it, and
//     MaxIdleConns == 0 keeps the database/sql default rather than disabling idle connections,
//   - assignEntries fails if the IntCoord row is missing (upstream silently drops the batch),
//   - consumed Seq rows are deleted with "= ANY($n)" and the affected row count is verified,
//   - publishCheckpoint and garbageCollect use a context-bound transaction rather than Begin(), and
//     every transaction runs at READ COMMITTED with a durable commit (see internal/pgtx),
//   - table names are schema-qualified when Config.Schema is set (see internal/pgtx.Qualifier),
//   - consumeEntries fails (rather than silently doing nothing) if the IntCoord row is missing,
//   - consumeEntries and publishCheckpoint pass their transaction to the function they call (see
//     withTx), so that the guards' reads use the transaction's connection,
//   - currentTree fails if it is answered by a standby (pg_is_in_recovery()),
//   - assignEntries refuses indices below minNext (the appender's high-water mark, see guard.go).

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Registers the "pgx" database/sql driver.
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/transparency-dev/tessera/internal/otel"
	"github.com/transparency-dev/tessera/storage/s3pg/internal/pgtx"
)

// SchemaSQL is the idempotent DDL (and initial rows) for the tables used by this driver.
//
// It is applied automatically at start-up unless Config.SkipSchemaInit is set, in which
// case it must be applied to the log's schema by external migration tooling beforehand.
//
//go:embed schema.sql
var SchemaSQL string

// schemaInitLockKey is hashed by PostgreSQL to the advisory lock key which serialises schema
// initialisation across concurrently starting instances. Concurrent "CREATE TABLE IF NOT EXISTS"
// statements can otherwise fail with unique violations on the system catalogues.
const schemaInitLockKey = "tessera/s3pg/init"

// coordTables are the names of the tables used by the sequencer, as written in SQL statements.
var coordTableNames = []string{"Tessera", "SeqCoord", "Seq", "IntCoord", "PubCoord", "GCCoord"}

// pgSequencer uses PostgreSQL to provide a durable and thread/multi-process safe sequencer.
type pgSequencer struct {
	dbPool         *sql.DB
	ownsDB         bool
	maxOutstanding uint64
	// q qualifies table names in SQL statements with Config.Schema, if set.
	q *pgtx.Qualifier
	// minNext, if set, returns the largest log size this instance has seen integrated or
	// published. assignEntries refuses to hand out indices below it (see guard.go).
	minNext func() uint64
}

// txKey is the context key under which consumeEntries and publishCheckpoint pass their
// transaction to the functions they call, so that reads made by those functions (see currentTree)
// use the transaction's connection: this can't starve the pool, and fails if the transaction's
// connection has been lost rather than reading through another one.
type txKey struct{}

func withTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// openDB returns the database handle described by cfg, and whether the caller owns (i.e. must close) it.
func openDB(ctx context.Context, cfg Config) (*sql.DB, bool, error) {
	if cfg.DB != nil {
		if err := cfg.DB.PingContext(ctx); err != nil {
			return nil, false, fmt.Errorf("failed to ping PostgreSQL db: %v", err)
		}
		return cfg.DB, false, nil
	}
	dbPool, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open PostgreSQL db: %v", err)
	}
	if cfg.MaxOpenConns > 0 {
		dbPool.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	// Unlike upstream (where 0 disables idle connections), 0 leaves the database/sql default in
	// place: establishing a new PostgreSQL connection for every query is very expensive.
	// A negative value disables idle connections.
	if cfg.MaxIdleConns != 0 {
		dbPool.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if err := dbPool.PingContext(ctx); err != nil {
		_ = dbPool.Close()
		return nil, false, fmt.Errorf("failed to ping PostgreSQL db: %v", err)
	}
	return dbPool, true, nil
}

// newPGSequencer returns a new pgSequencer which uses cfg.DB if set, or otherwise a new
// connection pool opened from cfg.DSN.
//
// Unless cfg.SkipSchemaInit is set, the schema is created if necessary. The compatibility
// version of the stored data is always checked.
func newPGSequencer(ctx context.Context, cfg Config, maxOutstanding uint64) (*pgSequencer, error) {
	dbPool, owns, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	r := &pgSequencer{
		dbPool:         dbPool,
		ownsDB:         owns,
		maxOutstanding: maxOutstanding,
		q:              pgtx.NewQualifier(cfg.Schema, coordTableNames...),
	}

	if !cfg.SkipSchemaInit {
		if err := r.initDB(ctx); err != nil {
			_ = r.close()
			return nil, fmt.Errorf("failed to initDB: %v", err)
		}
	}
	if err := r.checkDataCompatibility(ctx); err != nil {
		_ = r.close()
		return nil, fmt.Errorf("schema is not compatible with this version of the Tessera library: %v", err)
	}
	if err := r.checkSchema(ctx); err != nil {
		_ = r.close()
		return nil, fmt.Errorf("schema is incomplete (apply SchemaSQL): %v", err)
	}
	return r, nil
}

// checkSchema verifies that all coordination tables exist and hold their initial rows, so that a
// missing or partially applied schema (e.g. with SkipSchemaInit) fails at start-up rather than later.
func (s *pgSequencer) checkSchema(ctx context.Context) error {
	var n int
	row := s.dbPool.QueryRowContext(ctx, s.q.SQL(`SELECT
		(SELECT count(*) FROM SeqCoord WHERE id = 0) +
		(SELECT count(*) FROM IntCoord WHERE id = 0) +
		(SELECT count(*) FROM PubCoord WHERE id = 0) +
		(SELECT count(*) FROM GCCoord WHERE id = 0) +
		0 * (SELECT count(*) FROM (SELECT 1 FROM Seq LIMIT 1) AS s)`))
	if err := row.Scan(&n); err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("found %d of the 4 initial coordination rows", n)
	}
	return nil
}

// close releases the database pool if it was opened by this sequencer.
func (s *pgSequencer) close() error {
	if s.ownsDB {
		return s.dbPool.Close()
	}
	return nil
}

// checkDataCompatibility compares the Tessera library SchemaCompatibilityVersion with the one stored in the
// database, and returns an error if they are not identical.
func (s *pgSequencer) checkDataCompatibility(ctx context.Context) error {
	row := s.dbPool.QueryRowContext(ctx, s.q.SQL("SELECT compatibilityVersion FROM Tessera WHERE id = 0"))
	var gotVersion uint64
	if err := row.Scan(&gotVersion); err != nil {
		return fmt.Errorf("failed to read schema compatibility version from DB: %v", err)
	}

	if gotVersion != SchemaCompatibilityVersion {
		return fmt.Errorf("schema compatibilityVersion (%d) != library compatibilityVersion (%d)", gotVersion, SchemaCompatibilityVersion)
	}
	return nil
}

// initDB ensures that the coordination DB is initialised correctly.
//
// SchemaSQL is applied in a single transaction while holding a transaction-scoped advisory lock,
// so that any number of instances may initialise the same schema concurrently. SchemaSQL is
// idempotent and never modifies existing rows, so an existing log is never reset.
//
// The database schema consists of 6 tables:
//   - Tessera
//     This table holds the schema compatibility version of the stored data.
//   - SeqCoord
//     This table holds a single row which records the next available sequence number.
//   - Seq
//     This table holds batches of sequenced, but not yet integrated, entries, keyed by the
//     index of the first entry in the batch.
//   - IntCoord
//     This table holds the size and root hash of the integrated tree.
//   - PubCoord
//     This table coordinates the publication of checkpoints.
//   - GCCoord
//     This table records the progress of garbage collection of partial tiles and bundles.
func (s *pgSequencer) initDB(ctx context.Context) error {
	tx, err := pgtx.Begin(ctx, s.dbPool)
	if err != nil {
		return fmt.Errorf("failed to begin Tx: %v", err)
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", schemaInitLockKey); err != nil {
		return fmt.Errorf("failed to acquire schema initialisation lock: %v", err)
	}
	if err := s.q.SetSearchPath(ctx, tx); err != nil {
		return fmt.Errorf("failed to select schema: %v", err)
	}
	if _, err := tx.ExecContext(ctx, SchemaSQL); err != nil {
		return fmt.Errorf("failed to apply schema: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema: %v", err)
	}
	tx = nil
	return nil
}

// assignEntries will durably assign contiguous sequence numbers to the provided entries.
//
// It returns tessera.ErrPushbackIntegration if the number of sequenced but not yet integrated
// entries exceeds the configured maximum.
func (s *pgSequencer) assignEntries(ctx context.Context, entries []*tessera.Entry) error {
	return otel.TraceErr(ctx, "tessera.storage.s3pg.assignEntries", tracer, func(ctx context.Context, span trace.Span) error {
		start := time.Now()
		defer func() {
			opsHistogram.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(opNameKey.String("assignEntries")))
		}()

		span.SetAttributes(numEntriesKey.Int(len(entries)))

		// First grab the treeSize in a non-locking read-only fashion (we don't want to block/collide with integration).
		// We'll use this value to determine whether we need to apply back-pressure.
		var treeSize uint64
		row := s.dbPool.QueryRowContext(ctx, s.q.SQL("SELECT seq FROM IntCoord WHERE id = $1"), 0)
		if err := row.Scan(&treeSize); err != nil {
			// Note: upstream silently drops the batch if the row is missing; we never do that.
			return fmt.Errorf("failed to read integration coordination info: %v", err)
		}

		// Now move on with sequencing in a single transaction
		tx, err := pgtx.Begin(ctx, s.dbPool)
		if err != nil {
			return fmt.Errorf("failed to begin Tx: %v", err)
		}
		defer func() {
			if tx != nil {
				pgtx.Rollback(ctx, tx)
			}
		}()

		// First we need to grab the next available sequence number from the SeqCoord table.
		var next, id uint64
		r := tx.QueryRowContext(ctx, s.q.SQL("SELECT id, next FROM SeqCoord WHERE id = $1 FOR UPDATE"), 0)
		if err := r.Scan(&id, &next); err != nil {
			return fmt.Errorf("failed to read seqcoord: %v", err)
		}

		// Never hand out indices which this instance has already seen integrated or published:
		// that can only happen if the database has lost commits (see guard.go).
		if s.minNext != nil {
			if hw := s.minNext(); next < hw {
				return fmt.Errorf("%w: next index %d < %d (already integrated or published); refusing to sequence", ErrCoordinationBehind, next, hw)
			}
		}

		// Check whether there are too many outstanding entries and we should apply
		// back-pressure.
		if outstanding := next - treeSize; outstanding > s.maxOutstanding {
			return tessera.ErrPushbackIntegration
		}

		sequencedEntries := make([]storage.SequencedEntry, len(entries))
		// Assign provisional sequence numbers to entries.
		// We need to do this here in order to support serialisations which include the log position.
		for i, e := range entries {
			sequencedEntries[i] = storage.SequencedEntry{
				BundleData: e.MarshalBundleData(next + uint64(i)),
				LeafHash:   e.LeafHash(),
			}
		}

		// Flatten the entries into a single slice of bytes which we can store in the Seq.v column.
		b := &bytes.Buffer{}
		e := gob.NewEncoder(b)
		if err := e.Encode(sequencedEntries); err != nil {
			return fmt.Errorf("failed to serialise batch: %v", err)
		}
		data := b.Bytes()
		num := uint64(len(entries))

		// Insert our newly sequenced batch of entries into Seq,
		if _, err := tx.ExecContext(ctx, s.q.SQL("INSERT INTO Seq(id, seq, v) VALUES($1, $2, $3)"), 0, next, data); err != nil {
			return fmt.Errorf("insert into seq: %v", err)
		}
		// and update the next-available sequence number row in SeqCoord.
		if _, err := tx.ExecContext(ctx, s.q.SQL("UPDATE SeqCoord SET next = $1 WHERE id = $2"), next+num, 0); err != nil {
			return fmt.Errorf("update seqcoord: %v", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit Tx: %v", err)
		}
		tx = nil

		return nil

	}, trace.WithAttributes(otel.PeriodicKey.Bool(true)))
}

// consumeEntries calls f with previously sequenced entries.
//
// Once f returns without error, the entries it was called with are considered to have been consumed and are
// removed from the Seq table.
//
// Returns true if some entries were consumed as a weak signal that there may be further entries waiting to be consumed.
func (s *pgSequencer) consumeEntries(ctx context.Context, limit uint64, f consumeFunc, forceUpdate bool) (bool, error) {
	tx, err := pgtx.Begin(ctx, s.dbPool)
	if err != nil {
		return false, fmt.Errorf("failed to begin Tx: %v", err)
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()

	// Figure out which is the starting index of sequenced entries to start consuming from.
	//
	// The row lock on IntCoord serialises integration across all instances. Under READ COMMITTED,
	// a waiter which acquires the lock sees the latest committed version of the row, and every
	// subsequent statement in this transaction takes a fresh snapshot.
	row := tx.QueryRowContext(ctx, s.q.SQL("SELECT seq, rootHash FROM IntCoord WHERE id = $1 FOR UPDATE"), 0)
	var fromSeq uint64
	var rootHash []byte
	if err := row.Scan(&fromSeq, &rootHash); err != nil {
		// Note: upstream silently returns (false, nil) if the row is missing; we never do that.
		return false, fmt.Errorf("failed to read IntCoord: %v", err)
	}
	slog.DebugContext(ctx, "Consuming", slog.Uint64("fromseq", fromSeq))

	// Now read the sequenced starting at the index we got above.
	rows, err := tx.QueryContext(ctx, s.q.SQL("SELECT seq, v FROM Seq WHERE id = $1 AND seq >= $2 ORDER BY seq LIMIT $3 FOR UPDATE"), 0, fromSeq, limit)
	if err != nil {
		return false, fmt.Errorf("failed to read Seq: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.WarnContext(ctx, "rows.Close", slog.Any("error", err))
		}
	}()

	seqsConsumed := []int64{}
	entries := make([]storage.SequencedEntry, 0, limit)
	orderCheck := fromSeq
	for rows.Next() {

		var vGob []byte
		var seq uint64
		if err := rows.Scan(&seq, &vGob); err != nil {
			return false, fmt.Errorf("failed to scan Seq row: %v", err)
		}

		if orderCheck != seq {
			return false, fmt.Errorf("integrity fail - expected seq %d, but found %d", orderCheck, seq)
		}

		g := gob.NewDecoder(bytes.NewReader(vGob))
		b := []storage.SequencedEntry{}
		if err := g.Decode(&b); err != nil {
			return false, fmt.Errorf("failed to deserialise v from Seq: %v", err)
		}
		entries = append(entries, b...)
		seqsConsumed = append(seqsConsumed, int64(seq))
		orderCheck += uint64(len(b))
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to iterate over Seq rows: %v", err)
	}
	// The result set must be closed before the connection can be used for further statements.
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("failed to close Seq rows: %v", err)
	}
	if len(seqsConsumed) == 0 && !forceUpdate {
		slog.DebugContext(ctx, "Found no rows to sequence")
		return false, nil
	}

	// Call consumeFunc with the entries we've found
	newRoot, err := f(withTx(ctx, tx), uint64(fromSeq), entries)
	if err != nil {
		return false, err
	}

	// consumeFunc was successful, so we can update our coordination row, and delete the row(s) for
	// the then consumed entries.
	if _, err := tx.ExecContext(ctx, s.q.SQL("UPDATE IntCoord SET seq=$1, rootHash=$2 WHERE id=$3"), orderCheck, newRoot, 0); err != nil {
		return false, fmt.Errorf("update intcoord: %v", err)
	}

	if len(seqsConsumed) > 0 {
		res, err := tx.ExecContext(ctx, s.q.SQL("DELETE FROM Seq WHERE id=$1 AND seq = ANY($2)"), 0, seqsConsumed)
		if err != nil {
			return false, fmt.Errorf("delete from seq: %v", err)
		}
		// We hold row locks on exactly these rows, so anything else indicates corruption.
		if n, err := res.RowsAffected(); err != nil {
			return false, fmt.Errorf("delete from seq: failed to get affected rows: %v", err)
		} else if n != int64(len(seqsConsumed)) {
			return false, fmt.Errorf("integrity fail - deleted %d Seq rows, expected %d", n, len(seqsConsumed))
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit Tx: %v", err)
	}
	tx = nil

	return true, nil
}

// currentTree returns the size and root hash of the currently integrated tree.
//
// If ctx carries a transaction (see withTx), the read is made on it. The read fails if it is
// served by a standby (e.g. through a load-balancing pooler): a lagging standby must never be
// mistaken for a database which has lost commits (see guard.go).
func (s *pgSequencer) currentTree(ctx context.Context) (uint64, []byte, error) {
	const q = "SELECT seq, rootHash, pg_is_in_recovery() FROM IntCoord WHERE id = $1"
	var row *sql.Row
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok && tx != nil {
		row = tx.QueryRowContext(ctx, s.q.SQL(q), 0)
	} else {
		row = s.dbPool.QueryRowContext(ctx, s.q.SQL(q), 0)
	}
	var fromSeq uint64
	var rootHash []byte
	var standby bool
	if err := row.Scan(&fromSeq, &rootHash, &standby); err != nil {
		return 0, nil, fmt.Errorf("failed to read IntCoord: %v", err)
	}
	if standby {
		return 0, nil, errors.New("failed to read IntCoord: connected to a standby server; the coordination database connection must go to the primary")
	}

	return fromSeq, rootHash, nil
}

// nextIndex returns the next available index in the log.
func (s *pgSequencer) nextIndex(ctx context.Context) (uint64, error) {
	row := s.dbPool.QueryRowContext(ctx, s.q.SQL("SELECT next FROM SeqCoord WHERE id = $1"), 0)
	var nextSeq uint64
	if err := row.Scan(&nextSeq); err != nil {
		return 0, fmt.Errorf("failed to read DB: %v", err)
	}

	return nextSeq, nil
}

// publishCheckpoint checks when the last checkpoint was published, and if it was more than minAge ago, calls the provided
// function to publish a new one.
//
// This function uses PubCoord with an exclusive lock to guarantee that only one tessera instance can attempt to publish
// a checkpoint at any given time.
func (s *pgSequencer) publishCheckpoint(ctx context.Context, minStaleActive, minStaleRepub time.Duration, f func(context.Context, uint64, []byte) error) (nextPubAt time.Time, errR error) {
	start := time.Now()
	defer func() {
		// Detect any errors and update metrics accordingly.
		// Non-error cases are explicitly handled in the body of the function below.
		if errR != nil {
			publishCount.Add(ctx, 1, metric.WithAttributes(errorTypeKey.String("error")))
		}
	}()

	tx, err := pgtx.Begin(ctx, s.dbPool)
	if err != nil {
		return time.Time{}, err
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()

	pRow := tx.QueryRowContext(ctx, s.q.SQL("SELECT publishedAt, size FROM PubCoord WHERE id = $1 FOR UPDATE"), 0)
	var publishedAt int64
	var lastSize sql.NullInt64
	if err := pRow.Scan(&publishedAt, &lastSize); err != nil {
		return time.Time{}, fmt.Errorf("failed to parse PubCoord: %v", err)
	}
	var pubAt time.Time
	// Tessera used to store publishedAt in seconds, and now stores it in nanoseconds.
	// This handles the transition.
	const nanosecondThreshold = 1_310_324_790_000
	if publishedAt < nanosecondThreshold {
		pubAt = time.Unix(publishedAt, 0)
	} else {
		pubAt = time.Unix(0, publishedAt)
	}
	cpAge := time.Since(pubAt)
	if cpAge < minStaleActive {
		slog.DebugContext(ctx, "publishCheckpoint: last checkpoint published too recently, not publishing new checkpoint", slog.Duration("age", cpAge), slog.Duration("minstaleactive", minStaleActive))
		publishCount.Add(ctx, 1, metric.WithAttributes(errorTypeKey.String("skipped")))
		return pubAt.Add(minStaleActive), nil
	}

	row := tx.QueryRowContext(ctx, s.q.SQL("SELECT seq, rootHash FROM IntCoord WHERE id = $1"), 0)
	var fromSeq uint64
	var rootHash []byte
	if err := row.Scan(&fromSeq, &rootHash); err != nil {
		return time.Time{}, fmt.Errorf("failed to read IntCoord: %v", err)
	}

	currentSize := fromSeq
	shouldPublish := minStaleRepub > 0 && cpAge >= minStaleRepub

	if !shouldPublish {
		if !lastSize.Valid {
			// If we don't know the last published size, we should probably publish to be safe/self-heal.
			shouldPublish = true
		} else if currentSize > uint64(lastSize.Int64) {
			shouldPublish = true
		}
	}

	if !shouldPublish {
		slog.DebugContext(ctx, "publishCheckpoint: skipping publish because tree hasn't grown and previous checkpoint is too recent")
		publishCount.Add(ctx, 1, metric.WithAttributes(errorTypeKey.String("skipped_no_growth")))
		nextPubAt = start.Add(minStaleActive)
		if minStaleRepub > 0 && nextPubAt.After(pubAt.Add(minStaleRepub)) {
			nextPubAt = pubAt.Add(minStaleRepub)
		}
		return nextPubAt, nil
	}

	slog.DebugContext(ctx, "publishCheckpoint: updating checkpoint (replacing old checkpoint)", slog.Duration("age", cpAge))

	if err := f(withTx(ctx, tx), fromSeq, rootHash); err != nil {
		return time.Time{}, err
	}

	pubAt = time.Now()
	if _, err := tx.ExecContext(ctx, s.q.SQL("UPDATE PubCoord SET publishedAt=$1, size=$2 WHERE id=$3"), pubAt.UnixNano(), currentSize, 0); err != nil {
		return time.Time{}, err
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, err
	}
	opsHistogram.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(opNameKey.String("publishCheckpoint")))
	publishCount.Add(ctx, 1)
	tx = nil

	nextPubAt = pubAt.Add(minStaleActive)
	return nextPubAt, nil
}

// garbageCollect will identify up to maxBundles unneeded partial entry bundles (and any unneeded partial tiles which sit above them in the tree) and
// call the provided function to remove them.
//
// Uses the `GCCoord` table to ensure that only one binary is actively garbage collecting at any given time, and to track progress so that we don't
// needlessly attempt to GC over regions which have already been cleaned.
func (s *pgSequencer) garbageCollect(ctx context.Context, treeSize uint64, maxBundles uint, deleteWithPrefix func(ctx context.Context, prefix string) error, entriesPath func(uint64, uint8) string) error {
	// number of concurrent S3 deletes to perform while collecting garbage.
	const numWorkers = 5
	tx, err := pgtx.Begin(ctx, s.dbPool)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()
	pRow := tx.QueryRowContext(ctx, s.q.SQL("SELECT fromSize FROM GCCoord WHERE id = $1 FOR UPDATE"), 0)
	var fromSize uint64
	if err := pRow.Scan(&fromSize); err != nil {
		return fmt.Errorf("failed to parse publishedAt: %v", err)
	}

	if fromSize == treeSize {
		return nil
	}

	d := uint(0)
	work := make(chan string, maxBundles*3)

	eg := errgroup.Group{}
	eg.Go(func() error {
		// GC the tree in "vertical" chunks defined by entry bundles.
		for ri := range layout.Range(fromSize, treeSize-fromSize, treeSize) {
			// Only known-full bundles are in-scope for for GC, so exit if the current bundle is partial or
			// we've reached our limit of chunks.
			if ri.Partial > 0 || d > maxBundles {
				break
			}

			// GC any partial versions of the entry bundle itself and the tile which sits immediately above it.
			work <- entriesPath(ri.Index, 0) + ".p/"
			work <- layout.TilePath(0, ri.Index, 0) + ".p/"
			fromSize += uint64(ri.N)
			d++

			// Now consider (only) the part of the tree which sits above the bundle.
			// We'll walk up the parent tiles for as a long as we're tracing the right-hand
			// edge of a perfect subtree.
			// This gives the property we'll only visit each parent tile once, rather than up to 256 times.
			pL, pIdx := uint64(0), ri.Index
			for isLastLeafInParent(pIdx) {
				// Move our coordinates up to the parent
				pL, pIdx = pL+1, pIdx>>layout.TileHeight
				// GC any partial versions of the parent tile.
				work <- layout.TilePath(pL, pIdx, 0) + ".p/"
			}
		}
		close(work)
		return nil
	})
	for range numWorkers {
		eg.Go(func() error {
			errs := []error{}
			for prefix := range work {
				if err := deleteWithPrefix(ctx, prefix); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to delete one or more objects: %v", err)
	}

	if _, err := tx.ExecContext(ctx, s.q.SQL("UPDATE GCCoord SET fromSize=$1 WHERE id=$2"), fromSize, 0); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil

	return nil
}

// isLastLeafInParent returns true if a tile with the provided index is the final child node of a
// (hypothetical) full parent tile.
func isLastLeafInParent(i uint64) bool {
	return i%layout.TileWidth == layout.TileWidth-1
}
