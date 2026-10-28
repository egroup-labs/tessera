// Copyright 2025 The Tessera authors. All Rights Reserved.
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

// Package antispam contains a PostgreSQL-based antispam implementation for Tessera,
// intended for use with the s3pg storage driver.
//
// A PostgreSQL database provides a mechanism for maintaining an index of
// hash --> log position for detecting duplicate submissions. See schema.sql
// for the tables used; they may share a PostgreSQL schema with the s3pg log tables.
package antispam

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"iter"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/otel"
	"github.com/transparency-dev/tessera/storage/s3pg/internal/pgtx"
	"go.opentelemetry.io/otel/trace"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	DefaultMaxBatchSize      = 64
	DefaultPushbackThreshold = 2048

	// SchemaCompatibilityVersion represents the expected version (e.g. layout & serialisation) of stored data.
	//
	// A binary built with a given version of the Tessera library is compatible with stored data created by a different version
	// of the library if and only if this value is the same as the compatibilityVersion stored in the Tessera table.
	//
	// NOTE: if changing this version, you need to consider whether end-users are going to update their schema instances to be
	// compatible with the new format, and provide a means to do it if so.
	SchemaCompatibilityVersion = 1

	// defaultBatchTimeout is the max permitted duration for a single "chunk" of antispam updates.
	defaultBatchTimeout = 10 * time.Second

	// initLockSQL takes the transaction-scoped advisory lock which serialises concurrent schema
	// initialisation (concurrent CREATE TABLE IF NOT EXISTS of the same table can otherwise fail).
	// The key is distinct from the one used by the s3pg log, since both may share a schema.
	initLockSQL = "SELECT pg_advisory_xact_lock(hashtext('tessera/s3pg/antispam/init'))"
)

// SchemaSQL is the idempotent DDL (and initial rows) for the tables used by this antispam implementation.
// It is applied automatically at start-up unless AntispamOpts.SkipSchemaInit is set.
//
//go:embed schema.sql
var SchemaSQL string

// Every transaction runs at READ COMMITTED with a durable commit (see internal/pgtx): the
// follower's locking protocol relies on READ COMMITTED semantics.

// antispamTableNames are the names of the tables used by this package, as written in SQL statements.
var antispamTableNames = []string{"AntispamMeta", "AntispamIDSeq", "AntispamFollowCoord"}

// AntispamOpts allows configuration of some tunable options.
type AntispamOpts struct {
	// MaxBatchSize is the largest number of mutations permitted in a single write operation when
	// updating the antispam index.
	//
	// Larger batches can enable (up to a point) higher throughput, but care should be taken not to
	// overload the database instance.
	MaxBatchSize uint

	// PushbackThreshold allows configuration of when to start responding to Add requests with pushback due to
	// the antispam follower falling too far behind.
	//
	// When the antispam follower is at least this many entries behind the size of the locally integrated tree,
	// the antispam decorator will return a wrapped tessera.ErrPushback for every Add request.
	PushbackThreshold uint

	PushbackMaxOutstanding uint64
	// MaxOpenConns and MaxIdleConns size the pool opened by NewAntispam; they are ignored by NewAntispamWithDB.
	// MaxOpenConns zero means unlimited. MaxIdleConns zero keeps the database/sql default (2), and a
	// negative value disables idle connections (unlike storage/aws/antispam, where zero disables them).
	MaxOpenConns int
	MaxIdleConns int

	// SkipSchemaInit disables the creation of the database schema at start-up.
	//
	// Set this when the schema is managed by external migration tooling (see SchemaSQL /
	// schema.sql), so that the runtime database role does not need DDL privileges. The
	// schema must already exist, otherwise construction fails.
	SkipSchemaInit bool

	// Schema, if set, is the PostgreSQL schema holding the antispam tables. Every statement then
	// names its tables fully ("schema".table), independently of search_path. Strongly recommended
	// when connecting through a pooler such as PgBouncer. If empty, tables are resolved via search_path.
	Schema string
}

type AntispamStorage struct {
	opts AntispamOpts
	// q qualifies table names in SQL statements with opts.Schema, if set.
	q *pgtx.Qualifier

	dbPool *sql.DB
	// ownsDB is true if dbPool was opened by this instance, and so must be closed by Close.
	ownsDB bool

	// pushBack is used to prevent the follower from getting too far underwater.
	// Populate dynamically will set this to true/false based on how far behind the follower is from the
	// currently integrated tree size.
	// When pushBack is true, the decorator will start returning a wrapped ErrPushback to all calls.
	pushBack atomic.Bool

	numLookups atomic.Uint64
	numWrites  atomic.Uint64
	numHits    atomic.Uint64
}

// NewAntispam returns an antispam driver which uses a PostgreSQL table to maintain a mapping of
// previously seen entries and their assigned indices.
//
// The dsn is a pgx connection string. Select the PostgreSQL schema holding the antispam tables with
// opts.Schema (recommended), or else via search_path.
// The returned instance owns the pool it opens, which is released by Close.
//
// Note that the storage for this mapping is entirely separate and unconnected to the storage used for
// maintaining the Merkle tree.
//
// This functionality is experimental!
func NewAntispam(ctx context.Context, dsn string, opts AntispamOpts) (*AntispamStorage, error) {
	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL db: %v", err)
	}

	if opts.MaxOpenConns > 0 {
		dbPool.SetMaxOpenConns(opts.MaxOpenConns)
	}
	// Unlike upstream (where 0 disables idle connections), 0 leaves the database/sql default in
	// place: establishing a new PostgreSQL connection for every lookup is very expensive.
	// A negative value disables idle connections.
	if opts.MaxIdleConns != 0 {
		dbPool.SetMaxIdleConns(opts.MaxIdleConns)
	}

	r, err := newAntispam(ctx, dbPool, true, opts)
	if err != nil {
		_ = dbPool.Close()
		return nil, err
	}
	return r, nil
}

// NewAntispamWithDB is like NewAntispam, but uses an externally managed database handle, which must
// have been opened with the "pgx" driver from github.com/jackc/pgx/v5/stdlib (other drivers are not
// supported).
//
// The handle is used as-is: opts.MaxOpenConns and opts.MaxIdleConns are not applied, and the handle
// is never closed by this package (including by Close).
func NewAntispamWithDB(ctx context.Context, db *sql.DB, opts AntispamOpts) (*AntispamStorage, error) {
	if db == nil {
		return nil, errors.New("db must not be nil")
	}
	return newAntispam(ctx, db, false, opts)
}

func newAntispam(ctx context.Context, dbPool *sql.DB, ownsDB bool, opts AntispamOpts) (*AntispamStorage, error) {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = DefaultMaxBatchSize
	}
	if opts.PushbackThreshold == 0 {
		opts.PushbackThreshold = DefaultPushbackThreshold
	}

	if err := dbPool.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL db: %v", err)
	}

	r := &AntispamStorage{
		opts:   opts,
		q:      pgtx.NewQualifier(opts.Schema, antispamTableNames...),
		dbPool: dbPool,
		ownsDB: ownsDB,
	}

	if !opts.SkipSchemaInit {
		if err := r.initDB(ctx); err != nil {
			return nil, fmt.Errorf("failed to initDB: %v", err)
		}
	}
	if err := r.checkDataCompatibility(ctx); err != nil {
		return nil, fmt.Errorf("schema is not compatible with this version of the Tessera library: %v", err)
	}
	return r, nil
}

// Close releases the database pool if it was opened by NewAntispam.
// A handle passed to NewAntispamWithDB is left open.
func (s *AntispamStorage) Close() error {
	if !s.ownsDB {
		return nil
	}
	return s.dbPool.Close()
}

// initDB applies SchemaSQL, creating the tables and initial rows if necessary.
//
// The initial rows are a precondition for following and population of mapping data to occur.
// They are only inserted if no row exists, so there's no danger of "resetting" an existing antispam database.
// This is done in a single transaction holding an advisory lock, so that concurrent initialisation of the
// same schema by multiple instances is safe.
func (s *AntispamStorage) initDB(ctx context.Context) error {
	tx, err := pgtx.Begin(ctx, s.dbPool)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()
	if _, err := tx.ExecContext(ctx, initLockSQL); err != nil {
		return fmt.Errorf("failed to take schema init lock: %v", err)
	}
	if err := s.q.SetSearchPath(ctx, tx); err != nil {
		return fmt.Errorf("failed to select schema: %v", err)
	}
	// SchemaSQL holds multiple statements, which is fine as it is sent without arguments
	// (pgx then uses the simple query protocol).
	if _, err := tx.ExecContext(ctx, SchemaSQL); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

// checkDataCompatibility compares the Tessera library SchemaCompatibilityVersion with the one stored in the
// database, and returns an error if they are not identical.
func (s *AntispamStorage) checkDataCompatibility(ctx context.Context) error {
	row := s.dbPool.QueryRowContext(ctx, s.q.SQL("SELECT compatibilityVersion FROM AntispamMeta WHERE id = 0"))
	var gotVersion uint64
	if err := row.Scan(&gotVersion); err != nil {
		return fmt.Errorf("failed to read schema compatibility version from DB: %v", err)
	}

	if gotVersion != SchemaCompatibilityVersion {
		return fmt.Errorf("schema compatibilityVersion (%d) != library compatibilityVersion (%d)", gotVersion, SchemaCompatibilityVersion)
	}
	return nil
}

// index returns the index (if any) previously associated with the provided hash
func (d *AntispamStorage) index(ctx context.Context, h []byte) (*uint64, error) {
	d.numLookups.Add(1)
	row := d.dbPool.QueryRowContext(ctx, d.q.SQL("SELECT idx FROM AntispamIDSeq WHERE h = $1"), h)

	var idx uint64
	if err := row.Scan(&idx); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("error during scan: %v", err)
	}
	d.numHits.Add(1)
	return &idx, nil
}

// Decorator returns a function which will wrap an underlying Add delegate with
// code to dedup against the stored data.
func (d *AntispamStorage) Decorator() func(f tessera.AddFn) tessera.AddFn {
	return func(delegate tessera.AddFn) tessera.AddFn {
		return func(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
			if d.pushBack.Load() {
				// The follower is too far behind the currently integrated tree, so we're going to push back against
				// the incoming requests.
				// This should have two effects:
				//   1. The tree will cease growing, giving the follower a chance to catch up, and
				//   2. We'll stop doing lookups for each submission, freeing up the DB to catch up.
				//
				// We may decide in the future that serving duplicate reads is more important than catching up as quickly
				// as possible, in which case we'd move this check down below the call to index.
				return func() (tessera.Index, error) { return tessera.Index{}, tessera.ErrPushbackAntispam }
			}
			idx, err := d.index(ctx, e.Identity())
			if err != nil {
				return func() (tessera.Index, error) { return tessera.Index{}, err }
			}
			if idx != nil {
				return func() (tessera.Index, error) { return tessera.Index{Index: *idx, IsDup: true}, nil }
			}

			return delegate(ctx, e)
		}
	}
}

// Follower returns a follower which knows how to populate the antispam index.
//
// This implements tessera.Antispam.
func (d *AntispamStorage) Follower(b func([]byte) ([][]byte, error)) tessera.Follower {
	return &follower{
		as:           d,
		bundleHasher: b,
	}
}

// follower is a struct which knows how to populate the antispam storage with identity hashes
// for entries in a log.
type follower struct {
	as           *AntispamStorage
	bundleHasher func([]byte) ([][]byte, error)
}

func (f *follower) Name() string {
	return "S3PG antispam"
}

// Follow uses entry data from the log to populate the antispam storage.
func (f *follower) Follow(followCtx context.Context, lr tessera.LogReader) {
	errOutOfSync := errors.New("out-of-sync")

	t := time.NewTicker(time.Second)
	var (
		next func() (client.Entry[[]byte], error, bool)
		stop func()
	)

	defer func() {
		if stop != nil {
			stop()
		}
	}()

	for {
		select {
		case <-followCtx.Done():
			return
		case <-t.C:
		}

		// logSize is the latest known size of the log we're following.
		// This will get initialised below, inside the loop.
		var logSize uint64

		// Busy loop while there's work to be done
		for streamDone := false; !streamDone; {
			select {
			case <-followCtx.Done():
				return
			default:
			}
			err := otel.TraceErr(followCtx, "tessera.antispam.s3pg.FollowTask", tracer, func(ctx context.Context, span trace.Span) error {
				ctx, cancel := context.WithTimeout(ctx, defaultBatchTimeout)
				defer cancel()

				tx, err := pgtx.Begin(ctx, f.as.dbPool)
				if err != nil {
					return err
				}

				defer func() {
					if tx != nil {
						pgtx.Rollback(ctx, tx)
					}
				}()

				row := tx.QueryRowContext(ctx, f.as.q.SQL("SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0 FOR UPDATE"))

				var followFrom uint64
				if err := row.Scan(&followFrom); err != nil {
					return err
				}

				if followFrom >= logSize {
					// Our view of the log is out of date, update it
					logSize, err = lr.IntegratedSize(ctx)
					if err != nil {
						streamDone = true
						return fmt.Errorf("populate: IntegratedSize(): %v", err)
					}
					switch {
					case followFrom > logSize:
						streamDone = true
						return fmt.Errorf("followFrom %d > size %d", followFrom, logSize)
					case followFrom == logSize:
						// We're caught up, so unblock pushback and go back to sleep
						streamDone = true
						f.as.pushBack.Store(false)
						return ctx.Err()
					default:
						// size > followFrom, so there's more work to be done!
					}
				}

				f.as.pushBack.Store(logSize-followFrom > uint64(f.as.opts.PushbackThreshold))

				// If this is the first time around the loop we need to start the stream of entries now that we know where we want to
				// start reading from:
				if next == nil {
					streamSize := logSize
					sizeFn := func(_ context.Context) (uint64, error) {
						return streamSize, nil
					}
					numFetchers := uint(10)
					next, stop = iter.Pull2(client.Entries(client.EntryBundles(followCtx, numFetchers, sizeFn, lr.ReadEntryBundle, followFrom, streamSize-followFrom), f.bundleHasher))
				}

				bs := uint64(f.as.opts.MaxBatchSize)
				if r := logSize - followFrom; r < bs {
					bs = r
				}
				curEntries := make([][]byte, 0, bs)
				for i := range int(bs) {
					e, err, ok := next()
					if !ok {
						// The entry stream has ended so we'll need to start a new stream next time around the loop:
						stop()
						next = nil
						break
					}
					if err != nil {
						return fmt.Errorf("entryReader.next: %v", err)
					}
					if wantIdx := followFrom + uint64(i); e.Index != wantIdx {
						// We're out of sync
						return errOutOfSync
					}
					curEntries = append(curEntries, e.Entry)
				}

				if len(curEntries) == 0 {
					return ctx.Err()
				}

				slog.DebugContext(ctx, "Inserting entries into antispam database", slog.Int("count", len(curEntries)), slog.Uint64("followfrom", followFrom), slog.Uint64("logsize", logSize))

				// The whole batch is sent as two array parameters, so its size is not limited by the maximum
				// number of bind parameters per statement.
				// As with MySQL's INSERT IGNORE, duplicates (within the batch or with existing rows) are skipped,
				// so the first (lowest) index seen for a given hash is the one which is kept.
				idxs := make([]uint64, 0, len(curEntries))
				for i := range curEntries {
					idxs = append(idxs, followFrom+uint64(i))
				}
				const sqlStr = "INSERT INTO AntispamIDSeq (h, idx) SELECT h, idx FROM unnest($1::bytea[], $2::bigint[]) AS t(h, idx) ORDER BY idx ON CONFLICT (h) DO NOTHING"

				_, err = tx.ExecContext(ctx, f.as.q.SQL(sqlStr), curEntries, idxs)
				if err != nil {
					return fmt.Errorf("failed to insert into AntispamIDSeq with query %q: %v", sqlStr, err)
				}
				numAdded := uint64(len(curEntries))
				f.as.numWrites.Add(numAdded)
				nextIdx := uint64(followFrom + numAdded)

				// Insertion of dupe entries was successful, so update our follow coordination row:
				_, err = tx.ExecContext(ctx, f.as.q.SQL("UPDATE AntispamFollowCoord SET nextIdx=$1 WHERE id=0"), nextIdx)
				if err != nil {
					return fmt.Errorf("error updating AntispamFollowCoord: %v", err)
				}
				if err := tx.Commit(); err != nil {
					return err
				}
				tx = nil
				return ctx.Err()
			})
			if err != nil {
				if err != errOutOfSync {
					slog.ErrorContext(followCtx, "Failed to commit antispam population tx", slog.Any("error", err))
				}
				if next != nil {
					stop()
					next = nil
					stop = nil
				}
				streamDone = true
				continue
			}
		}
	}
}

// EntriesProcessed returns the total number of log entries processed.
func (f *follower) EntriesProcessed(ctx context.Context) (uint64, error) {
	row := f.as.dbPool.QueryRowContext(ctx, f.as.q.SQL("SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0"))

	var idx uint64
	if err := row.Scan(&idx); err != nil {
		return 0, fmt.Errorf("failed to read follow coordination info: %v", err)
	}

	return idx, nil
}
