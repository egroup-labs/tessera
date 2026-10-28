// Copyright 2024 The Tessera authors. All Rights Reserved.
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

// Package s3pg contains an S3 + PostgreSQL storage implementation for Tessera.
//
// It is derived from the storage/aws driver: S3-compatible object storage (e.g. MinIO)
// is used for long-term storage and serving of entry bundles and log tiles, and
// PostgreSQL is used for coordinating updates when multiple instances of a
// personality binary are running.
//
// A single bucket is used to hold entry bundles and log internal tiles.
// The object keys for the bucket are selected so as to conform to the
// expected layout of a tile-based log.
//
// A PostgreSQL database provides a transactional mechanism to allow multiple
// frontends to safely update the contents of the log. See schema.sql for the
// tables used; one log uses one PostgreSQL schema (select it with Config.Schema).
package s3pg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/fetcher"
	"github.com/transparency-dev/tessera/internal/migrate"
	"github.com/transparency-dev/tessera/internal/otel"
	"github.com/transparency-dev/tessera/internal/parse"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"github.com/transparency-dev/tessera/storage/s3pg/internal/pgtx"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	logContType           = "application/octet-stream"
	ckptContType          = "text/plain; charset=utf-8"
	logCacheControl       = "max-age=604800,immutable"
	ckptCacheControl      = "no-cache"
	minCheckpointInterval = time.Second

	DefaultPushbackMaxOutstanding = 4096
	DefaultIntegrationSizeLimit   = 5 * 4096

	// SchemaCompatibilityVersion represents the expected version (e.g. layout & serialisation) of stored data.
	//
	// A binary built with a given version of the Tessera library is compatible with stored data created by a different version
	// of the library if and only if this value is the same as the compatibilityVersion stored in the Tessera table.
	//
	// NOTE: if changing this version, you need to consider whether end-users are going to update their schema instances to be
	// compatible with the new format, and provide a means to do it if so.
	SchemaCompatibilityVersion = 1

	// defaultAssignEntriesTimeout is the default context timeout applied when assigning a batch of entries to the PostgreSQL sequencer.
	defaultAssignEntriesTimeout = 2 * time.Second
	// defaultIntegrationTimeout is the default context timeout applied when undertaking an integration task.
	defaultIntegrationTimeout = 10 * time.Second
	// defaultGCTimeout is the default context timeout applied when undertaking a garbage collection task.
	defaultGCTimeout = 30 * time.Second
)

// Storage is an S3 + PostgreSQL based storage implementation for Tessera.
type Storage struct {
	cfg Config

	mu         sync.Mutex
	sequencers []*pgSequencer
	behind     atomic.Pointer[error]
}

// CoordinationError returns a non-nil error (wrapping ErrCoordinationBehind) once an appender
// created by this Storage has found the coordination database to be behind, or inconsistent
// with, the published log. The appender then refuses new entries and stops integrating and
// publishing. Recovery: reconcile the database with the bucket, delete the tripped marker object,
// and restart (see README.md, "Recovering from a lost commit"). Use it e.g. in health checks.
func (s *Storage) CoordinationError() error {
	if e := s.behind.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *Storage) setBehind(err error) {
	s.behind.CompareAndSwap(nil, &err)
}

// Close releases the PostgreSQL connection pools opened by this Storage (i.e. those opened from
// Config.DSN; a Config.DB handle is never closed). Call it only once the Appender / MigrationWriter
// and the LogReader obtained from this Storage are no longer in use.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := []error{}
	for _, seq := range s.sequencers {
		errs = append(errs, seq.close())
	}
	s.sequencers = nil
	return errors.Join(errs...)
}

func (s *Storage) track(seq *pgSequencer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequencers = append(s.sequencers, seq)
}

// objStore describes a type which can store and retrieve objects.
type objStore interface {
	getObject(ctx context.Context, obj string) ([]byte, error)
	setObject(ctx context.Context, obj string, data []byte, contType string, cacheControl string) error
	setObjectIfNoneMatch(ctx context.Context, obj string, data []byte, contType string, cacheControl string) error
	deleteObjectsWithPrefix(ctx context.Context, prefix string) error

	// s3pg additions:

	// getObjectWithETag is getObject, also returning the object's ETag.
	getObjectWithETag(ctx context.Context, obj string) ([]byte, string, error)
	// setObjectIfETag stores data only if the object's current ETag is etag, or, if etag is
	// empty, only if the object doesn't exist. It returns errPreconditionFailed otherwise.
	setObjectIfETag(ctx context.Context, obj string, data []byte, contType string, cacheControl string, etag string) error
	// listObjects returns the names of all objects whose name starts with prefix.
	listObjects(ctx context.Context, prefix string) ([]string, error)
}

// errPreconditionFailed is returned by objStore.setObjectIfETag when the object has changed.
var errPreconditionFailed = errors.New("precondition failed")

// sequencer describes a type which knows how to sequence entries.
type sequencer interface {
	// assignEntries should durably allocate contiguous index numbers to the provided entries.
	assignEntries(ctx context.Context, entries []*tessera.Entry) error
	// consumeEntries should call the provided function with up to limit previously sequenced entries.
	// If the call to consumeFunc returns no error, the entries should be considered to have been consumed.
	// If any entries were successfully consumed, the implementation should also return true; this
	// serves as a weak hint that there may be more entries to be consumed.
	// If forceUpdate is true, then the consumeFunc should be called, with an empty slice of entries if
	// necessary. This allows the log self-initialise in a transactionally safe manner.
	consumeEntries(ctx context.Context, limit uint64, f consumeFunc, forceUpdate bool) (bool, error)

	// currentTree returns the sequencer's view of the current tree state.
	currentTree(ctx context.Context) (uint64, []byte, error)

	// nextIndex returns the next available index in the log.
	nextIndex(ctx context.Context) (uint64, error)

	// publishCheckpoint coordinates the publication of new checkpoints based on the current integrated tree.
	publishCheckpoint(ctx context.Context, minStaleActive, minStaleRepub time.Duration, f func(ctx context.Context, size uint64, root []byte) error) (time.Time, error)

	// garbageCollect coordinates the removal of unneeded partial tiles/entry bundles for the provided tree size, up to a maximum number of deletes per invocation.
	garbageCollect(ctx context.Context, treeSize uint64, maxDeletes uint, removePrefix func(ctx context.Context, prefix string) error, entriesPath func(uint64, uint8) string) error
}

// consumeFunc is the signature of a function which can consume entries from the sequencer.
// Returns the updated root hash of the tree with the consumed entries integrated.
type consumeFunc func(ctx context.Context, from uint64, entries []storage.SequencedEntry) ([]byte, error)

// Config holds S3 and PostgreSQL configuration for a storage instance.
type Config struct {
	// SDKConfig is an optional AWS SDK config to use when configuring the S3 client, e.g. to
	// use non-AWS S3-compatible services such as MinIO.
	//
	// If nil, the value from config.LoadDefaultConfig() will be used.
	SDKConfig *aws.Config
	// S3Options is an optional function which can be used to configure the S3 library,
	// e.g. to set BaseEndpoint, static credentials and UsePathStyle for MinIO.
	S3Options func(*s3.Options)
	// Bucket is the name of the S3 bucket to use for storing log state.
	Bucket string
	// BucketPrefix is an optional prefix to prepend to all log resource paths.
	// This can be used e.g. to store multiple logs in the same bucket.
	BucketPrefix string

	// DSN is the PostgreSQL connection string (URL or keyword/value form understood by pgx)
	// used to coordinate the log, e.g. "postgres://user@host/db?sslmode=verify-full". Select the
	// PostgreSQL schema holding this log's tables with Schema (recommended), or else via search_path.
	//
	// DSN is ignored if DB is set.
	DSN string
	// DB is an optional, externally managed database handle. It must have been opened with the
	// "pgx" driver from github.com/jackc/pgx/v5/stdlib (other drivers are not supported). When set,
	// DSN, MaxOpenConns and MaxIdleConns are ignored, and Tessera will never close the handle.
	DB *sql.DB
	// Maximum connections to the database. Zero means unlimited.
	MaxOpenConns int
	// Maximum idle database connections in the connection pool. Zero keeps the database/sql
	// default (2), and a negative value disables idle connections. Note that this differs from
	// storage/aws, where zero disables idle connections: a new PostgreSQL connection per query
	// is expensive.
	MaxIdleConns int
	// Schema, if set, is the PostgreSQL schema holding this log's tables. Every statement then
	// names its tables fully ("schema".table), independently of search_path. Strongly recommended
	// when connecting through a pooler such as PgBouncer, which may drop, ignore or even leak the
	// search_path startup parameter between clients. If empty, tables are resolved via search_path.
	Schema string
	// SkipSchemaInit disables the creation of the database schema at start-up.
	//
	// Set this when the schema is managed by external migration tooling (see SchemaSQL /
	// schema.sql), so that the runtime database role does not need DDL privileges. The
	// schema must already exist, otherwise construction fails.
	SkipSchemaInit bool

	// HTTPClient will be used for other HTTP requests. If unset, Tessera will use the net/http DefaultClient.
	HTTPClient *http.Client
}

// New creates a new instance of the S3 + PostgreSQL based Storage.
//
// Storage instances created via this c'tor will participate in integrating newly sequenced entries into the log
// and periodically publishing a new checkpoint which commits to the state of the tree.
func New(ctx context.Context, cfg Config) (tessera.Driver, error) {
	if cfg.SDKConfig == nil {
		// We're running on AWS so use the SDK's default config which will will handle credentials etc.
		sdkConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load default AWS configuration: %v", err)
		}
		cfg.SDKConfig = &sdkConfig
	}
	if cfg.S3Options == nil {
		// We need a non-nil options func to pass in to s3.NewFromConfig below or it'll panic, so
		// we'll use a "do nothing" placeholder.
		// Note: unlike upstream storage/aws, a caller-supplied S3Options is honoured even when
		// SDKConfig is nil.
		cfg.S3Options = func(_ *s3.Options) {}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	return &Storage{
		cfg: cfg,
	}, nil
}

// Appender creates a new tessera.Appender lifecycle object.
func (s *Storage) Appender(ctx context.Context, opts *tessera.AppendOptions) (*tessera.Appender, tessera.LogReader, error) {
	seq, err := newPGSequencer(ctx, s.cfg, uint64(opts.PushbackMaxOutstanding()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create PostgreSQL sequencer: %v", err)
	}

	s3Store := &s3Storage{
		s3Client:     s3.NewFromConfig(*s.cfg.SDKConfig, s.cfg.S3Options),
		bucket:       s.cfg.Bucket,
		bucketPrefix: s.cfg.BucketPrefix,
	}

	a, lr, err := s.newAppender(ctx, s3Store, seq, opts)
	if err != nil {
		_ = seq.close()
		return nil, nil, err
	}
	s.track(seq)
	return &tessera.Appender{
		Add: a.Add,
	}, lr, nil
}

// newAppender creates and initialises an Appender struct with the provided underlying storage implementations.
func (s *Storage) newAppender(ctx context.Context, o objStore, seq sequencer, opts *tessera.AppendOptions) (*Appender, tessera.LogReader, error) {
	if opts.CheckpointInterval() < minCheckpointInterval {
		return nil, nil, fmt.Errorf("requested CheckpointInterval (%v) is less than minimum permitted %v", opts.CheckpointInterval(), minCheckpointInterval)
	}

	logStore := &logResourceStore{
		objStore:    o,
		entriesPath: opts.EntriesPath(),
		integratedSize: func(ctx context.Context) (uint64, error) {
			s, _, err := seq.currentTree(ctx)
			return s, err
		},
		nextIndex: func(ctx context.Context) (uint64, error) {
			return seq.nextIndex(ctx)
		},
	}

	newCP, err := opts.CheckpointPublisherContext(ctx, logStore, s.cfg.HTTPClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create checkpoint publisher: %v", err)
	}

	r := &Appender{
		logStore:        logStore,
		sequencer:       seq,
		newCP:           newCP,
		treeUpdated:     make(chan struct{}),
		entriesAssigned: make(chan struct{}, 1),
	}
	r.onBehind = s.setBehind
	if ps, ok := seq.(*pgSequencer); ok {
		ps.minNext = r.highWater.Load
	}
	r.queue = storage.NewQueue(ctx, opts.BatchMaxAge(), opts.BatchMaxSize(), func(ctx context.Context, entries []*tessera.Entry) error {
		// Don't hand out indices which may already be in use in the published log (see guard.go).
		if err := r.behindErr(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, defaultAssignEntriesTimeout)
		defer cancel()
		if err := r.sequencer.assignEntries(ctx, entries); err != nil {
			return r.trip(ctx, err)
		}
		select {
		case r.entriesAssigned <- struct{}{}:
		default:
		}
		return nil
	})

	if err := r.init(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to initialise log storage: %v", err)
	}

	// Kick off go-routine which handles the integration of entries.
	go r.integrateEntriesJob(ctx)

	// Kick off go-routine which handles the publication of checkpoints.
	go r.publishCheckpointJob(ctx, opts.CheckpointInterval(), opts.CheckpointRepublishInterval(), opts.CheckpointPublicationTimeout())

	if i := opts.GarbageCollectionInterval(); i > 0 {
		go r.garbageCollectorJob(ctx, i)
	}

	return r, r.logStore, nil
}

// Appender is an implementation of the Tessera appender lifecycle contract.
type Appender struct {
	newCP func(context.Context, uint64, []byte) ([]byte, error)

	// behind is set, permanently, once a guard has found the coordination database to be behind
	// the published log (see guard.go). New entries are then refused.
	behind atomic.Pointer[error]
	// onBehind, if set, is called when behind is first set.
	onBehind func(error)
	// highWater is the largest log size this instance has seen published or integrated (see guard.go).
	highWater atomic.Uint64

	sequencer sequencer
	logStore  *logResourceStore

	queue *storage.Queue

	treeUpdated     chan struct{}
	entriesAssigned chan struct{}
}

// integrateEntriesJob periodically appends newly sequenced entries to the log.
//
// This function does not return until the passed context is done.
func (a *Appender) integrateEntriesJob(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	// Unlike upstream, the number of Seq rows integrated per attempt adapts: it is halved after a
	// failed attempt and doubled (up to DefaultIntegrationSizeLimit) after a successful one. Failed
	// attempts keep nothing, and a retry of a large batch whose objects were already written (e.g.
	// after a lost commit) costs much more than the first attempt (see checkBucketConsistent), so
	// a fixed size could keep failing on the integration timeout forever.
	limit := uint64(DefaultIntegrationSizeLimit)
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.entriesAssigned:
		case <-t.C:
		}

		if err := otel.TraceErr(ctx, "tessera.storage.s3pg.integrateEntriesJob", tracer, func(ctx context.Context, span trace.Span) error {
			start := time.Now()
			defer func() {
				opsHistogram.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(opNameKey.String("integrateEntries")))
			}()

			ctx, cancel := context.WithTimeout(ctx, defaultIntegrationTimeout)
			defer cancel() // Note: ok because we're in a func passed to TraceErr here!

			if _, err := a.sequencer.consumeEntries(ctx, limit, a.integrateEntries, false); err != nil {
				limit = max(1, limit/2)
				return err
			}
			limit = min(DefaultIntegrationSizeLimit, limit*2)
			select {
			case a.treeUpdated <- struct{}{}:
			default:
			}
			return nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true))); err != nil {
			slog.ErrorContext(ctx, "integrateEntries", slog.Any("error", err))
		}
	}
}

// publishCheckpointJob periodically attempts to publish a new checkpoint representing the current state
// of the tree, once per interval.
//
// This function does not return until the passed in context is done.
func (a *Appender) publishCheckpointJob(ctx context.Context, pubInterval, republishInterval, publicationTimeout time.Duration) {
	t := time.NewTicker(pubInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.treeUpdated:
		case <-t.C:
		}
		if err := otel.TraceErr(ctx, "tessera.storage.s3pg.publishCheckpointJob", tracer, func(ctx context.Context, span trace.Span) error {
			ctx, cancel := context.WithTimeout(ctx, publicationTimeout)
			defer cancel() // Note: ok because we're in a func passed to TraceErr here!

			nextPub, err := a.sequencer.publishCheckpoint(ctx, pubInterval, republishInterval, a.updateCheckpoint)
			if err != nil {
				return fmt.Errorf("publishCheckpoint failed: %v", err)
			}
			// Schedule a checkpoint update immediately, if an updated is due.
			t.Reset(max(time.Millisecond, time.Until(nextPub)))
			return nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true))); err != nil {
			t.Reset(pubInterval)
			slog.ErrorContext(ctx, "Failed to execute checkpoint publisher job", slog.Any("error", err))
		}
	}
}

// garbageCollectorJob is a long-running function which handles the removal of obsolete partial tiles
// and entry bundles.
// Blocks until ctx is done.
func (a *Appender) garbageCollectorJob(ctx context.Context, i time.Duration) {
	t := time.NewTicker(i)
	defer t.Stop()

	// Entirely arbitrary number.
	maxBundlesPerRun := uint(100)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := otel.TraceErr(ctx, "tessera.storage.s3pg.garbageCollectJob", tracer, func(ctx context.Context, span trace.Span) error {
			ctx, cancel := context.WithTimeout(ctx, defaultGCTimeout)
			defer cancel() // Note: ok because we're in a func passed to TraceErr here!

			// Figure out the size of the latest published checkpoint - we can't be removing partial tiles implied by
			// that checkpoint just because we've done an integration and know about a larger (but as yet unpublished)
			// checkpoint!
			cp, err := a.logStore.ReadCheckpoint(ctx)
			if err != nil {
				return fmt.Errorf("failed to get published checkpoint: %v", err)
			}
			_, pubSize, _, err := parse.CheckpointUnsafe(cp)
			if err != nil {
				return fmt.Errorf("failed to parse published checkpoint: %v", err)
			}

			if err := a.sequencer.garbageCollect(ctx, pubSize, maxBundlesPerRun, a.logStore.objStore.deleteObjectsWithPrefix, a.logStore.entriesPath); err != nil {
				return fmt.Errorf("garbageCollect failed: %v", err)
			}
			return nil
		}, trace.WithAttributes(otel.PeriodicKey.Bool(true))); err != nil {
			slog.WarnContext(ctx, "Failed to execute garbage Collector job", slog.Any("error", err))
		}
	}

}

// Add is the entrypoint for adding entries to a sequencing log.
func (a *Appender) Add(ctx context.Context, e *tessera.Entry) tessera.IndexFuture {
	return a.queue.Add(ctx, e)
}

// init ensures that the storage represents a log in a valid state.
func (a *Appender) init(ctx context.Context) error {
	// Refuse to start while a previous guard trip hasn't been resolved (see guard.go).
	if err := a.checkTrippedMarker(ctx); err != nil {
		return err
	}
	_, err := a.logStore.ReadCheckpoint(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No checkpoint exists, do a forced (possibly empty) integration to create one in a safe
			// way (calling updateCP directly here would not be safe as it's outside the transactional
			// framework which prevents the tree from rolling backwards or otherwise forking).
			ctx, c := context.WithTimeout(ctx, 10*time.Second)
			defer c()
			if _, err := a.sequencer.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, true); err != nil {
				return fmt.Errorf("forced integrate: %v", err)
			}
			select {
			case a.treeUpdated <- struct{}{}:
			default:
			}
			return nil
		}
		return fmt.Errorf("failed to read checkpoint: %v", err)
	}

	// Refuse to start against a coordination database which is behind the published log (see guard.go).
	return a.checkStartup(ctx)
}

func (a *Appender) updateCheckpoint(ctx context.Context, size uint64, root []byte) error {
	// Only ever publish append-only extensions of the published checkpoint (see guard.go), and
	// only if the checkpoint hasn't changed since we checked it: the PubCoord row lock serialises
	// publishers only while their transaction lives, and a write from a publisher whose transaction
	// has since ended (e.g. a delayed or retried PUT) must never land on top of a newer checkpoint.
	pubRaw, etag, err := a.logStore.readCheckpointWithETag(ctx)
	if err != nil {
		return err
	}
	if err := a.checkPublicationAllowed(ctx, pubRaw, size, root); err != nil {
		return err
	}
	cpRaw, err := a.newCP(ctx, size, root)
	if err != nil {
		return fmt.Errorf("newCP: %v", err)
	}

	if err := a.logStore.setCheckpointIfETag(ctx, cpRaw, etag); err != nil {
		return fmt.Errorf("writeCheckpoint: %w", err)
	}

	slog.DebugContext(ctx, "Created and stored latest checkpoint", slog.Uint64("size", size), slog.String("root", fmt.Sprintf("%x", root)))

	return nil
}

// integrateEntries appends the provided entries into the log starting at fromSeq.
//
// Returns the new root hash of the log with the entries added.
func (a *Appender) integrateEntries(ctx context.Context, fromSeq uint64, entries []storage.SequencedEntry) ([]byte, error) {
	// Never write tiles or bundles for a tree which is behind the published one (see guard.go).
	if err := a.checkIntegrationAllowed(ctx, fromSeq, entries); err != nil {
		return nil, err
	}
	// These entries have been sequenced, so the database has handed out indices up to here.
	a.observe(fromSeq + uint64(len(entries)))
	var newRoot []byte

	errG := errgroup.Group{}

	errG.Go(func() error {
		if err := a.updateEntryBundles(ctx, fromSeq, entries); err != nil {
			return fmt.Errorf("updateEntryBundles: %v", err)
		}
		return nil
	})

	errG.Go(func() error {
		lh := make([][]byte, len(entries))
		for i, e := range entries {
			lh[i] = e.LeafHash
		}
		r, err := integrate(ctx, fromSeq, lh, a.logStore)
		if err != nil {
			return fmt.Errorf("integrate: %v", err)
		}
		newRoot = r
		return nil
	})

	err := errG.Wait()
	return newRoot, err
}

// updateEntryBundles adds the entries being integrated into the entry bundles.
//
// The right-most bundle will be grown, if it's partial, and/or new bundles will be created as required.
func (a *Appender) updateEntryBundles(ctx context.Context, fromSeq uint64, entries []storage.SequencedEntry) error {
	if len(entries) == 0 {
		return nil
	}

	numAdded := uint64(0)
	bundleIndex, entriesInBundle := fromSeq/layout.EntryBundleWidth, fromSeq%layout.EntryBundleWidth
	bundleWriter := &bytes.Buffer{}
	if entriesInBundle > 0 {
		// If the latest bundle is partial, we need to read the data it contains in for our newer, larger, bundle.
		part, err := a.logStore.getEntryBundle(ctx, uint64(bundleIndex), uint8(entriesInBundle))
		if err != nil {
			return err
		}

		if _, err := bundleWriter.Write(part); err != nil {
			return fmt.Errorf("bundleWriter: %v", err)
		}
	}

	seqErr := errgroup.Group{}

	// goSetEntryBundle is a function which uses seqErr to spin off a go-routine to write out an entry bundle.
	// It's used in the for loop below.
	goSetEntryBundle := func(ctx context.Context, bundleIndex uint64, p uint8, bundleRaw []byte) {
		seqErr.Go(func() error {
			if err := a.logStore.setEntryBundle(ctx, bundleIndex, p, bundleRaw); err != nil {
				return err
			}
			return nil
		})
	}

	// Add new entries to the bundle
	for _, e := range entries {
		if _, err := bundleWriter.Write(e.BundleData); err != nil {
			return fmt.Errorf("bundlewriter.Write: %v", err)
		}
		entriesInBundle++
		fromSeq++
		numAdded++
		if entriesInBundle == layout.EntryBundleWidth {
			//  This bundle is full, so we need to write it out...
			slog.DebugContext(ctx, "In-memory bundle is full, attempting write to S3", slog.Uint64("bundleindex", bundleIndex))
			goSetEntryBundle(ctx, bundleIndex, 0, bundleWriter.Bytes())
			// ... and prepare the next entry bundle for any remaining entries in the batch
			bundleIndex++
			entriesInBundle = 0
			// Don't use Reset/Truncate here - the backing []bytes is still being used by goSetEntryBundle above.
			bundleWriter = &bytes.Buffer{}
			slog.DebugContext(ctx, "Starting to fill in-memory bundle", slog.Uint64("bundleindex", bundleIndex))
		}
	}
	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		slog.DebugContext(ctx, "Attempting to write in-memory partial bundle idx . to S3", slog.Uint64("bundleindex", bundleIndex), slog.Uint64("entriesinbundle", entriesInBundle))
		goSetEntryBundle(ctx, bundleIndex, uint8(entriesInBundle), bundleWriter.Bytes())
	}
	return seqErr.Wait()
}

// MigrationWriter creates a new S3 + PostgreSQL storage for the MigrationWriter lifecycle mode.
func (s *Storage) MigrationWriter(ctx context.Context, opts *tessera.MigrationOptions) (migrate.MigrationWriter, tessera.LogReader, error) {
	logStore := &logResourceStore{
		objStore: &s3Storage{
			s3Client:     s3.NewFromConfig(*s.cfg.SDKConfig, s.cfg.S3Options),
			bucket:       s.cfg.Bucket,
			bucketPrefix: s.cfg.BucketPrefix,
		},
		entriesPath: opts.EntriesPath(),
	}
	seq, err := newPGSequencer(ctx, s.cfg, DefaultPushbackMaxOutstanding)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create PostgreSQL sequencer: %v", err)
	}
	s.track(seq)
	// Unlike upstream (where these are nil, so LogReader.IntegratedSize/NextIndex panic).
	logStore.integratedSize = func(ctx context.Context) (uint64, error) {
		s, _, err := seq.currentTree(ctx)
		return s, err
	}
	logStore.nextIndex = seq.nextIndex
	m := &MigrationStorage{
		s:            s,
		dbPool:       seq.dbPool,
		q:            seq.q,
		bundleHasher: opts.LeafHasher(),
		sequencer:    seq,
		logStore:     logStore,
	}

	return m, logStore, nil
}

// MigrationStorage implements the tessera.MigrationStorage lifecycle contract.
type MigrationStorage struct {
	s            *Storage
	dbPool       *sql.DB
	q            *pgtx.Qualifier
	bundleHasher func([]byte) ([][]byte, error)
	sequencer    sequencer
	logStore     *logResourceStore
}

var _ migrate.MigrationWriter = &MigrationStorage{}

func (m *MigrationStorage) AwaitIntegration(ctx context.Context, sourceSize uint64) ([]byte, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			from, _, err := m.sequencer.currentTree(ctx)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "readTreeState", slog.Any("error", err))
				continue
			}
			slog.InfoContext(ctx, "Integrating", slog.Uint64("from", from), slog.Uint64("target", sourceSize))
			newSize, newRoot, err := m.buildTree(ctx, sourceSize)
			if err != nil {
				slog.WarnContext(ctx, "integrate", slog.Any("error", err))
			}
			if newSize == sourceSize {
				slog.InfoContext(ctx, "Integration complete", slog.Uint64("size", newSize), slog.String("root", fmt.Sprintf("%x", newRoot)))
				return newRoot, nil
			}
		}
	}
}

func (m *MigrationStorage) SetEntryBundle(ctx context.Context, index uint64, partial uint8, bundle []byte) error {
	return m.logStore.setEntryBundle(ctx, index, partial, bundle)
}

func (m *MigrationStorage) IntegratedSize(ctx context.Context) (uint64, error) {
	sz, _, err := m.sequencer.currentTree(ctx)
	return sz, err
}

// fetchLeafHashes returns the leaf hashes for the entries in the range [from, to).
func (m *MigrationStorage) fetchLeafHashes(ctx context.Context, from, to, sourceSize uint64) ([][]byte, error) {
	// TODO(al): Make this configurable.
	const maxBundles = 300

	toBeAdded := sync.Map{}
	eg := errgroup.Group{}
	n := 0
	for ri := range layout.Range(from, to-from, sourceSize) {
		eg.Go(func() error {
			b, err := m.logStore.getEntryBundle(ctx, ri.Index, ri.Partial)
			if err != nil {
				return fmt.Errorf("getEntryBundle(%d.%d): %v", ri.Index, ri.Partial, err)
			}

			bh, err := m.bundleHasher(b)
			if err != nil {
				return fmt.Errorf("bundleHasherFunc for bundle index %d: %v", ri.Index, err)
			}
			if l := len(bh); l < int(ri.First+ri.N) {
				return fmt.Errorf("bundle index %d has fewer entries than expected (%d < %d)", ri.Index, l, ri.First+ri.N)
			}
			toBeAdded.Store(ri.Index, bh[ri.First:ri.First+ri.N])
			return nil
		})
		n++
		if n >= maxBundles {
			break
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	lh := make([][]byte, 0, maxBundles*layout.EntryBundleWidth)
	for i := from / layout.EntryBundleWidth; ; i++ {
		v, ok := toBeAdded.LoadAndDelete(i)
		if !ok {
			break
		}
		bh := v.([][]byte)
		lh = append(lh, bh...)
	}

	return lh, nil
}

func (m *MigrationStorage) buildTree(ctx context.Context, sourceSize uint64) (uint64, []byte, error) {
	var newSize uint64
	var newRoot []byte

	tx, err := pgtx.Begin(ctx, m.dbPool)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to begin Tx: %v", err)
	}
	defer func() {
		if tx != nil {
			pgtx.Rollback(ctx, tx)
		}
	}()

	// Figure out which is the starting index of sequenced entries to start consuming from.
	row := tx.QueryRowContext(ctx, m.q.SQL("SELECT seq, rootHash FROM IntCoord WHERE id = $1 FOR UPDATE"), 0)
	var from uint64
	var rootHash []byte
	if err := row.Scan(&from, &rootHash); err != nil {
		return 0, nil, fmt.Errorf("failed to read IntCoord: %v", err)
	}

	slog.DebugContext(ctx, "Integrating", slog.Uint64("from", from))

	lh, err := m.fetchLeafHashes(ctx, from, sourceSize, sourceSize)
	if err != nil {
		return 0, nil, fmt.Errorf("fetchLeafHashes(%d, %d, %d): %v", from, sourceSize, sourceSize, err)
	}

	if len(lh) == 0 {
		slog.InfoContext(ctx, "Integrate: nothing to do, nothing done")
		return from, rootHash, nil
	}

	added := uint64(len(lh))
	slog.InfoContext(ctx, "Integrate: adding entries to existing tree size", slog.Int("count", len(lh)), slog.Uint64("from", from))
	newRoot, err = integrate(ctx, from, lh, m.logStore)
	if err != nil {
		slog.WarnContext(ctx, "integrate failed", slog.Any("error", err))
		return 0, nil, fmt.Errorf("integrate failed: %v", err)
	}
	newSize = from + added
	slog.InfoContext(ctx, "Integrate: added entries", slog.Uint64("count", added))

	if _, err := tx.ExecContext(ctx, m.q.SQL("UPDATE IntCoord SET seq=$1, rootHash=$2 WHERE id=$3"), newSize, newRoot, 0); err != nil {
		return 0, nil, fmt.Errorf("update intcoord: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("failed to commit Tx: %v", err)
	}
	tx = nil
	return newSize, newRoot, nil
}

// logResourceStore knows how to read and write entries which represent a tiles log inside an objStore.
type logResourceStore struct {
	objStore       objStore
	entriesPath    func(uint64, uint8) string
	integratedSize func(context.Context) (uint64, error)
	nextIndex      func(context.Context) (uint64, error)
}

func (lr *logResourceStore) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	r, err := lr.get(ctx, layout.CheckpointPath)
	if err != nil {
		var nske *types.NoSuchKey
		if errors.As(err, &nske) {
			return r, os.ErrNotExist
		}
	}
	return r, err
}

func (lr *logResourceStore) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return lr.get(ctx, layout.TilePath(l, i, p))
	})
}

func (lr *logResourceStore) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return lr.get(ctx, lr.entriesPath(i, p))
	})
}

func (lr *logResourceStore) IntegratedSize(ctx context.Context) (uint64, error) {
	return lr.integratedSize(ctx)
}

func (lr *logResourceStore) NextIndex(ctx context.Context) (uint64, error) {
	return lr.nextIndex(ctx)
}

// get returns the requested object.
//
// This is indended to be used to proxy read requests through the personality for debug/testing purposes.
func (s *logResourceStore) get(ctx context.Context, path string) ([]byte, error) {
	d, err := s.objStore.getObject(ctx, path)
	// Unlike upstream, map a missing object to os.ErrNotExist so that the partial -> full resource
	// fallback in ReadTile/ReadEntryBundle (fetcher.PartialOrFullResource) works.
	var nske *types.NoSuchKey
	if errors.As(err, &nske) {
		return d, fmt.Errorf("%w: %w", os.ErrNotExist, err)
	}
	return d, err
}

// readCheckpointWithETag returns the published checkpoint and its ETag, or (nil, "", nil) if there is none.
func (lrs *logResourceStore) readCheckpointWithETag(ctx context.Context) ([]byte, string, error) {
	raw, etag, err := lrs.objStore.getObjectWithETag(ctx, layout.CheckpointPath)
	if err != nil {
		var nske *types.NoSuchKey
		if errors.As(err, &nske) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("failed to read published checkpoint: %v", err)
	}
	return raw, etag, nil
}

// setCheckpointIfETag writes the checkpoint only if the published checkpoint still has the given
// ETag (or, if etag is empty, if there is no checkpoint yet).
func (lrs *logResourceStore) setCheckpointIfETag(ctx context.Context, cpRaw []byte, etag string) error {
	return lrs.objStore.setObjectIfETag(ctx, layout.CheckpointPath, cpRaw, ckptContType, ckptCacheControl, etag)
}

// setTile idempotently stores the provided tile at the location implied by the given level, index, and treeSize.
//
// The location to which the tile is written is defined by the tile layout spec.
func (lrs *logResourceStore) setTile(ctx context.Context, level, index, logSize uint64, tile *api.HashTile) error {
	start := time.Now()
	data, err := tile.MarshalText()
	if err != nil {
		return err
	}
	tPath := layout.TilePath(level, index, layout.PartialTileSize(level, index, logSize))
	slog.DebugContext(ctx, "StoreTile", slog.String("tpath", tPath), slog.Int("count", len(tile.Nodes)))

	err = lrs.objStore.setObjectIfNoneMatch(ctx, tPath, data, logContType, logCacheControl)
	opsHistogram.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(opNameKey.String("writeTile")))
	return err
}

// getTiles returns the tiles with the given tile-coords for the specified log size.
//
// Tiles are returned in the same order as they're requested, nils represent tiles which were not found.
func (lrs *logResourceStore) getTiles(ctx context.Context, tileIDs []storage.TileID, logSize uint64) ([]*api.HashTile, error) {
	r := make([]*api.HashTile, len(tileIDs))
	errG := errgroup.Group{}
	for i, id := range tileIDs {
		i := i
		id := id
		errG.Go(func() error {
			objName := layout.TilePath(id.Level, id.Index, layout.PartialTileSize(id.Level, id.Index, logSize))
			data, err := lrs.objStore.getObject(ctx, objName)
			if err != nil {
				// Do not use errors.Is. Keep errors.As to compare by type and not by value.
				var nske *types.NoSuchKey
				if errors.As(err, &nske) {
					// Depending on context, this may be ok.
					// We'll signal to higher levels that it wasn't found by retuning a nil for this tile.
					return nil
				}
				return err
			}
			t := &api.HashTile{}
			if err := t.UnmarshalText(data); err != nil {
				return fmt.Errorf("unmarshal(%q): %v", objName, err)
			}
			r[i] = t
			return nil
		})
	}
	if err := errG.Wait(); err != nil {
		return nil, err
	}
	return r, nil

}

// getEntryBundle returns the serialised entry bundle at the location implied by the given index and treeSize.
//
// Returns a wrapped os.ErrNotExist if the bundle does not exist.
func (lrs *logResourceStore) getEntryBundle(ctx context.Context, bundleIndex uint64, p uint8) ([]byte, error) {
	objName := lrs.entriesPath(bundleIndex, p)
	data, err := lrs.objStore.getObject(ctx, objName)
	if err != nil {
		// Do not use errors.Is. Keep errors.As to compare by type and not by value.
		var nske *types.NoSuchKey
		if errors.As(err, &nske) {
			// Return the generic NotExist error so that higher levels can differentiate
			// between this and other errors.
			return nil, fmt.Errorf("%v: %w", objName, os.ErrNotExist)
		}
		return nil, err
	}

	return data, nil
}

// setEntryBundle idempotently stores the serialised entry bundle at the location implied by the bundleIndex and treeSize.
func (lrs *logResourceStore) setEntryBundle(ctx context.Context, bundleIndex uint64, p uint8, bundleRaw []byte) error {
	objName := lrs.entriesPath(bundleIndex, p)
	// Note that setObject does an idempotent interpretation of IfNoneMatch - it only
	// returns an error if the named object exists _and_ contains different data to what's
	// passed in here.
	if err := lrs.objStore.setObjectIfNoneMatch(ctx, objName, bundleRaw, logContType, logCacheControl); err != nil {
		return fmt.Errorf("setObjectIfNoneMatch(%q): %v", objName, err)

	}
	return nil
}

// integrate adds the provided leaf hashes to the merkle tree, starting at the provided location.
func integrate(ctx context.Context, fromSeq uint64, lh [][]byte, lrs *logResourceStore) ([]byte, error) {
	getTiles := func(ctx context.Context, tileIDs []storage.TileID, treeSize uint64) ([]*api.HashTile, error) {
		n, err := lrs.getTiles(ctx, tileIDs, treeSize)
		if err != nil {
			return nil, fmt.Errorf("getTiles: %w", err)
		}
		return n, nil
	}

	newSize, newRoot, tiles, err := storage.Integrate(ctx, getTiles, fromSeq, lh)
	if err != nil {
		return nil, fmt.Errorf("storage.Integrate: %v", err)
	}
	errG := errgroup.Group{}
	for k, v := range tiles {
		func(ctx context.Context, k storage.TileID, v *api.HashTile) {
			errG.Go(func() error {
				return lrs.setTile(ctx, uint64(k.Level), k.Index, newSize, v)
			})
		}(ctx, k, v)
	}
	if err := errG.Wait(); err != nil {
		return nil, err
	}
	slog.DebugContext(ctx, "New tree", slog.Uint64("size", newSize), slog.String("root", fmt.Sprintf("%x", newRoot)))
	return newRoot, nil
}

// s3Storage knows how to store and retrieve objects from S3.
type s3Storage struct {
	bucket       string
	bucketPrefix string
	s3Client     *s3.Client
}

// key returns the bucket key for the named object, taking the bucket prefix into account.
//
// Unlike filepath.Join, a trailing slash is preserved so that prefixes (e.g. "x.p/") are not widened.
func (s *s3Storage) key(obj string) string {
	if s.bucketPrefix == "" {
		return obj
	}
	k := path.Join(s.bucketPrefix, obj)
	if strings.HasSuffix(obj, "/") && !strings.HasSuffix(k, "/") {
		k += "/"
	}
	return k
}

// getObject returns the data of the specified object, or an error.
func (s *s3Storage) getObject(ctx context.Context, obj string) ([]byte, error) {
	return s.getKey(ctx, s.key(obj))
}

// getKey fetches the object stored under the provided (already prefixed) bucket key.
func (s *s3Storage) getKey(ctx context.Context, obj string) ([]byte, error) {
	d, _, err := s.getKeyWithETag(ctx, obj)
	return d, err
}

// getKeyWithETag fetches the object stored under the provided (already prefixed) bucket key, and its ETag.
func (s *s3Storage) getKeyWithETag(ctx context.Context, obj string) ([]byte, string, error) {
	r, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(obj),
	})
	if err != nil {
		return nil, "", fmt.Errorf("getObject: failed to create reader for object %q in bucket %q: %w", obj, s.bucket, err)
	}

	d, err := io.ReadAll(r.Body)
	if err != nil {
		_ = r.Body.Close()
		return nil, "", fmt.Errorf("getObject: failed to read %q: %v", obj, err)
	}
	return d, aws.ToString(r.ETag), r.Body.Close()
}

// getObjectWithETag returns the data and ETag of the specified object, or an error.
func (s *s3Storage) getObjectWithETag(ctx context.Context, obj string) ([]byte, string, error) {
	return s.getKeyWithETag(ctx, s.key(obj))
}

// setObject stores the provided data in the specified object.
func (s *s3Storage) setObject(ctx context.Context, objName string, data []byte, contType string, cacheControl string) error {
	objName = s.key(objName)

	put := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(objName),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contType),
		CacheControl: aws.String(cacheControl),
	}

	if _, err := s.s3Client.PutObject(ctx, put); err != nil {
		return fmt.Errorf("failed to write object %q to bucket %q: %w", objName, s.bucket, err)
	}
	return nil
}

// isPreconditionFailure returns true if err is S3's answer to a failed conditional write.
// AWS S3 answers 409 ConditionalRequestConflict when conflicting conditional writes race.
func isPreconditionFailure(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "ConditionalRequestConflict")
}

// isAPIError returns true if err carries an S3 error response (i.e. the request's outcome is known).
func isAPIError(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr)
}

// isNotFound returns true if err is S3's answer for a missing object.
func isNotFound(err error) bool {
	var nske *types.NoSuchKey
	if errors.As(err, &nske) {
		return true
	}
	// Outside GetObject, the SDK doesn't model NoSuchKey as a type.
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}

// setObjectIfNoneMatch stores data in the specified object gated by a IfNoneMatch condition.
//
// ifNoneMatch can be used to specify the IfNoneMatch preconditions for the write, i.e write
// iff no object exists under this key already. If an object already exists under the same key,
// an error will be returned *unless*  the currently stored data is bit-for-bit identical to the
// data to-be-written. This is intended to provide idempotentency for writes.
//
// Unlike upstream, the same check is made when the outcome of the write is unknown (no response,
// e.g. because MinIO rejects a large conditional PUT before reading its body and closes the
// connection), so that such writes don't fail an integration which has in fact succeeded.
func (s *s3Storage) setObjectIfNoneMatch(ctx context.Context, objName string, data []byte, contType string, cacheControl string) error {
	objName = s.key(objName)

	put := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(objName),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contType),
		CacheControl: aws.String(cacheControl),
		// "*" is the expected character for this condition
		IfNoneMatch: aws.String("*"),
	}

	if _, err := s.s3Client.PutObject(ctx, put); err != nil {
		precondition := isPreconditionFailure(err)
		if !precondition && isAPIError(err) {
			return fmt.Errorf("failed to write object %q to bucket %q: %w", objName, s.bucket, err)
		}

		// If we run into a precondition failure error (or don't know whether the write happened),
		// check that the object which exists contains the same content that we want to write.
		// If so, we can consider this write to be idempotently successful.
		// objName already includes the bucket prefix (upstream re-applies it here, which
		// breaks idempotent re-writes when BucketPrefix is set).
		existing, gErr := s.getKey(ctx, objName)
		if gErr != nil {
			if !precondition {
				// Unknown outcome, and we can't tell: report the original error.
				return fmt.Errorf("failed to write object %q to bucket %q: %w", objName, s.bucket, err)
			}
			return fmt.Errorf("failed to fetch existing content for %q: %v", objName, gErr)
		}
		if !bytes.Equal(existing, data) {
			slog.ErrorContext(ctx, "Resource non-idempotent writen", slog.String("objname", objName), slog.Int("existingsize", len(existing)), slog.Int("newsize", len(data)), slog.String("existingsha256", fmt.Sprintf("%x", sha256.Sum256(existing))), slog.String("newsha256", fmt.Sprintf("%x", sha256.Sum256(data))))
			return fmt.Errorf("precondition failed: resource content for %q differs from data to-be-written", objName)
		}

		slog.DebugContext(ctx, "setObjectIfNoneMatch: identical resource already exists. Continuing", slog.String("objname", objName))
		return nil
	}
	return nil
}

// setObjectIfETag stores data in the specified object only if the object's current ETag is etag
// or, if etag is empty, only if no object exists. It returns errPreconditionFailed otherwise.
//
// If the outcome of the write is unknown, the object is read back: if it now holds exactly data,
// the write is considered successful.
func (s *s3Storage) setObjectIfETag(ctx context.Context, objName string, data []byte, contType string, cacheControl string, etag string) error {
	objName = s.key(objName)

	put := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(objName),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contType),
		CacheControl: aws.String(cacheControl),
	}
	if etag == "" {
		put.IfNoneMatch = aws.String("*")
	} else {
		put.IfMatch = aws.String(etag)
	}

	_, err := s.s3Client.PutObject(ctx, put)
	if err == nil {
		return nil
	}
	if isPreconditionFailure(err) || !isAPIError(err) {
		// Our own earlier attempt (e.g. a retry by the SDK after a lost response) may be the
		// reason for the precondition failure; if the object holds exactly our data, we're done.
		if existing, gErr := s.getKey(ctx, objName); gErr == nil && bytes.Equal(existing, data) {
			return nil
		} else if gErr != nil && !isNotFound(gErr) && isPreconditionFailure(err) {
			return fmt.Errorf("%w: %q changed concurrently (and re-reading it failed: %v)", errPreconditionFailed, objName, gErr)
		}
		if isPreconditionFailure(err) {
			return fmt.Errorf("%w: %q changed concurrently", errPreconditionFailed, objName)
		}
	}
	if etag != "" && isNotFound(err) {
		// MinIO and moto answer If-Match on a missing object with 404 NoSuchKey.
		return fmt.Errorf("%w: %q was deleted concurrently", errPreconditionFailed, objName)
	}
	return fmt.Errorf("failed to write object %q to bucket %q: %w", objName, s.bucket, err)
}

// listObjects returns the names (without the bucket prefix) of all objects whose name starts with prefix.
func (s *s3Storage) listObjects(ctx context.Context, prefix string) ([]string, error) {
	full := s.key(prefix)
	trim := ""
	if s.bucketPrefix != "" {
		trim = strings.TrimSuffix(s.key(""), "/") + "/"
	}
	keys := []string{}
	p := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(full),
	})
	for p.HasMorePages() {
		l, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects with prefix %q: %v", full, err)
		}
		for _, o := range l.Contents {
			keys = append(keys, strings.TrimPrefix(aws.ToString(o.Key), trim))
		}
	}
	return keys, nil
}

// deleteObjectsWithPrefix removes any objects with the provided prefix from S3.
//
// Unlike upstream storage/aws, this lists all pages of results, does not issue a DeleteObjects
// request when there is nothing to delete (S3-compatible services reject an empty request with
// MalformedXML, which would stall garbage collection), and reports per-object delete failures.
func (s *s3Storage) deleteObjectsWithPrefix(ctx context.Context, objPrefix string) error {
	return otel.TraceErr(ctx, "tessera.storage.s3pg.deleteObject", tracer, func(ctx context.Context, span trace.Span) error {
		objPrefix = s.key(objPrefix)
		span.SetAttributes(objectPathKey.String(objPrefix))

		p := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(objPrefix),
		})
		for p.HasMorePages() {
			l, err := p.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to list objects with prefix %q: %v", objPrefix, err)
			}
			if len(l.Contents) == 0 {
				continue
			}
			di := &s3.DeleteObjectsInput{
				Bucket: aws.String(s.bucket),
				Delete: &types.Delete{
					Objects: make([]types.ObjectIdentifier, 0, len(l.Contents)),
					Quiet:   aws.Bool(true),
				},
			}
			for _, k := range l.Contents {
				slog.DebugContext(ctx, "Deleting object", slog.String("key", *k.Key))
				di.Delete.Objects = append(di.Delete.Objects, types.ObjectIdentifier{Key: k.Key})
			}
			out, err := s.s3Client.DeleteObjects(ctx, di)
			if err != nil {
				return fmt.Errorf("failed to delete objects: %v", err)
			}
			if len(out.Errors) > 0 {
				errs := make([]error, 0, len(out.Errors))
				for _, e := range out.Errors {
					errs = append(errs, fmt.Errorf("%s: %s", aws.ToString(e.Key), aws.ToString(e.Message)))
				}
				return fmt.Errorf("failed to delete %d object(s): %w", len(out.Errors), errors.Join(errs...))
			}
		}
		return nil
	})
}
