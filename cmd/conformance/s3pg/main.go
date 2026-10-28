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

// s3pg is a simple personality allowing to run conformance/compliance/performance tests and showing how to use
// the Tessera S3-compatible object storage + PostgreSQL storage implementation.
//
// It is a port of cmd/conformance/aws: the MySQL flags are replaced with PostgreSQL DSNs, and
// secrets may be passed via environment variables instead of flags (see envPGDSN etc. below).
// See README.md for the full list of flags and environment variables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"log/slog"

	aaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/s3pg"
	s3pg_as "github.com/transparency-dev/tessera/storage/s3pg/antispam"
	"golang.org/x/mod/sumdb/note"
)

// Environment variables which may be used instead of the corresponding flags, so that secrets
// don't need to appear in process arguments. A non-empty flag value always takes precedence.
const (
	envPGDSN         = "TESSERA_PG_DSN"
	envAntispamPGDSN = "TESSERA_ANTISPAM_PG_DSN"
	envS3AccessKey   = "TESSERA_S3_ACCESS_KEY"
	envS3Secret      = "TESSERA_S3_SECRET"
	envSigner        = "TESSERA_SIGNER"
)

// envAdditionalSigners may hold additional checkpoint signer keys, separated by newlines and/or
// commas (surrounding whitespace is trimmed, empty entries are ignored). Unlike the variables
// above, it does not act as a fallback: its keys are appended to any --additional_signer flags.
const envAdditionalSigners = "TESSERA_ADDITIONAL_SIGNERS"

var (
	bucket            = flag.String("bucket", "", "Bucket to use for storing log")
	bucketPrefix      = flag.String("bucket_prefix", "", "Optional prefix within the bucket under which all log resources are stored, e.g. to store several logs in one bucket")
	pgDSN             = flag.String("pg_dsn", "", "PostgreSQL DSN (URL or keyword/value) for the log coordination DB, e.g. postgres://user@host:5432/db?sslmode=verify-full (select the log's schema with --pg_schema, or else search_path). If unset, the "+envPGDSN+" environment variable is used. Standard PG* environment variables (e.g. PGPASSWORD, PGPASSFILE) are also honoured")
	pgMaxConns        = flag.Int("pg_max_conns", 0, "Maximum open connections per database pool (log and, if enabled, antispam), defaults to 0, i.e unlimited")
	pgMaxIdle         = flag.Int("pg_max_idle_conns", 2, "Maximum idle connections per database pool (log and, if enabled, antispam): 0 keeps the database/sql default (2), a negative value keeps no idle connections")
	pgSchema          = flag.String("pg_schema", "", "PostgreSQL schema holding the log's tables. If set, every statement names its tables fully instead of relying on search_path (recommended, and required behind PgBouncer)")
	skipSchemaInit    = flag.Bool("skip_schema_init", false, "If true, don't create the database tables at start-up (for the log and, if enabled, antispam); they must already have been created, e.g. by applying storage/s3pg/schema.sql with migration tooling. Allows the runtime DB role to have no DDL privileges")
	s3Endpoint        = flag.String("s3_endpoint", "", "Endpoint for custom non-AWS S3 service, e.g. http://minio:9000. When set, path-style addressing and static credentials are used")
	s3Region          = flag.String("s3_region", "us-east-1", "Region to use with --s3_endpoint")
	s3AccessKeyID     = flag.String("s3_access_key", "", "Access key ID for custom non-AWS S3 service. If unset, the "+envS3AccessKey+" environment variable is used")
	s3SecretAccessKey = flag.String("s3_secret", "", "Secret access key for custom non-AWS S3 service. If unset, the "+envS3Secret+" environment variable is used")

	listen            = flag.String("listen", ":2024", "Address:port to listen on")
	signer            = flag.String("signer", "", "Note signer to use to sign checkpoints. If unset, the "+envSigner+" environment variable is used")
	publishInterval   = flag.Duration("publish_interval", 3*time.Second, "How frequently to publish updated checkpoints")
	traceFraction     = flag.Float64("trace_fraction", 0, "Fraction of open-telemetry span traces to sample")
	slogLevel         = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
	logFormat         = flag.String("log_format", "text", "The format of the logs: text or json.")
	additionalSigners = []string{}

	antispamEnable = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable persistent antispam storage")
	antispamPGDSN  = flag.String("antispam_pg_dsn", "", "PostgreSQL DSN for the antispam DB. If unset, the "+envAntispamPGDSN+" environment variable is used")
	antispamSchema = flag.String("antispam_pg_schema", "", "PostgreSQL schema holding the antispam tables. If set, every statement names its tables fully instead of relying on search_path (recommended, and required behind PgBouncer). May be the same as --pg_schema")
)

func init() {
	flag.Func("additional_signer", "Additional note signer for checkpoints, may be specified multiple times. Keys in the "+envAdditionalSigners+" environment variable (newline- and/or comma-separated) are appended to these", func(s string) error {
		additionalSigners = append(additionalSigners, s)
		return nil
	})
}

func main() {
	flag.Parse()
	ctx := context.Background()
	var handler slog.Handler
	switch *logFormat {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})
	default:
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})
	}
	slog.SetDefault(slog.New(handler))

	shutdownOTel := initOTel(ctx, *traceFraction)
	defer shutdownOTel(ctx)
	s, a := signerFromFlags()

	// Create our Tessera storage backend:
	cfg := storageConfigFromFlags()
	driver, err := s3pg.New(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new S3+PostgreSQL storage", slog.Any("error", err))
		os.Exit(1)
	}
	var antispam tessera.Antispam
	var closeAntispam func() error
	// Persistent antispam is currently experimental.
	if *antispamEnable {
		asOpts := s3pg_as.AntispamOpts{
			MaxOpenConns:   *pgMaxConns,
			MaxIdleConns:   *pgMaxIdle,
			SkipSchemaInit: *skipSchemaInit,
			Schema:         *antispamSchema,
		}
		as, err := s3pg_as.NewAntispam(ctx, antispamDSNFromFlags(), asOpts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create new PostgreSQL antispam storage", slog.Any("error", err))
			os.Exit(1)
		}
		antispam, closeAntispam = as, as.Close
	}
	// appCtx scopes the appender's background work (integration, publication, garbage collection,
	// antispam following); it is cancelled after shutdown, before the database pools are closed.
	appCtx, cancelApp := context.WithCancel(ctx)
	defer cancelApp()
	appender, shutdown, reader, err := tessera.NewAppender(appCtx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s, a...).
		WithCheckpointInterval(*publishInterval).
		WithBatching(512, 300*time.Millisecond).
		WithPushback(10*4096).
		WithAntispam(tessera.DefaultAntispamInMemorySize, antispam))
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new appender", slog.Any("error", err))
		os.Exit(1)
	}

	// Expose a HTTP handler for the conformance test writes.
	// This should accept arbitrary bytes POSTed to /add, and return an ascii
	// decimal representation of the index assigned to the entry.
	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		idx, err := appender.Add(r.Context(), tessera.NewEntry(b))()
		if err != nil {
			if errors.Is(err, tessera.ErrPushback) {
				w.Header().Add("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		// Write out the assigned index
		_, _ = fmt.Fprintf(w, "%d", idx.Index)
	})

	// Expose a health check for orchestrators (e.g. the compose healthcheck): it succeeds
	// once the server is serving and can read the log's checkpoint from object storage, and
	// fails permanently if the coordination database is found to be behind the published log.
	http.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := driver.(*s3pg.Storage).CoordinationError(); err != nil {
			slog.ErrorContext(ctx, "healthz: coordination database is behind the published log; manual recovery required", slog.Any("error", err))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if _, err := reader.ReadCheckpoint(ctx); err != nil {
			slog.WarnContext(ctx, "healthz: failed to read checkpoint", slog.Any("error", err))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	server := &http.Server{
		Addr:              *listen,
		Handler:           http.DefaultServeMux,
		Protocols:         &protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// On SIGINT/SIGTERM (e.g. `docker stop`), stop accepting requests, let in-flight requests
	// finish, then wait for everything sequenced by this instance to be integrated and published.
	sigCtx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	serverStopped := make(chan struct{})
	go func() {
		defer close(serverStopped)
		<-sigCtx.Done()
		slog.InfoContext(ctx, "Received signal, shutting down")
		sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := server.Shutdown(sctx); err != nil {
			slog.WarnContext(ctx, "HTTP server shutdown", slog.Any("error", err))
		}
	}()

	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-serverStopped
		err = nil
	}
	exitCode := 0
	if cErr := driver.(*s3pg.Storage).CoordinationError(); cErr != nil {
		// Outstanding entries can never be integrated: don't wait for them.
		slog.ErrorContext(ctx, "Not waiting for outstanding entries: the coordination database is behind the published log", slog.Any("error", cErr))
		exitCode = 1
	} else if err := shutdown(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to cleanly shutdown appender", slog.Any("error", err))
		exitCode = 1
	}
	// The appender and log reader are no longer used: stop their background work, then release
	// the PostgreSQL pools opened by the antispam and log storage.
	cancelApp()
	if closeAntispam != nil {
		if err := closeAntispam(); err != nil {
			slog.WarnContext(ctx, "Failed to close antispam storage", slog.Any("error", err))
		}
	}
	if err := driver.(*s3pg.Storage).Close(); err != nil {
		slog.WarnContext(ctx, "Failed to close log storage", slog.Any("error", err))
	}
	if err != nil {
		slog.ErrorContext(ctx, "ListenAndServe", slog.Any("error", err))
		exitCode = 1
	}
	shutdownOTel(ctx)
	os.Exit(exitCode)
}

// flagOrEnv returns the flag value if non-empty, otherwise the value of the named environment variable.
func flagOrEnv(flagVal, envName string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envName)
}

// checkDSN exits if dsn can't be parsed, and, unless an explicit schema is configured, warns if
// it doesn't select a schema via search_path.
//
// The DSN itself is never logged as it may contain a password.
func checkDSN(ctx context.Context, what, dsn, schema string) {
	c, err := pgx.ParseConfig(dsn)
	if err != nil {
		// Don't log err: pgx redacts passwords from it on a best-effort basis only.
		slog.ErrorContext(ctx, "Failed to parse PostgreSQL DSN", slog.String("dsn", what))
		os.Exit(1)
	}
	if schema != "" {
		return
	}
	_, hasSearchPath := c.RuntimeParams["search_path"]
	if !hasSearchPath && !strings.Contains(c.RuntimeParams["options"], "search_path") {
		slog.WarnContext(ctx, "PostgreSQL DSN does not set search_path and no schema flag is set, so the role's default search_path applies (fine if set with ALTER ROLE ... SET search_path). Prefer --pg_schema / --antispam_pg_schema. Use one schema per log.", slog.String("dsn", what))
	}
}

// storageConfigFromFlags returns an s3pg.Config struct populated with values
// provided via flags (or the equivalent environment variables).
func storageConfigFromFlags() s3pg.Config {
	ctx := context.Background()
	if *bucket == "" {
		slog.ErrorContext(ctx, "--bucket must be set")
		os.Exit(1)
	}
	dsn := flagOrEnv(*pgDSN, envPGDSN)
	if dsn == "" {
		slog.ErrorContext(ctx, "--pg_dsn or "+envPGDSN+" must be set")
		os.Exit(1)
	}
	checkDSN(ctx, "log", dsn, *pgSchema)

	// Configure to use a non-AWS S3-compatible service, e.g. MinIO.
	var awsConfig *aaws.Config
	var s3Opts func(o *s3.Options)
	if *s3Endpoint != "" {
		accessKey := flagOrEnv(*s3AccessKeyID, envS3AccessKey)
		secret := flagOrEnv(*s3SecretAccessKey, envS3Secret)
		if accessKey == "" || secret == "" {
			slog.ErrorContext(ctx, "--s3_access_key (or "+envS3AccessKey+") and --s3_secret (or "+envS3Secret+") must be set when --s3_endpoint is set")
			os.Exit(1)
		}
		region := *s3Region
		s3Opts = func(o *s3.Options) {
			o.BaseEndpoint = aaws.String(*s3Endpoint)
			o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secret, "")
			o.Region = region
			o.UsePathStyle = true
		}

		awsConfig = &aaws.Config{
			Region: region,
		}
	}

	return s3pg.Config{
		Bucket:         *bucket,
		BucketPrefix:   *bucketPrefix,
		SDKConfig:      awsConfig,
		S3Options:      s3Opts,
		DSN:            dsn,
		MaxOpenConns:   *pgMaxConns,
		MaxIdleConns:   *pgMaxIdle,
		SkipSchemaInit: *skipSchemaInit,
		Schema:         *pgSchema,
	}
}

func antispamDSNFromFlags() string {
	ctx := context.Background()
	dsn := flagOrEnv(*antispamPGDSN, envAntispamPGDSN)
	if dsn == "" {
		slog.ErrorContext(ctx, "--antispam_pg_dsn or "+envAntispamPGDSN+" must be set when --antispam is enabled")
		os.Exit(1)
	}
	checkDSN(ctx, "antispam", dsn, *antispamSchema)
	return dsn
}

func signerFromFlags() (note.Signer, []note.Signer) {
	s, err := fnote.NewSigner(flagOrEnv(*signer, envSigner))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create new signer", slog.Any("error", err))
		os.Exit(1)
	}

	var a []note.Signer
	for i, as := range additionalSignerKeys(additionalSigners, os.Getenv(envAdditionalSigners)) {
		s, err := fnote.NewSigner(as)
		if err != nil {
			// Never log the key itself. Indices count --additional_signer flags first, then
			// the entries of envAdditionalSigners.
			slog.ErrorContext(context.Background(), "Failed to create additional signer", slog.Int("index", i), slog.Any("error", err))
			os.Exit(1)
		}
		a = append(a, s)
	}

	return s, a
}

// additionalSignerKeys returns the keys given with --additional_signer (flagKeys), followed by
// those in envVal, the value of envAdditionalSigners: a newline- and/or comma-separated list
// whose entries are whitespace-trimmed, and empty entries ignored.
func additionalSignerKeys(flagKeys []string, envVal string) []string {
	keys := append([]string{}, flagKeys...)
	for _, k := range strings.FieldsFunc(envVal, func(r rune) bool { return r == '\n' || r == ',' }) {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}
