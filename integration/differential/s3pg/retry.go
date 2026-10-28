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

package main

// Scenario retry: an integration whose coordination database transaction fails after the tiles
// and entry bundles have been written is retried, and both drivers must converge to identical
// buckets.
//
// Failures are injected with triggers on IntCoord, which make the integration's
// "UPDATE IntCoord" (MySQL and PostgreSQL) or its COMMIT (PostgreSQL only, via a deferred
// constraint trigger) fail. The number of injected failures is counted outside the failing
// transaction (a MyISAM table in MySQL, a sequence in PostgreSQL), so that a trigger can fail
// exactly once, and the harness can tell when a failure has happened.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/sync/errgroup"
)

// retryPlan is the workload of the retry scenario. Phase names select the failure injected.
func retryPlan() *plan {
	return newPlan([]phaseSpec{
		{"base", "tiny", 250, 1},         // -> 250: no failures
		{"fail-once", "tiny", 3, 1},      // -> 253: the IntCoord UPDATE fails exactly once
		{"fail-grow", "tiny", 7, 2},      // -> 267: fails (s3pg: at COMMIT) until a 2nd batch is queued; crosses 256
		{"fail-restart", "tiny", 300, 1}, // -> 567: fails, the appender is killed, a new one retries; crosses 512
		{"after", "tiny", 1, 3},          // -> 570: no failures
	})
}

var mysqlFaultDDL = []string{
	"CREATE TABLE DiffFailCtl (id INT PRIMARY KEY, armed INT NOT NULL, budget BIGINT NOT NULL, failures BIGINT NOT NULL) ENGINE=MyISAM",
	"INSERT INTO DiffFailCtl VALUES (0, 0, 0, 0)",
	// Changes to the (non-transactional) MyISAM table survive the failed statement's rollback.
	`CREATE TRIGGER DiffIntCoordFail BEFORE UPDATE ON IntCoord FOR EACH ROW
BEGIN
  DECLARE a, b, f BIGINT;
  SELECT armed, budget, failures INTO a, b, f FROM DiffFailCtl WHERE id = 0;
  IF NEW.seq > OLD.seq AND a = 1 AND f < b THEN
    UPDATE DiffFailCtl SET failures = failures + 1 WHERE id = 0;
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'differential: injected IntCoord update failure';
  END IF;
END`,
}

var pgFaultDDL = []string{
	"CREATE TABLE DiffFailCtl (id INT PRIMARY KEY, armed INT NOT NULL, budget BIGINT NOT NULL, mode TEXT NOT NULL)",
	"INSERT INTO DiffFailCtl VALUES (0, 0, 0, 'update')",
	// nextval is not rolled back with the failing transaction.
	"CREATE SEQUENCE DiffFailCount MINVALUE 1 START 1",
	// SET search_path FROM CURRENT: the inspection handle's search_path is the log's schema, whereas
	// the driver's sessions may use a different one (--pg_schema_mode=qualified).
	`CREATE FUNCTION DiffFailFn() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE c DiffFailCtl%ROWTYPE; n BIGINT;
BEGIN
  SELECT * INTO c FROM DiffFailCtl WHERE id = 0;
  SELECT CASE WHEN is_called THEN last_value ELSE 0 END INTO n FROM DiffFailCount;
  IF NEW.seq > OLD.seq AND c.armed = 1 AND c.mode = TG_ARGV[0] AND n < c.budget THEN
    PERFORM nextval('DiffFailCount');
    RAISE EXCEPTION 'differential: injected IntCoord % failure', TG_ARGV[0];
  END IF;
  RETURN NEW;
END $$`,
	"CREATE TRIGGER DiffIntCoordFailUpdate BEFORE UPDATE ON IntCoord FOR EACH ROW EXECUTE FUNCTION DiffFailFn('update')",
	"CREATE CONSTRAINT TRIGGER DiffIntCoordFailCommit AFTER UPDATE ON IntCoord DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION DiffFailFn('commit')",
}

// installFaults creates the failure-injecting triggers in a side's coordination database.
func installFaults(ctx context.Context, s *side) error {
	ddl := mysqlFaultDDL
	if s.name == nameB {
		ddl = pgFaultDDL
	}
	for _, q := range ddl {
		if _, err := s.insp.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%s: %s: %v", s.name, strings.SplitN(q, "\n", 2)[0], err)
		}
	}
	return nil
}

// failCount returns the number of failures injected so far.
func failCount(ctx context.Context, s *side) (int64, error) {
	q := "SELECT failures FROM DiffFailCtl WHERE id = 0"
	if s.name == nameB {
		q = "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM DiffFailCount"
	}
	var n int64
	err := s.insp.QueryRowContext(ctx, q).Scan(&n)
	return n, err
}

// arm makes integrations fail until failCount reaches budget, or until disarm. pgMode selects
// whether s3pg fails at the IntCoord UPDATE ("update") or at COMMIT ("commit"); upstream always
// fails at the UPDATE.
func arm(ctx context.Context, s *side, budget int64, pgMode string) error {
	var err error
	if s.name == nameB {
		_, err = s.insp.ExecContext(ctx, "UPDATE DiffFailCtl SET armed = 1, budget = $1, mode = $2 WHERE id = 0", budget, pgMode)
	} else {
		_, err = s.insp.ExecContext(ctx, "UPDATE DiffFailCtl SET armed = 1, budget = ? WHERE id = 0", budget)
	}
	return err
}

func disarm(ctx context.Context, s *side) error {
	_, err := s.insp.ExecContext(ctx, "UPDATE DiffFailCtl SET armed = 0 WHERE id = 0")
	return err
}

// awaitFailures waits until at least n failures have been injected in total.
func awaitFailures(ctx context.Context, s *side, n int64) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		got, err := failCount(ctx, s)
		if err == nil && got >= n {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %d injected failures (have %d, err %v): %v", n, got, err, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// runFaultPhase runs phase pi of the retry plan on one side, injecting the failure the phase is
// named after.
func runFaultPhase(ctx context.Context, s *side, p *plan, pi int) error {
	ph := p.phases[pi]
	from := p.starts[pi]
	end := from + uint64(ph.batch*ph.steps)
	base, err := failCount(ctx, s)
	if err != nil {
		return err
	}
	in, err := startInstance(ctx, s, ph.batch, 0)
	if err != nil {
		return err
	}
	defer in.kill()
	switch ph.name {
	case "fail-once":
		// The first integration attempt fails; the next one (same entries) succeeds.
		if err := arm(ctx, s, base+1, "update"); err != nil {
			return err
		}
		if err := in.add(p, from, ph.batch); err != nil {
			return err
		}
	case "fail-grow":
		// Attempts covering only the first batch fail (s3pg: at COMMIT) and leave its partial
		// tiles/bundles behind; the successful retry integrates both batches at once.
		if err := arm(ctx, s, math.MaxInt32, "commit"); err != nil {
			return err
		}
		if err := in.add(p, from, ph.batch); err != nil {
			return err
		}
		if err := awaitFailures(ctx, s, base+1); err != nil {
			return err
		}
		if err := in.add(p, from+uint64(ph.batch), ph.batch); err != nil {
			return err
		}
		if err := disarm(ctx, s); err != nil {
			return err
		}
	case "fail-restart":
		// Attempts fail until the appender "crashes"; a new appender performs the retry.
		if err := arm(ctx, s, math.MaxInt32, "update"); err != nil {
			return err
		}
		if err := in.add(p, from, ph.batch); err != nil {
			return err
		}
		if err := awaitFailures(ctx, s, base+1); err != nil {
			return err
		}
		in.kill()
		if err := disarm(ctx, s); err != nil {
			return err
		}
		if in, err = startInstance(ctx, s, ph.batch, 0); err != nil {
			return err
		}
		defer in.kill()
	default:
		return fmt.Errorf("unknown fault phase %q", ph.name)
	}
	if err := awaitIntegrated(in.ctx, in.lr, end); err != nil {
		return err
	}
	if err := disarm(ctx, s); err != nil {
		return err
	}
	return in.stop(ctx, end)
}

// bothSides runs f for both sides concurrently.
func bothSides(a, b *side, f func(s *side) error) error {
	eg := errgroup.Group{}
	for _, s := range []*side{a, b} {
		eg.Go(func() error {
			if err := f(s); err != nil {
				return fmt.Errorf("%s: %v", s.name, err)
			}
			return nil
		})
	}
	return eg.Wait()
}

// scenarioRetry runs the retry plan on a new pair of logs, comparing them after every phase.
func scenarioRetry(ctx context.Context, r *resources) error {
	report("scenario retry: integration retried after the coordination DB failed to commit it (tiles/bundles already written)")
	prefixA := *bucketPrefix
	if prefixA != "" {
		if err := retryPrefixBug(ctx, r); err != nil {
			return err
		}
		prefixA = ""
		report("  the %s log of the following comparison uses no bucket prefix (see above); keys are compared relative to the prefix", nameA)
	}
	a, b, err := newLogPairPrefix(ctx, r, "rty", prefixA, *bucketPrefix)
	if err != nil {
		return err
	}
	p := retryPlan()
	for pi, ph := range p.phases {
		start := time.Now()
		var before [2]int64
		if ph.name == "fail-once" { // First failure phase.
			if err := bothSides(a, b, func(s *side) error { return installFaults(ctx, s) }); err != nil {
				return err
			}
		}
		faulty := strings.HasPrefix(ph.name, "fail-")
		if faulty {
			for i, s := range []*side{a, b} {
				if before[i], err = failCount(ctx, s); err != nil {
					return err
				}
			}
		}
		if err := bothSides(a, b, func(s *side) error {
			if faulty {
				return runFaultPhase(ctx, s, p, pi)
			}
			return runPhase(ctx, s, p, pi, 0)
		}); err != nil {
			return fmt.Errorf("retry phase %s: %v", ph.name, err)
		}
		size := p.starts[pi] + uint64(ph.batch*ph.steps)
		report("phase %-12s batch=%-5d steps=%-3d size=%-6d (%v)", ph.name, ph.batch, ph.steps, size, time.Since(start).Round(time.Millisecond))
		if faulty {
			var n [2]int64
			for i, s := range []*side{a, b} {
				after, err := failCount(ctx, s)
				if err != nil {
					return err
				}
				n[i] = after - before[i]
			}
			at := "UPDATE"
			if ph.name == "fail-grow" {
				at = "COMMIT"
			}
			if ph.name == "fail-once" {
				check(n[0] == 1 && n[1] == 1, "injected failures: %s=%d (at UPDATE) %s=%d (at %s), want exactly 1 each", nameA, n[0], nameB, n[1], at)
			} else {
				check(n[0] >= 1 && n[1] >= 1, "injected failures: %s=%d (at UPDATE) %s=%d (at %s), want >= 1 each", nameA, n[0], nameB, n[1], at)
			}
		}
		if err := compareLogs(ctx, a, b, size); err != nil {
			return err
		}
		if ph.name == "fail-grow" {
			// The failed attempts integrated the first batch only, writing partial resources which no
			// committed tree size refers to.
			s1 := p.starts[pi] + uint64(ph.batch)
			orphans := []string{
				layout.EntriesPath(s1/layout.EntryBundleWidth, uint8(s1%layout.EntryBundleWidth)),
				layout.TilePath(0, s1/layout.TileWidth, uint8(s1%layout.TileWidth)),
			}
			var present []string
			for _, sd := range []*side{a, b} {
				snap, err := sd.br.snapshot(ctx)
				if err != nil {
					return err
				}
				for _, k := range orphans {
					if _, ok := snap[k]; ok {
						present = append(present, sd.name+":"+k)
					}
				}
			}
			check(len(present) == 2*len(orphans), "partial resources of the failed attempts (size %d) left in both buckets: %v", s1, present)
		}
	}
	return nil
}

// retryPrefixBug shows that upstream cannot retry an integration whose DB transaction failed when
// BucketPrefix is set: every re-write of an existing object fails (see README). s3pg must retry
// successfully.
func retryPrefixBug(ctx context.Context, r *resources) error {
	a, b, err := newLogPair(ctx, r, "rtyp")
	if err != nil {
		return err
	}
	p := newPlan([]phaseSpec{{"base", "tiny", 5, 1}, {"fail-once", "tiny", 3, 1}})
	if err := bothSides(a, b, func(s *side) error { return runPhase(ctx, s, p, 0, 0) }); err != nil {
		return err
	}
	if err := bothSides(a, b, func(s *side) error { return installFaults(ctx, s) }); err != nil {
		return err
	}
	const wait = 15 * time.Second
	var aSize uint64
	var aFails int64
	errB := runFaultPhase(ctx, b, p, 1)
	errA := func() error {
		if err := arm(ctx, a, 1, "update"); err != nil {
			return err
		}
		in, err := startInstance(ctx, a, 3, 0)
		if err != nil {
			return err
		}
		defer in.kill()
		if err := in.add(p, 5, 3); err != nil {
			return err
		}
		time.Sleep(wait)
		if aSize, err = in.lr.IntegratedSize(ctx); err != nil {
			return err
		}
		aFails, err = failCount(ctx, a)
		return err
	}()
	if err := errors.Join(errA, disarm(ctx, a)); err != nil {
		return err
	}
	upstreamErr := recordedError(a.bucket, "failed to fetch existing content")
	if errB == nil && aSize == 5 && aFails == 1 && upstreamErr != "" {
		known("BucketPrefix=%q: after one injected IntCoord UPDATE failure, %s never integrates the retry (size still %d after %v, no further "+
			"injected failures; its retries fail with: %.300s): upstream setObjectIfNoneMatch re-applies the bucket prefix when reading back an "+
			"existing object (same bug as the migration re-run); %s retried successfully",
			*bucketPrefix, nameA, aSize, wait, upstreamErr, nameB)
		return nil
	}
	check(errB == nil && aSize == 8, "BucketPrefix=%q, one injected failure: %s integrated size %d (injected failures %d, last error %q); %s: %v",
		*bucketPrefix, nameA, aSize, aFails, upstreamErr, nameB, errB)
	return nil
}

// errorRecorder is a slog.Handler which records the error-level log lines it passes on, so that
// errors logged by the drivers' background jobs can be inspected.
type errorRecorder struct {
	slog.Handler
}

var (
	recMu   sync.Mutex
	recErrs []string
)

func (h *errorRecorder) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Level >= slog.LevelError {
		var b strings.Builder
		b.WriteString(rec.Message)
		rec.Attrs(func(a slog.Attr) bool {
			fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
			return true
		})
		recMu.Lock()
		recErrs = append(recErrs, b.String())
		if len(recErrs) > 10000 {
			recErrs = recErrs[5000:]
		}
		recMu.Unlock()
	}
	return h.Handler.Handle(ctx, rec)
}

func (h *errorRecorder) WithAttrs(as []slog.Attr) slog.Handler {
	return &errorRecorder{h.Handler.WithAttrs(as)}
}

func (h *errorRecorder) WithGroup(name string) slog.Handler {
	return &errorRecorder{h.Handler.WithGroup(name)}
}

// recordedError returns the most recent recorded error which contains all of substrs, or "".
func recordedError(substrs ...string) string {
	recMu.Lock()
	defer recMu.Unlock()
	for i := len(recErrs) - 1; i >= 0; i-- {
		all := true
		for _, s := range substrs {
			all = all && strings.Contains(recErrs[i], s)
		}
		if all {
			return recErrs[i]
		}
	}
	return ""
}
