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

// dbcheck verifies the s3pg coordination-database invariants, reading PostgreSQL directly (never
// through a fault proxy) in one REPEATABLE READ snapshot:
//
//   - IntCoord.seq <= SeqCoord.next, PubCoord.size <= IntCoord.seq;
//   - the Seq rows are exactly a contiguous chain from IntCoord.seq to SeqCoord.next (each row's
//     key + the number of entries in its gob-encoded batch = the next row's key);
//   - with -checkpoint_url: the published checkpoint (read first) is not larger than IntCoord.seq,
//     i.e. the database is never behind the published log;
//
// and, with -settled, additionally that the log has settled consistently with object storage
// (checkpoint read directly from -checkpoint_url): Seq is empty, the published checkpoint's size
// equals IntCoord.seq and its root equals IntCoord.rootHash, and PubCoord.size equals it too.
//
// With -watch it checks repeatedly (until SIGTERM) and records every violation observed.
// Prints a JSON summary; exits 1 if any invariant was violated.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	dsn      = flag.String("dsn", "", "PostgreSQL DSN selecting the log's schema (search_path)")
	cpURL    = flag.String("checkpoint_url", "", "direct URL of the published checkpoint (without -settled: also check, reading it first, that it is not larger than IntCoord.seq)")
	settled  = flag.Bool("settled", false, "also check the settled-state invariants")
	watch    = flag.Duration("watch", 0, "if > 0, check repeatedly at this interval until SIGTERM")
	outFile  = flag.String("out", "", "write JSON summary here")
	eventLog = flag.String("events", "", "append observations/violations here")
)

// sequencedEntry mirrors storage/internal.SequencedEntry (gob matches fields by name).
type sequencedEntry struct {
	BundleData []byte
	LeafHash   []byte
}

type state struct {
	Next, IntSeq, PubSize, GCFrom uint64
	PublishedAt                   int64
	Root                          []byte
	SeqRows                       int
	SeqEntries                    uint64
	CPSize                        uint64
	CPRoot                        []byte
}

func readCheckpoint(ctx context.Context) (uint64, []byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, *cpURL, nil)
	resp, err := (&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return 0, nil, fmt.Errorf("checkpoint: HTTP %d", resp.StatusCode)
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) < 3 {
		return 0, nil, fmt.Errorf("short checkpoint")
	}
	size, err := strconv.ParseUint(lines[1], 10, 64)
	if err != nil {
		return 0, nil, err
	}
	root, err := base64.StdEncoding.DecodeString(lines[2])
	return size, root, err
}

func check(ctx context.Context, db *sql.DB, withSettled bool) (state, []string, error) {
	var st state
	var problems []string
	// Read the published checkpoint BEFORE the database: IntCoord.seq only grows and checkpoints are
	// only published for sizes IntCoord has reached, so in this order a healthy log always has
	// checkpoint size <= IntCoord.seq.
	var preSize uint64
	var preOK bool
	if *cpURL != "" && !withSettled {
		if size, _, err := readCheckpoint(ctx); err == nil {
			preSize, preOK = size, true
		}
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return st, nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.QueryRowContext(ctx, `SELECT s.next, i.seq, i.rootHash, p.size, p.publishedAt, g.fromSize
		FROM SeqCoord s, IntCoord i, PubCoord p, GCCoord g WHERE s.id=0 AND i.id=0 AND p.id=0 AND g.id=0`).
		Scan(&st.Next, &st.IntSeq, &st.Root, &st.PubSize, &st.PublishedAt, &st.GCFrom); err != nil {
		return st, nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT id, seq, v FROM Seq ORDER BY id, seq")
	if err != nil {
		return st, nil, err
	}
	expect := st.IntSeq
	for rows.Next() {
		var id int
		var seq uint64
		var v []byte
		if err := rows.Scan(&id, &seq, &v); err != nil {
			return st, nil, err
		}
		var b []sequencedEntry
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&b); err != nil {
			problems = append(problems, fmt.Sprintf("Seq row %d: undecodable batch: %v", seq, err))
			continue
		}
		if id != 0 {
			problems = append(problems, fmt.Sprintf("Seq row with id %d", id))
		}
		if seq != expect {
			problems = append(problems, fmt.Sprintf("Seq chain broken: expected key %d, found %d", expect, seq))
		}
		if len(b) == 0 {
			problems = append(problems, fmt.Sprintf("Seq row %d holds an empty batch", seq))
		}
		st.SeqRows++
		st.SeqEntries += uint64(len(b))
		expect = seq + uint64(len(b))
	}
	if err := rows.Err(); err != nil {
		return st, nil, err
	}
	_ = rows.Close()
	if expect != st.Next {
		problems = append(problems, fmt.Sprintf("Seq chain ends at %d but SeqCoord.next=%d (IntCoord.seq=%d, %d rows)", expect, st.Next, st.IntSeq, st.SeqRows))
	}
	if st.IntSeq > st.Next {
		problems = append(problems, fmt.Sprintf("IntCoord.seq %d > SeqCoord.next %d", st.IntSeq, st.Next))
	}
	if st.PubSize > st.IntSeq {
		problems = append(problems, fmt.Sprintf("PubCoord.size %d > IntCoord.seq %d", st.PubSize, st.IntSeq))
	}
	if preOK {
		st.CPSize = preSize
		if preSize > st.IntSeq {
			problems = append(problems, fmt.Sprintf("published checkpoint size %d > IntCoord.seq %d (database behind the published log)", preSize, st.IntSeq))
		}
	}
	if withSettled {
		size, root, err := readCheckpoint(ctx)
		if err != nil {
			problems = append(problems, fmt.Sprintf("settled: cannot read checkpoint: %v", err))
		} else {
			st.CPSize, st.CPRoot = size, root
			if st.SeqRows != 0 {
				problems = append(problems, fmt.Sprintf("settled: Seq not empty (%d rows)", st.SeqRows))
			}
			if size != st.IntSeq {
				problems = append(problems, fmt.Sprintf("settled: checkpoint size %d != IntCoord.seq %d", size, st.IntSeq))
			} else if !bytes.Equal(root, st.Root) {
				problems = append(problems, fmt.Sprintf("settled: checkpoint root %x != IntCoord.rootHash %x at size %d", root, st.Root, size))
			}
			if st.PubSize != size {
				problems = append(problems, fmt.Sprintf("settled: PubCoord.size %d != checkpoint size %d", st.PubSize, size))
			}
		}
	}
	return st, problems, nil
}

func main() {
	flag.Parse()
	ctx := context.Background()
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	db.SetMaxOpenConns(2)
	ev := io.Discard
	if *eventLog != "" {
		f, err := os.OpenFile(*eventLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			ev = f
		}
	}
	summary := map[string]any{}
	allProblems := map[string]int{}
	checks, errs := 0, 0
	var last state
	if *watch > 0 {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		t := time.NewTicker(*watch)
	loop:
		for {
			select {
			case <-sig:
				break loop
			case <-t.C:
			}
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			st, problems, err := check(cctx, db, false)
			cancel()
			if err != nil {
				errs++
				continue
			}
			checks++
			last = st
			for _, p := range problems {
				if allProblems[p] == 0 {
					_, _ = fmt.Fprintf(ev, "%s VIOLATION %s state=%+v\n", time.Now().UTC().Format(time.RFC3339Nano), p, st)
				}
				allProblems[p]++
			}
		}
	} else {
		st, problems, err := check(ctx, db, *settled)
		if err != nil {
			fmt.Fprintln(os.Stderr, "check failed:", err)
			os.Exit(2)
		}
		checks = 1
		last = st
		for _, p := range problems {
			allProblems[p]++
			_, _ = fmt.Fprintf(ev, "%s VIOLATION %s state=%+v\n", time.Now().UTC().Format(time.RFC3339Nano), p, st)
		}
	}
	summary["checks"] = checks
	summary["check_errors"] = errs
	summary["violations"] = allProblems
	summary["last_state"] = map[string]any{"seqcoord_next": last.Next, "intcoord_seq": last.IntSeq, "intcoord_root": fmt.Sprintf("%x", last.Root),
		"pubcoord_size": last.PubSize, "gccoord_from": last.GCFrom, "seq_rows": last.SeqRows, "seq_entries": last.SeqEntries,
		"checkpoint_size": last.CPSize, "checkpoint_root": fmt.Sprintf("%x", last.CPRoot)}
	j, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(j))
	if *outFile != "" {
		_ = os.WriteFile(*outFile, j, 0o644)
	}
	if len(allProblems) > 0 {
		os.Exit(1)
	}
}
