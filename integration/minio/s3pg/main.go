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

// s3probe checks the S3 semantics that the storage/s3pg driver relies on against a real
// S3-compatible server (e.g. MinIO). It creates (and removes) its own uniquely named buckets.
//
//	go run ./integration/minio/s3pg --endpoint=http://127.0.0.1:9100 --access_key=... --secret=...
//
// Probes:
//
//	inm-race   concurrent PutObject(If-None-Match: *) on one key: exactly one 2xx, all others
//	           412 PreconditionFailed, stored content == winner's (repeated, 1B..5MiB payloads)
//	inm-rewrite identical re-writes of an existing object (sizes 4KiB..5MiB), with and without
//	           SDK retries -> 412 expected every time
//	inm-same   If-None-Match on an existing key with identical content -> 412
//	del-empty  DeleteObjects with an empty object list -> error code recorded (driver skips it)
//	list-del   ListObjectsV2 pagination (small MaxKeys) while deleting each listed page -> no key skipped
//	raw        read-after-write / list-after-write / overwrite-then-read under concurrency
//	anon       anonymous GET through a bucket policy (existing key, missing key, without policy)
//	headers    Cache-Control / Content-Type preserved (authenticated and anonymous GET/HEAD)
//	ifmatch, ifmatch-race  conditional checkpoint overwrites with If-Match (see ifmatch.go)
//
// The exit code is non-zero if any probe fails. A probe that only finds "deviations" prints DEVI
// and does not fail unless --strict_412 is set: MinIO evaluates If-None-Match under its per-object
// lock *before* reading the request body and answers 412 without draining it, so for large
// bodies (MiB range) Go's HTTP server closes the connection and the client may see "connection
// reset by peer"/"broken pipe" instead of the 412. Exactly-once is unaffected; the driver resolves
// such unknown-outcome conditional writes by reading the object back.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var (
	endpoint    = flag.String("endpoint", "http://127.0.0.1:9100", "S3 endpoint")
	accessKey   = flag.String("access_key", "", "S3 access key")
	secret      = flag.String("secret", "", "S3 secret key")
	region      = flag.String("region", "us-east-1", "S3 region")
	probes      = flag.String("probes", "inm-race,inm-rewrite,inm-same,del-empty,list-del,raw,anon,headers,ifmatch,ifmatch-race", "comma separated probes to run")
	iterations  = flag.Int("iterations", 200, "inm-race: number of rounds")
	rewrites    = flag.Int("rewrites", 50, "inm-rewrite: identical re-writes per size")
	racers      = flag.Int("racers", 32, "inm-race: concurrent writers per round")
	listKeys    = flag.Int("list_keys", 1000, "list-del: number of keys")
	rawWorkers  = flag.Int("raw_workers", 32, "raw: concurrent workers")
	rawOps      = flag.Int("raw_ops", 200, "raw: operations per worker")
	keepBuckets = flag.Bool("keep_buckets", false, "do not delete buckets at the end")
	strict412   = flag.Bool("strict_412", false, "inm-race/inm-rewrite: treat a rejected conditional write that got no HTTP response (connection closed by the server while the body was still being sent) as a failure rather than a deviation")
	sdkAttempts = flag.Int("sdk_attempts", 1, "max SDK attempts per request for the main client (1 = no retries, so every server response is observed as-is; 0 = SDK default, as the driver uses)")
)

// The driver's object metadata (storage/s3pg/s3pg.go).
const (
	logContType      = "application/octet-stream"
	ckptContType     = "text/plain; charset=utf-8"
	logCacheControl  = "max-age=604800,immutable"
	ckptCacheControl = "no-cache"
)

// deviation is an error that does not violate an invariant the driver relies on.
type deviation struct{ msg string }

func (d deviation) Error() string { return d.msg }

type prober struct {
	c       *s3.Client
	buckets []string
	failed  bool
}

func main() {
	flag.Parse()
	ctx := context.Background()
	// Same client construction as the driver (cmd/conformance/s3pg), by default with SDK retries
	// disabled so that every server response is observed as-is.
	c := s3.NewFromConfig(aws.Config{Region: *region}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.Credentials = credentials.NewStaticCredentialsProvider(*accessKey, *secret, "")
		o.Region = *region
		o.UsePathStyle = true
		if *sdkAttempts > 0 {
			o.RetryMaxAttempts = *sdkAttempts
		}
	})
	p := &prober{c: c}
	defer p.cleanup(ctx)
	for _, name := range strings.Split(*probes, ",") {
		start := time.Now()
		var err error
		switch name {
		case "inm-race":
			err = p.inmRace(ctx)
		case "inm-rewrite":
			err = p.inmRewrite(ctx)
		case "inm-same":
			err = p.inmSame(ctx)
		case "del-empty":
			err = p.delEmpty(ctx)
		case "list-del":
			err = p.listDel(ctx)
		case "raw":
			err = p.readAfterWrite(ctx)
		case "anon":
			err = p.anon(ctx)
		case "headers":
			err = p.headers(ctx)
		case "ifmatch":
			err = p.ifMatch(ctx)
		case "ifmatch-race":
			err = p.ifMatchRace(ctx)
		default:
			err = fmt.Errorf("unknown probe")
		}
		var dev deviation
		if errors.As(err, &dev) && !*strict412 {
			fmt.Printf("DEVI %-9s (%v): %v\n", name, time.Since(start).Round(time.Millisecond), err)
		} else if err != nil {
			p.failed = true
			fmt.Printf("FAIL %-9s (%v): %v\n", name, time.Since(start).Round(time.Millisecond), err)
		} else {
			fmt.Printf("PASS %-9s (%v)\n", name, time.Since(start).Round(time.Millisecond))
		}
	}
	p.cleanup(ctx)
	if p.failed {
		os.Exit(1)
	}
}

func (p *prober) newBucket(ctx context.Context, tag string) (string, error) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	name := fmt.Sprintf("probe-%s-%x", tag, b)
	if _, err := p.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		return "", fmt.Errorf("CreateBucket(%s): %v", name, err)
	}
	p.buckets = append(p.buckets, name)
	return name, nil
}

func (p *prober) cleanup(ctx context.Context) {
	if *keepBuckets {
		return
	}
	for _, b := range p.buckets {
		pg := s3.NewListObjectsV2Paginator(p.c, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
		for pg.HasMorePages() {
			l, err := pg.NextPage(ctx)
			if err != nil {
				break
			}
			if len(l.Contents) == 0 {
				continue
			}
			d := &types.Delete{Quiet: aws.Bool(true)}
			for _, o := range l.Contents {
				d.Objects = append(d.Objects, types.ObjectIdentifier{Key: o.Key})
			}
			_, _ = p.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: d})
		}
		if _, err := p.c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b)}); err != nil {
			fmt.Printf("cleanup: DeleteBucket(%s): %v\n", b, err)
		}
	}
	p.buckets = nil
}

func errCode(err error) string {
	if err == nil {
		return "OK"
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return "non-API: " + err.Error()
}

func httpStatus(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func payload(size int, writer int) []byte {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	if size >= 8 {
		binary.BigEndian.PutUint64(b, uint64(writer))
	}
	return b
}

func (p *prober) put(ctx context.Context, bucket, key string, data []byte, inm bool) error {
	in := &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(logContType),
		CacheControl: aws.String(logCacheControl),
	}
	if inm {
		in.IfNoneMatch = aws.String("*")
	}
	_, err := p.c.PutObject(ctx, in)
	return err
}

func (p *prober) get(ctx context.Context, bucket, key string) ([]byte, error) {
	o, err := p.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = o.Body.Close() }()
	return io.ReadAll(o.Body)
}

// classify maps a PutObject result to "ok", "412", "transport" (no HTTP response: the server
// closed the connection while the request body was still being sent) or "other <status> <code>".
func classify(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errCode(err) == "PreconditionFailed":
		return "412"
	case httpStatus(err) == 0 && (strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "broken pipe")):
		return "transport"
	default:
		return fmt.Sprintf("other %d %s", httpStatus(err), errCode(err))
	}
}

// inmRace: N writers race to create the same key with If-None-Match: *.
func (p *prober) inmRace(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "inm")
	if err != nil {
		return err
	}
	sizes := []int{1, 13, 512, 4 << 10, 64 << 10, 256 << 10, 1 << 20, 2 << 20, 3<<20 + 12345, 5 << 20}
	bySize := map[int]map[string]int{}
	rewrite := map[int]map[string]int{}
	var bad []string
	var firstOther error
	for it := 0; it < *iterations; it++ {
		size := sizes[it%len(sizes)]
		if bySize[size] == nil {
			bySize[size] = map[string]int{}
			rewrite[size] = map[string]int{}
		}
		key := fmt.Sprintf("tile/0/x%03d/%d", it/1000, it%1000)
		data := make([][]byte, *racers)
		for w := range data {
			data[w] = payload(size, w)
		}
		res := make([]error, *racers)
		var wg sync.WaitGroup
		startGate := make(chan struct{})
		for w := 0; w < *racers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-startGate
				res[w] = p.put(ctx, bucket, key, data[w], true)
			}(w)
		}
		close(startGate)
		wg.Wait()
		win := -1
		nOK := 0
		for w, e := range res {
			c := classify(e)
			bySize[size][c]++
			if e == nil {
				nOK++
				win = w
			} else if strings.HasPrefix(c, "other") && firstOther == nil {
				firstOther = e
			}
		}
		if nOK != 1 {
			bad = append(bad, fmt.Sprintf("round %d (size %d): %d successes", it, size, nOK))
		}
		got, err := p.get(ctx, bucket, key)
		if err != nil {
			bad = append(bad, fmt.Sprintf("round %d: GET: %v", it, err))
			continue
		}
		if win >= 0 && !bytes.Equal(got, data[win]) {
			owner := -1
			for w := range data {
				if bytes.Equal(got, data[w]) {
					owner = w
				}
			}
			bad = append(bad, fmt.Sprintf("round %d (size %d): stored content is writer %d's, but the only success was writer %d", it, size, owner, win))
		}
		// Afterwards, a retry of the winner's own write (idempotent re-write) must also be 412.
		if win >= 0 {
			rewrite[size][classify(p.put(ctx, bucket, key, data[win], true))]++
		}
	}
	fmt.Printf("  inm-race: %d rounds x %d writers (max SDK attempts %d, 0=default)\n", *iterations, *racers, *sdkAttempts)
	nonPre, transport := 0, 0
	for _, size := range sizes {
		if bySize[size] == nil {
			continue
		}
		fmt.Printf("  inm-race: size %8d: racers %v | identical re-write by winner: %v\n", size, bySize[size], rewrite[size])
		for _, m := range []map[string]int{bySize[size], rewrite[size]} {
			for c, n := range m {
				if c == "transport" {
					transport += n
				} else if strings.HasPrefix(c, "other") {
					nonPre += n
				}
			}
		}
	}
	if firstOther != nil {
		fmt.Printf("  inm-race: first 'other' error: %v\n", firstOther)
	}
	if nonPre > 0 {
		bad = append(bad, fmt.Sprintf("%d rejected writes got an unexpected error (neither 412 nor a closed connection)", nonPre))
	}
	if len(bad) > 0 {
		if len(bad) > 20 {
			bad = append(bad[:20], fmt.Sprintf("... and %d more", len(bad)-20))
		}
		return errors.New(strings.Join(bad, "\n    "))
	}
	if transport > 0 {
		return deviation{fmt.Sprintf("exactly one winner and the winner's content in every round, but %d rejected writes got no 412 response (connection closed by the server)", transport)}
	}
	return nil
}

// inmRewrite: sequential identical re-writes of an existing object (what the driver does when
// integration is retried after a failure between the S3 writes and the database commit), with
// SDK retries disabled and with the SDK's default retryer (as the driver uses it).
func (p *prober) inmRewrite(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "rewrite")
	if err != nil {
		return err
	}
	retrying := s3.NewFromConfig(aws.Config{Region: *region}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.Credentials = credentials.NewStaticCredentialsProvider(*accessKey, *secret, "")
		o.Region = *region
		o.UsePathStyle = true
	})
	bad, transport := 0, 0
	for _, size := range []int{4 << 10, 64 << 10, 128 << 10, 192 << 10, 256 << 10, 320 << 10, 512 << 10, 1 << 20, 5 << 20} {
		key := fmt.Sprintf("tile/entries/%d", size)
		data := payload(size, 1)
		if err := p.put(ctx, bucket, key, data, true); err != nil {
			return err
		}
		res := [2]map[string]int{{}, {}}
		for i := 0; i < *rewrites; i++ {
			res[0][classify(p.put(ctx, bucket, key, data, true))]++
			_, err := retrying.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
				ContentType: aws.String(logContType), CacheControl: aws.String(logCacheControl), IfNoneMatch: aws.String("*")})
			res[1][classify(err)]++
		}
		fmt.Printf("  inm-rewrite: size %8d x %d: no retries %v | default SDK retryer %v\n", size, *rewrites, res[0], res[1])
		for _, r := range res {
			transport += r["transport"]
			bad += *rewrites - r["412"] - r["transport"]
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d identical re-writes got an unexpected result (neither 412 nor a closed connection)", bad)
	}
	if transport > 0 {
		return deviation{fmt.Sprintf("%d identical re-writes got no 412 response (connection closed by the server)", transport)}
	}
	return nil
}

func (p *prober) inmSame(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "same")
	if err != nil {
		return err
	}
	data := payload(4096, 1)
	if err := p.put(ctx, bucket, "k", data, true); err != nil {
		return fmt.Errorf("first conditional put: %v", err)
	}
	e1 := p.put(ctx, bucket, "k", data, true)
	e2 := p.put(ctx, bucket, "k", payload(4096, 2), true)
	fmt.Printf("  inm-same: identical content -> %d %s; different content -> %d %s\n", httpStatus(e1), errCode(e1), httpStatus(e2), errCode(e2))
	if errCode(e1) != "PreconditionFailed" || errCode(e2) != "PreconditionFailed" {
		return fmt.Errorf("want PreconditionFailed for both, got %v / %v", e1, e2)
	}
	got, err := p.get(ctx, bucket, "k")
	if err != nil || !bytes.Equal(got, data) {
		return fmt.Errorf("content changed after rejected writes (err=%v)", err)
	}
	// A deleted key can be created again with If-None-Match.
	if _, err := p.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")}); err != nil {
		return err
	}
	if err := p.put(ctx, bucket, "k", data, true); err != nil {
		return fmt.Errorf("conditional put after delete: %v", err)
	}
	return nil
}

func (p *prober) delEmpty(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "delempty")
	if err != nil {
		return err
	}
	_, err = p.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{}, Quiet: aws.Bool(true)},
	})
	fmt.Printf("  del-empty: DeleteObjects([]) -> %d %s (%v)\n", httpStatus(err), errCode(err), err)
	// Deleting keys that do not exist is not an error.
	out, err2 := p.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("nope/1")}, {Key: aws.String("nope/2")}}, Quiet: aws.Bool(true)},
	})
	nerr := -1
	if out != nil {
		nerr = len(out.Errors)
	}
	fmt.Printf("  del-empty: DeleteObjects(2 missing keys, quiet) -> %s, per-object errors: %d\n", errCode(err2), nerr)
	if err == nil {
		return errors.New("empty DeleteObjects unexpectedly succeeded (driver skips it anyway)")
	}
	if err2 != nil || nerr != 0 {
		return fmt.Errorf("deleting missing keys: %v (%d per-object errors)", err2, nerr)
	}
	return nil
}

// listDel mimics deleteObjectsWithPrefix: list a page, DeleteObjects that page, list the next page.
func (p *prober) listDel(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "listdel")
	if err != nil {
		return err
	}
	var bad []string
	for _, maxKeys := range []int32{1, 3, 7, 100, 1000} {
		prefix := fmt.Sprintf("tile/data/mk%d/", maxKeys)
		want := map[string]bool{}
		// Write keys concurrently, with a mix of "directory" depths like tile paths.
		var wg sync.WaitGroup
		keysCh := make(chan string)
		var putErr atomic.Value
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range keysCh {
					if err := p.put(ctx, bucket, k, []byte(k), false); err != nil {
						putErr.Store(err)
					}
				}
			}()
		}
		for i := 0; i < *listKeys; i++ {
			k := fmt.Sprintf("%sx%03d/%03d", prefix, i/100, i%100)
			if i%10 == 0 {
				k += ".p/" + fmt.Sprint(i%256)
			}
			want[k] = true
			keysCh <- k
		}
		close(keysCh)
		wg.Wait()
		if e := putErr.Load(); e != nil {
			return fmt.Errorf("put: %v", e)
		}
		seen := map[string]int{}
		pages := 0
		pg := s3.NewListObjectsV2Paginator(p.c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(maxKeys)})
		for pg.HasMorePages() {
			l, err := pg.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("list page %d: %v", pages, err)
			}
			pages++
			if len(l.Contents) == 0 {
				continue
			}
			d := &types.Delete{Quiet: aws.Bool(true)}
			for _, o := range l.Contents {
				seen[*o.Key]++
				d.Objects = append(d.Objects, types.ObjectIdentifier{Key: o.Key})
			}
			out, err := p.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: d})
			if err != nil {
				return fmt.Errorf("DeleteObjects: %v", err)
			}
			if len(out.Errors) > 0 {
				return fmt.Errorf("DeleteObjects: %d per-object errors, first: %s", len(out.Errors), aws.ToString(out.Errors[0].Message))
			}
		}
		missing, dups := 0, 0
		for k := range want {
			if seen[k] == 0 {
				missing++
			}
		}
		for _, n := range seen {
			if n > 1 {
				dups++
			}
		}
		l, err := p.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
		if err != nil {
			return err
		}
		fmt.Printf("  list-del: MaxKeys=%-4d keys=%d pages=%d listed=%d skipped=%d duplicates=%d remaining-after=%d\n", maxKeys, len(want), pages, len(seen), missing, dups, len(l.Contents))
		if missing > 0 || dups > 0 || len(l.Contents) > 0 || len(seen) != len(want) {
			bad = append(bad, fmt.Sprintf("MaxKeys=%d: skipped=%d dups=%d remaining=%d", maxKeys, missing, dups, len(l.Contents)))
		}
	}
	if len(bad) > 0 {
		return errors.New(strings.Join(bad, "; "))
	}
	return nil
}

// readAfterWrite: every acknowledged PUT is immediately visible to GET, HEAD and LIST (from
// the same and from other goroutines), and overwrites are visible immediately.
func (p *prober) readAfterWrite(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "raw")
	if err != nil {
		return err
	}
	var stale, listMiss, ow, other, total, globalMiss atomic.Int64
	var mu sync.Mutex
	acked := []string{}
	var firstErr atomic.Value
	stop := make(chan struct{})
	// Background listers: every key acknowledged before a LIST started must be in that LIST.
	var lwg sync.WaitGroup
	var lists atomic.Int64
	for l := 0; l < 4; l++ {
		lwg.Add(1)
		go func() {
			defer lwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				snap := append([]string(nil), acked...)
				mu.Unlock()
				got := map[string]bool{}
				pg := s3.NewListObjectsV2Paginator(p.c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("w/")})
				for pg.HasMorePages() {
					lo, err := pg.NextPage(ctx)
					if err != nil {
						firstErr.CompareAndSwap(nil, fmt.Errorf("list: %v", err))
						other.Add(1)
						break
					}
					for _, o := range lo.Contents {
						got[*o.Key] = true
					}
				}
				for _, k := range snap {
					if !got[k] {
						globalMiss.Add(1)
					}
				}
				lists.Add(1)
			}
		}()
	}
	var wg sync.WaitGroup
	for w := 0; w < *rawWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < *rawOps; i++ {
				total.Add(1)
				key := fmt.Sprintf("w/%02d/%05d", w, i)
				size := 1 + (w*7919+i*104729)%(256<<10)
				d1 := payload(size, w)
				if err := p.put(ctx, bucket, key, d1, true); err != nil {
					other.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Errorf("put %s: %v", key, err))
					continue
				}
				mu.Lock()
				acked = append(acked, key)
				mu.Unlock()
				got, err := p.get(ctx, bucket, key)
				if err != nil || !bytes.Equal(got, d1) {
					stale.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Errorf("read-after-write %s: err=%v", key, err))
				}
				if _, err := p.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
					stale.Add(1)
				}
				l, err := p.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(key)})
				if err != nil || len(l.Contents) != 1 || *l.Contents[0].Key != key || *l.Contents[0].Size != int64(size) {
					listMiss.Add(1)
				}
				// Overwrite (like the checkpoint object) and read back immediately.
				if i%4 == 0 {
					ck := fmt.Sprintf("ckpt/%02d", w)
					d2 := []byte(fmt.Sprintf("checkpoint %d %d %x", w, i, sha256.Sum256(d1)))
					if _, err := p.c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(ck), Body: bytes.NewReader(d2), ContentType: aws.String(ckptContType), CacheControl: aws.String(ckptCacheControl)}); err != nil {
						other.Add(1)
						continue
					}
					got, err := p.get(ctx, bucket, ck)
					if err != nil || !bytes.Equal(got, d2) {
						ow.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	lwg.Wait()
	fmt.Printf("  raw: %d conditional PUTs by %d workers; stale GET/HEAD=%d, LIST miss (own key)=%d, overwrite stale=%d, concurrent LIST passes=%d missing acked keys=%d, other errors=%d\n",
		total.Load(), *rawWorkers, stale.Load(), listMiss.Load(), ow.Load(), lists.Load(), globalMiss.Load(), other.Load())
	if stale.Load()+listMiss.Load()+ow.Load()+other.Load()+globalMiss.Load() > 0 {
		return fmt.Errorf("consistency violations; first error: %v", firstErr.Load())
	}
	return nil
}

func anonGet(url string) (int, []byte, http.Header, error) {
	r, err := http.Get(url)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = r.Body.Close() }()
	b, err := io.ReadAll(r.Body)
	return r.StatusCode, b, r.Header, err
}

func s3ErrCode(body []byte) string {
	s := string(body)
	if i := strings.Index(s, "<Code>"); i >= 0 {
		if j := strings.Index(s[i:], "</Code>"); j >= 0 {
			return s[i+6 : i+j]
		}
	}
	return ""
}

func (p *prober) setPolicy(ctx context.Context, bucket string, actions []string, bucketActions []string) error {
	st := []map[string]any{{"Effect": "Allow", "Principal": map[string]any{"AWS": []string{"*"}}, "Action": actions, "Resource": []string{"arn:aws:s3:::" + bucket + "/*"}}}
	if len(bucketActions) > 0 {
		st = append(st, map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": []string{"*"}}, "Action": bucketActions, "Resource": []string{"arn:aws:s3:::" + bucket}})
	}
	pol, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": st})
	_, err := p.c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(string(pol))})
	return err
}

func (p *prober) anon(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "anon")
	if err != nil {
		return err
	}
	data := payload(1000, 7)
	if err := p.put(ctx, bucket, "tile/0/000", data, true); err != nil {
		return err
	}
	base := strings.TrimSuffix(*endpoint, "/") + "/" + bucket + "/"
	var bad []string
	st, _, _, err := anonGet(base + "tile/0/000")
	fmt.Printf("  anon: no policy: GET existing -> %d\n", st)
	if err != nil || st != http.StatusForbidden {
		bad = append(bad, fmt.Sprintf("no policy: want 403, got %d (%v)", st, err))
	}
	// 1. Policy with s3:GetObject only (as in run.sh and e2e-basic.sh).
	if err := p.setPolicy(ctx, bucket, []string{"s3:GetObject"}, nil); err != nil {
		return fmt.Errorf("PutBucketPolicy: %v", err)
	}
	st, b, _, err := anonGet(base + "tile/0/000")
	stm, bm, _, _ := anonGet(base + "tile/0/missing")
	fmt.Printf("  anon: GetObject-only policy: GET existing -> %d (content ok=%v); GET missing -> %d %s\n", st, bytes.Equal(b, data), stm, s3ErrCode(bm))
	if err != nil || st != 200 || !bytes.Equal(b, data) {
		bad = append(bad, fmt.Sprintf("GetObject policy: GET existing: %d %v", st, err))
	}
	missingGetOnly := stm
	// 2. Policy equivalent to `mc anonymous set download` (adds s3:GetBucketLocation + s3:ListBucket).
	if err := p.setPolicy(ctx, bucket, []string{"s3:GetObject"}, []string{"s3:GetBucketLocation", "s3:ListBucket"}); err != nil {
		return fmt.Errorf("PutBucketPolicy: %v", err)
	}
	st, b, _, err = anonGet(base + "tile/0/000")
	stm, bm, _, _ = anonGet(base + "tile/0/missing")
	fmt.Printf("  anon: download policy (GetObject+ListBucket): GET existing -> %d (content ok=%v); GET missing -> %d %s\n", st, bytes.Equal(b, data), stm, s3ErrCode(bm))
	if err != nil || st != 200 || !bytes.Equal(b, data) {
		bad = append(bad, fmt.Sprintf("download policy: GET existing: %d %v", st, err))
	}
	if stm != http.StatusNotFound {
		bad = append(bad, fmt.Sprintf("download policy: GET missing: want 404, got %d", stm))
	}
	// Anonymous writes must still be refused.
	req, _ := http.NewRequest(http.MethodPut, base+"tile/0/evil", strings.NewReader("x"))
	r, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = r.Body.Close()
		fmt.Printf("  anon: anonymous PUT -> %d\n", r.StatusCode)
		if r.StatusCode != http.StatusForbidden {
			bad = append(bad, fmt.Sprintf("anonymous PUT: %d", r.StatusCode))
		}
	}
	if missingGetOnly != http.StatusNotFound {
		fmt.Printf("  anon: NOTE: with a GetObject-only policy a missing object is %d, not 404 (clients that map only 404 to 'not found' need s3:ListBucket in the policy)\n", missingGetOnly)
	}
	if len(bad) > 0 {
		return errors.New(strings.Join(bad, "; "))
	}
	return nil
}

func (p *prober) headers(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "hdr")
	if err != nil {
		return err
	}
	if err := p.setPolicy(ctx, bucket, []string{"s3:GetObject"}, []string{"s3:ListBucket"}); err != nil {
		return err
	}
	type obj struct{ key, ct, cc string }
	objs := []obj{{"tile/0/000", logContType, logCacheControl}, {"checkpoint", ckptContType, ckptCacheControl}}
	if err := p.put(ctx, bucket, objs[0].key, []byte("tile"), true); err != nil {
		return err
	}
	if _, err := p.c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(objs[1].key), Body: strings.NewReader("ckpt"), ContentType: aws.String(objs[1].ct), CacheControl: aws.String(objs[1].cc)}); err != nil {
		return err
	}
	var bad []string
	for _, o := range objs {
		h, err := p.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
		if err != nil {
			return err
		}
		g, err := p.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
		if err != nil {
			return err
		}
		_ = g.Body.Close()
		_, _, ah, err := anonGet(strings.TrimSuffix(*endpoint, "/") + "/" + bucket + "/" + o.key)
		if err != nil {
			return err
		}
		fmt.Printf("  headers: %-10s HEAD ct=%q cc=%q | GET ct=%q cc=%q | anon GET ct=%q cc=%q\n", o.key, aws.ToString(h.ContentType), aws.ToString(h.CacheControl), aws.ToString(g.ContentType), aws.ToString(g.CacheControl), ah.Get("Content-Type"), ah.Get("Cache-Control"))
		for _, v := range [][2]string{{aws.ToString(h.ContentType), o.ct}, {aws.ToString(h.CacheControl), o.cc}, {aws.ToString(g.ContentType), o.ct}, {aws.ToString(g.CacheControl), o.cc}, {ah.Get("Content-Type"), o.ct}, {ah.Get("Cache-Control"), o.cc}} {
			if v[0] != v[1] {
				bad = append(bad, fmt.Sprintf("%s: got %q want %q", o.key, v[0], v[1]))
			}
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		return errors.New(strings.Join(bad, "; "))
	}
	return nil
}
