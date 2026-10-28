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

// Tests for S3 writes whose outcome is unknown (the request was sent, but no response arrived):
// the object is read back, and the write counts as successful iff it now holds exactly our data.
// Also: listObjects returns names relative to the BucketPrefix, across pages.

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// lossyS3 is a minimal path-style S3 endpoint (bucket "b") supporting GET, conditional PUT
// (If-None-Match: * and If-Match) and ListObjectsV2, which can apply a PUT and then drop the
// connection without responding.
type lossyS3 struct {
	t        *testing.T
	mu       sync.Mutex
	objs     map[string][]byte
	puts     map[string]int
	pageSize int
	listed   []string
	// put decides, for the n-th PUT (from 1) of key, whether the write is applied (if its
	// condition holds) and whether a response is sent at all. Default: (true, true).
	put func(key string, n int) (apply, respond bool)
	// beforePut, if set, runs (under the lock, on the objects) before a PUT's condition is
	// evaluated, e.g. to simulate a concurrent writer.
	beforePut func(objs map[string][]byte, key string, n int)
	// failGets makes GETs fail with a 500.
	failGets bool
	// denyPuts makes PUTs fail with 403 AccessDenied (a known outcome: nothing written).
	denyPuts bool
}

func s3ETag(d []byte) string { return fmt.Sprintf(`"%x"`, md5.Sum(d)) }

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func (f *lossyS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key, ok := strings.CutPrefix(r.URL.Path, "/b/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		f.list(w, q.Get("prefix"), q.Get("continuation-token"))
	case !ok:
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
	case r.Method == http.MethodGet:
		if f.failGets {
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
		d, ok := f.objs[key]
		if !ok {
			s3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", s3ETag(d))
		_, _ = w.Write(d)
	case r.Method == http.MethodPut:
		if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
			f.t.Errorf("unexpected aws-chunked PUT body")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			f.t.Errorf("read PUT body: %v", err)
			return
		}
		f.puts[key]++
		n := f.puts[key]
		if f.denyPuts {
			s3Error(w, http.StatusForbidden, "AccessDenied")
			return
		}
		apply, respond := true, true
		if f.put != nil {
			apply, respond = f.put(key, n)
		}
		if f.beforePut != nil {
			f.beforePut(f.objs, key, n)
		}
		cur, exists := f.objs[key]
		status, code := http.StatusOK, ""
		if r.Header.Get("If-None-Match") == "*" && exists {
			status, code = http.StatusPreconditionFailed, "PreconditionFailed"
		}
		if im := r.Header.Get("If-Match"); im != "" {
			if !exists {
				status, code = http.StatusNotFound, "NoSuchKey"
			} else if s3ETag(cur) != im {
				status, code = http.StatusPreconditionFailed, "PreconditionFailed"
			}
		}
		if status == http.StatusOK && apply {
			f.objs[key] = body
		}
		if !respond {
			// The request's outcome is unknown to the client.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				f.t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		if status != http.StatusOK {
			s3Error(w, status, code)
			return
		}
		w.Header().Set("ETag", s3ETag(body))
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func (f *lossyS3) list(w http.ResponseWriter, prefix, token string) {
	f.listed = append(f.listed, prefix)
	matching := []string{}
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			matching = append(matching, k)
		}
	}
	slices.Sort(matching)
	start := 0
	if token != "" {
		start, _ = slices.BinarySearch(matching, token+"\x00")
	}
	end := min(start+f.pageSize, len(matching))
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys>`, prefix, end-start, f.pageSize)
	if end < len(matching) {
		fmt.Fprintf(&b, `<IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken>`, matching[end-1])
	} else {
		b.WriteString(`<IsTruncated>false</IsTruncated>`)
	}
	for _, k := range matching[start:end] {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, k)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, b.String())
}

func (f *lossyS3) object(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.objs[key]
	return d, ok
}

func (f *lossyS3) putCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts[key]
}

// newLossyS3Storage returns an s3Storage using f, whose client makes at most attempts attempts
// per request, without backoff.
func newLossyS3Storage(t *testing.T, f *lossyS3, prefix string, attempts int) *s3Storage {
	t.Helper()
	f.t = t
	if f.objs == nil {
		f.objs = map[string][]byte{}
	}
	f.puts = map[string]int{}
	if f.pageSize == 0 {
		f.pageSize = 1000
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("a", "b", "")}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = attempts
			so.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			so.RateLimiter = ratelimit.None
		})
	})
	return &s3Storage{s3Client: c, bucket: "b", bucketPrefix: prefix}
}

func TestUnknownOutcomeWrites(t *testing.T) {
	t.Parallel()
	const prefix = "pfx"
	const key = "tile/0/000.p/5"
	full := prefix + "/" + key
	data, other := []byte("our data"), []byte("other data")
	lostResponse := func(string, int) (bool, bool) { return true, false }
	lostRequest := func(string, int) (bool, bool) { return false, false }
	firstResponseLost := func(_ string, n int) (bool, bool) { return true, n > 1 }

	for _, tc := range []struct {
		name     string
		existing []byte
		put      func(string, int) (bool, bool)
		before   func(objs map[string][]byte, key string, n int)
		failGets bool
		denyPuts bool
		attempts int
		// ifETag selects setObjectIfETag; etag is then the ETag of existing (or "" if none, or if noETag).
		ifETag, noETag bool
		// wantOK is whether the write must succeed, want the object afterwards (nil: absent).
		wantOK           bool
		want             []byte
		wantPuts         int
		wantPrecondition bool
	}{
		// setObjectIfNoneMatch
		{name: "IfNoneMatch: applied, response lost", put: lostResponse, attempts: 1, wantOK: true, want: data, wantPuts: 1},
		{name: "IfNoneMatch: applied, response lost, SDK retry gets 412", put: firstResponseLost, attempts: 3, wantOK: true, want: data, wantPuts: 2},
		{name: "IfNoneMatch: identical object exists, response lost", existing: data, put: lostResponse, attempts: 1, wantOK: true, want: data, wantPuts: 1},
		{name: "IfNoneMatch: different object exists, response lost", existing: other, put: lostResponse, attempts: 1, want: other, wantPuts: 1},
		{name: "IfNoneMatch: different object exists, 412", existing: other, attempts: 1, want: other, wantPuts: 1},
		{name: "IfNoneMatch: not applied, no response", put: lostRequest, attempts: 1, want: nil, wantPuts: 1},
		{name: "IfNoneMatch: not applied, no response, all retries", put: lostRequest, attempts: 3, want: nil, wantPuts: 3},
		{name: "IfNoneMatch: response lost and read-back fails", put: lostResponse, failGets: true, attempts: 1, want: data, wantPuts: 1},
		// A definitive API error other than a precondition failure is returned as such (as
		// upstream), even if the object happens to hold the data already.
		{name: "IfNoneMatch: identical object exists, PUT denied", existing: data, denyPuts: true, attempts: 1, want: data, wantPuts: 1},
		// setObjectIfETag
		{name: "IfMatch: applied, response lost", ifETag: true, existing: other, put: lostResponse, attempts: 1, wantOK: true, want: data, wantPuts: 1},
		{name: "IfMatch: applied, response lost, SDK retry gets 412", ifETag: true, existing: other, put: firstResponseLost, attempts: 3, wantOK: true, want: data, wantPuts: 2},
		{name: "IfMatch: not applied, no response", ifETag: true, existing: other, put: lostRequest, attempts: 1, want: other, wantPuts: 1},
		{name: "IfMatch: concurrent writer wins, response lost", ifETag: true, existing: other, put: lostResponse, attempts: 1,
			before: func(objs map[string][]byte, k string, _ int) { objs[k] = []byte("newer") }, want: []byte("newer"), wantPuts: 1},
		{name: "IfMatch: concurrent writer wins, 412", ifETag: true, existing: other, attempts: 1,
			before: func(objs map[string][]byte, k string, _ int) { objs[k] = []byte("newer") }, want: []byte("newer"), wantPuts: 1, wantPrecondition: true},
		{name: "IfMatch: 412 and read-back fails", ifETag: true, existing: other, attempts: 1, failGets: true,
			before: func(objs map[string][]byte, k string, _ int) { objs[k] = []byte("newer") }, want: []byte("newer"), wantPuts: 1, wantPrecondition: true},
		{name: "IfMatch: concurrent writer wrote our data, 412", ifETag: true, existing: other, attempts: 1,
			before: func(objs map[string][]byte, k string, _ int) { objs[k] = bytes.Clone(data) }, wantOK: true, want: data, wantPuts: 1},
		{name: "IfMatch: identical object exists, PUT denied", ifETag: true, existing: data, denyPuts: true, attempts: 1, want: data, wantPuts: 1},
		{name: "IfNoneMatch via IfETag: applied, response lost", ifETag: true, put: lostResponse, attempts: 1, wantOK: true, want: data, wantPuts: 1},
		{name: "IfNoneMatch via IfETag: not applied, no response", ifETag: true, put: lostRequest, attempts: 1, want: nil, wantPuts: 1},
		{name: "IfNoneMatch via IfETag: different object exists, response lost", ifETag: true, noETag: true, existing: other, put: lostResponse, attempts: 1, want: other, wantPuts: 1},
		{name: "IfNoneMatch via IfETag: identical object exists, response lost", ifETag: true, noETag: true, existing: data, put: lostResponse, attempts: 1, wantOK: true, want: data, wantPuts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			f := &lossyS3{put: tc.put, beforePut: tc.before}
			s := newLossyS3Storage(t, f, prefix, tc.attempts)
			etag := ""
			if tc.existing != nil {
				f.objs[full] = bytes.Clone(tc.existing)
				if !tc.noETag {
					etag = s3ETag(tc.existing)
				}
			}
			f.failGets, f.denyPuts = tc.failGets, tc.denyPuts
			var err error
			if tc.ifETag {
				err = s.setObjectIfETag(ctx, key, data, ckptContType, ckptCacheControl, etag)
			} else {
				err = s.setObjectIfNoneMatch(ctx, key, data, logContType, logCacheControl)
			}
			if gotOK := err == nil; gotOK != tc.wantOK {
				t.Errorf("write = %v, want success %v", err, tc.wantOK)
			}
			if tc.ifETag && errors.Is(err, errPreconditionFailed) != tc.wantPrecondition {
				t.Errorf("write = %v, want errPreconditionFailed: %v", err, tc.wantPrecondition)
			}
			got, ok := f.object(full)
			if tc.want == nil && ok {
				t.Errorf("object = %q, want absent", got)
			} else if tc.want != nil && (!ok || !bytes.Equal(got, tc.want)) {
				t.Errorf("object = (%q, %v), want %q", got, ok, tc.want)
			}
			if n := f.putCount(full); n != tc.wantPuts {
				t.Errorf("%d PUTs of %q, want %d", n, full, tc.wantPuts)
			}
			if f.putCount(key) != 0 {
				t.Errorf("object written without the bucket prefix")
			}
		})
	}
}

// listObjects returns all names under the prefix, relative to the BucketPrefix, across pages.
func TestListObjectsPrefix(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, tc := range []struct {
		bucketPrefix, listPrefix string
		keys                     []string
		wantListed               string
		want                     []string
	}{
		{"", "tile/0/000", []string{"tile/0/000", "tile/0/000.p/5", "tile/0/000.p/7", "tile/0/001", "x/tile/0/000"}, "tile/0/000", []string{"tile/0/000", "tile/0/000.p/5", "tile/0/000.p/7"}},
		{"pfx", "tile/0/000", []string{"pfx/tile/0/000", "pfx/tile/0/000.p/5", "pfx/tile/0/000.p/7", "pfx/tile/0/001", "pfxx/tile/0/000", "tile/0/000"}, "pfx/tile/0/000", []string{"tile/0/000", "tile/0/000.p/5", "tile/0/000.p/7"}},
		{"pfx/", "tile/0/000", []string{"pfx/tile/0/000", "pfx/tile/0/000.p/5"}, "pfx/tile/0/000", []string{"tile/0/000", "tile/0/000.p/5"}},
		{"a/b", "tile/entries/000.p/", []string{"a/b/tile/entries/000.p/1", "a/b/tile/entries/000.p/2", "a/b/tile/entries/000.p/3", "a/b/tile/entries/000", "a/tile/entries/000.p/1"}, "a/b/tile/entries/000.p/", []string{"tile/entries/000.p/1", "tile/entries/000.p/2", "tile/entries/000.p/3"}},
		{"pfx", "nothing", []string{"pfx/tile/0/000"}, "pfx/nothing", []string{}},
	} {
		t.Run(fmt.Sprintf("%q %q", tc.bucketPrefix, tc.listPrefix), func(t *testing.T) {
			f := &lossyS3{pageSize: 2, objs: map[string][]byte{}}
			for _, k := range tc.keys {
				f.objs[k] = []byte(k)
			}
			s := newLossyS3Storage(t, f, tc.bucketPrefix, 1)
			got, err := s.listObjects(ctx, tc.listPrefix)
			if err != nil {
				t.Fatalf("listObjects: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("listObjects(%q) = %q, want %q", tc.listPrefix, got, tc.want)
			}
			if len(f.listed) == 0 || f.listed[0] != tc.wantListed {
				t.Errorf("listed prefixes %q, want %q first", f.listed, tc.wantListed)
			}
			if want := max(1, (len(tc.want)+1)/2); len(f.listed) != want && len(tc.want) > 0 {
				t.Errorf("%d ListObjects requests, want %d (pages of 2)", len(f.listed), want)
			}
		})
	}
}
