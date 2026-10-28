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

// Byte-for-byte comparison of two buckets, and of coordination state.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/sync/errgroup"
)

type object struct {
	etag         string
	data         []byte
	contentType  string
	cacheControl string
}

// bucketReader snapshots a bucket. Objects are fetched in full; an object whose ETag has not
// changed since a previous snapshot is not fetched again (tiles and bundles are immutable).
type bucketReader struct {
	c      *s3.Client
	bucket string
	prefix string // BucketPrefix of the log, "" if none

	mu    sync.Mutex
	cache map[string]object
}

func newBucketReader(c *s3.Client, bucket, prefix string) *bucketReader {
	return &bucketReader{c: c, bucket: bucket, prefix: prefix, cache: map[string]object{}}
}

// snapshot returns all objects in the bucket, keyed by their log path (i.e. without the prefix).
// Objects outside the prefix are returned under their full key, prefixed with "!outside!".
func (b *bucketReader) snapshot(ctx context.Context) (map[string]object, error) {
	type item struct{ key, etag string }
	var items []item
	p := s3.NewListObjectsV2Paginator(b.c, &s3.ListObjectsV2Input{Bucket: sdkaws.String(b.bucket)})
	for p.HasMorePages() {
		l, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range l.Contents {
			items = append(items, item{sdkaws.ToString(o.Key), sdkaws.ToString(o.ETag)})
		}
	}
	res := make(map[string]object, len(items))
	var mu sync.Mutex
	eg, ectx := errgroup.WithContext(ctx)
	eg.SetLimit(16)
	for _, it := range items {
		b.mu.Lock()
		cached, ok := b.cache[it.key]
		b.mu.Unlock()
		if ok && cached.etag == it.etag {
			mu.Lock()
			res[it.key] = cached
			mu.Unlock()
			continue
		}
		eg.Go(func() error {
			r, err := b.c.GetObject(ectx, &s3.GetObjectInput{Bucket: sdkaws.String(b.bucket), Key: sdkaws.String(it.key)})
			if err != nil {
				return fmt.Errorf("GetObject(%s/%s): %v", b.bucket, it.key, err)
			}
			defer func() { _ = r.Body.Close() }()
			d, err := io.ReadAll(r.Body)
			if err != nil {
				return err
			}
			o := object{etag: sdkaws.ToString(r.ETag), data: d, contentType: sdkaws.ToString(r.ContentType), cacheControl: sdkaws.ToString(r.CacheControl)}
			b.mu.Lock()
			b.cache[it.key] = o
			b.mu.Unlock()
			mu.Lock()
			res[it.key] = o
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	if b.prefix == "" {
		return res, nil
	}
	out := make(map[string]object, len(res))
	pfx := strings.TrimSuffix(b.prefix, "/") + "/"
	for k, v := range res {
		if rest, ok := strings.CutPrefix(k, pfx); ok {
			out[rest] = v
		} else {
			out["!outside!"+k] = v
		}
	}
	return out, nil
}

// diffResult summarises the comparison of two bucket snapshots.
type diffResult struct {
	objects  int   // number of keys present in both
	bytes    int64 // number of bytes compared
	partials int   // number of partial (".p/") objects among them
	bundles  int
	tiles    int
	onlyA    []string
	onlyB    []string
	differ   []string // keys whose content or metadata differs
}

func (d diffResult) identical() bool {
	return len(d.onlyA) == 0 && len(d.onlyB) == 0 && len(d.differ) == 0
}

func (d diffResult) String() string {
	s := fmt.Sprintf("%d objects (%d entry bundles, %d tiles, %d partial, %d bytes) compared", d.objects, d.bundles, d.tiles, d.partials, d.bytes)
	if d.identical() {
		return s + ": byte-identical (data, Content-Type, Cache-Control)"
	}
	return s + fmt.Sprintf(": DIFFERENT: only in %s: %s; only in %s: %s; differing: %s", nameA, trunc(d.onlyA), nameB, trunc(d.onlyB), trunc(d.differ))
}

func trunc(s []string) string {
	if len(s) > 12 {
		return fmt.Sprintf("%v ... (%d total)", s[:12], len(s))
	}
	return fmt.Sprintf("%v", s)
}

func diffSnapshots(a, b map[string]object) diffResult {
	var d diffResult
	for k, oa := range a {
		ob, ok := b[k]
		if !ok {
			d.onlyA = append(d.onlyA, k)
			continue
		}
		d.objects++
		d.bytes += int64(len(oa.data))
		if strings.Contains(k, ".p/") {
			d.partials++
		}
		switch {
		case strings.HasPrefix(k, "tile/entries/"):
			d.bundles++
		case strings.HasPrefix(k, "tile/"):
			d.tiles++
		}
		if !bytes.Equal(oa.data, ob.data) || oa.contentType != ob.contentType || oa.cacheControl != ob.cacheControl {
			d.differ = append(d.differ, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d.onlyB = append(d.onlyB, k)
		}
	}
	slices.Sort(d.onlyA)
	slices.Sort(d.onlyB)
	slices.Sort(d.differ)
	return d
}

// compareCheckpoints compares two signed checkpoints, separately for the note text and signatures.
func compareCheckpoints(a, b []byte) (textEqual, sigEqual bool, desc string) {
	ta, sa, _ := bytes.Cut(a, []byte("\n\n"))
	tb, sb, _ := bytes.Cut(b, []byte("\n\n"))
	textEqual = bytes.Equal(ta, tb)
	sigEqual = bytes.Equal(sa, sb)
	lines := strings.Split(string(ta), "\n")
	return textEqual, sigEqual, fmt.Sprintf("%q", strings.Join(lines, `\n`))
}

// gcModel returns the keys of snapshot s which upstream's garbage collection semantics say must
// remain once GC has processed the tree up to gcSize (computed independently of either driver).
func gcModel(s map[string]object, gcSize uint64) map[string]object {
	doomed := map[string]bool{}
	for i := uint64(0); i < gcSize/layout.EntryBundleWidth; i++ {
		doomed[layout.EntriesPath(i, 0)+".p/"] = true
		doomed[layout.TilePath(0, i, 0)+".p/"] = true
		l, idx := uint64(0), i
		for idx%layout.TileWidth == layout.TileWidth-1 {
			l, idx = l+1, idx/layout.TileWidth
			doomed[layout.TilePath(l, idx, 0)+".p/"] = true
		}
	}
	out := map[string]object{}
	for k, v := range s {
		if i := strings.Index(k, ".p/"); i >= 0 && doomed[k[:i+3]] {
			continue
		}
		out[k] = v
	}
	return out
}
