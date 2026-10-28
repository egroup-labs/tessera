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

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Probes for the conditional checkpoint write (storage/s3pg: updateCheckpoint -> setObjectIfETag):
// the driver GETs the checkpoint, keeps the ETag exactly as the SDK returns it, and PUTs the new
// checkpoint with If-Match: <that ETag> (or If-None-Match: * if there was no checkpoint). It treats
// 412 PreconditionFailed and 409 ConditionalRequestConflict as "changed concurrently".
//
//	ifmatch       single-request semantics: current/stale/unquoted/garbage/"*" ETags, missing and
//	              deleted objects, ETag formats of PUT/GET/HEAD/LIST responses, body sizes
//	ifmatch-race  --racers writers per round, --ifm_rounds rounds, checkpoint-sized bodies
//	              (--ifm_min_size..--ifm_max_size); each round every writer PUTs with the ETag read by
//	              GET before the round (every --ifm_fresh_every-th round: a fresh key and
//	              If-None-Match: *). Exactly one success per round, stored content and ETag == winner's.

var (
	ifmRounds     = flag.Int("ifm_rounds", 200, "ifmatch-race: number of rounds")
	ifmMinSize    = flag.Int("ifm_min_size", 200, "ifmatch-race: minimum body size")
	ifmMaxSize    = flag.Int("ifm_max_size", 2048, "ifmatch-race: maximum body size")
	ifmFreshEvery = flag.Int("ifm_fresh_every", 10, "ifmatch-race: every n-th round races If-None-Match: * on a fresh key (0 = never)")
)

// outcome describes a PutObject result as "<status> <code>", e.g. "200 OK", "412 PreconditionFailed",
// "404 NoSuchKey", or "transport" if no HTTP response was received.
func outcome(err error) string {
	if err == nil {
		return "200 OK"
	}
	if st := httpStatus(err); st != 0 {
		return fmt.Sprintf("%d %s", st, errCode(err))
	}
	return "transport"
}

func (p *prober) putCond(ctx context.Context, bucket, key string, data []byte, ifMatch, ifNoneMatch string) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(ckptContType),
		CacheControl: aws.String(ckptCacheControl),
	}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	if ifNoneMatch != "" {
		in.IfNoneMatch = aws.String(ifNoneMatch)
	}
	out, err := p.c.PutObject(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// getWithETag is what the driver does (getKeyWithETag): GET, keep aws.ToString(ETag) verbatim.
func (p *prober) getWithETag(ctx context.Context, bucket, key string) ([]byte, string, error) {
	o, err := p.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = o.Body.Close() }()
	b := new(bytes.Buffer)
	if _, err := b.ReadFrom(o.Body); err != nil {
		return nil, "", err
	}
	return b.Bytes(), aws.ToString(o.ETag), nil
}

func ckptBody(size, writer, round int) []byte {
	head := fmt.Sprintf("example.com/log/testdata\n%d\n", round*1000+writer)
	b := payload(size, writer)
	copy(b, head)
	return b
}

func (p *prober) ifMatch(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "ifm")
	if err != nil {
		return err
	}
	var bad []string
	expect := func(what, got string, want ...string) {
		ok := false
		for _, w := range want {
			ok = ok || got == w
		}
		mark := "ok "
		if !ok {
			mark = "BAD"
			bad = append(bad, fmt.Sprintf("%s: got %q, want %q", what, got, want))
		}
		fmt.Printf("  ifmatch: %s %-62s -> %s\n", mark, what, got)
	}
	key := "checkpoint"
	c1 := ckptBody(300, 1, 1)
	putETag, err := p.putCond(ctx, bucket, key, c1, "", "*")
	expect("If-None-Match:* on missing object (first checkpoint)", outcome(err), "200 OK")
	_, getETag, err := p.getWithETag(ctx, bucket, key)
	if err != nil {
		return err
	}
	h, err := p.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	l, err := p.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(key)})
	if err != nil || len(l.Contents) != 1 {
		return fmt.Errorf("list: %v", err)
	}
	fmt.Printf("  ifmatch: ETag formats: PUT response %s | GET %s | HEAD %s | LIST %s\n", putETag, getETag, aws.ToString(h.ETag), aws.ToString(l.Contents[0].ETag))
	quoted := func(s string) bool { return len(s) > 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) }
	if !quoted(getETag) || getETag != putETag || getETag != aws.ToString(h.ETag) || getETag != aws.ToString(l.Contents[0].ETag) {
		bad = append(bad, "ETag formats differ or are not quoted")
	}
	expect("If-None-Match:* on existing object", outcome(func() error { _, e := p.putCond(ctx, bucket, key, ckptBody(300, 2, 1), "", "*"); return e }()), "412 PreconditionFailed")

	// Current ETag (as returned by GET, quoted) -> 200; the old one is stale afterwards.
	c2 := ckptBody(400, 3, 2)
	e2, err := p.putCond(ctx, bucket, key, c2, getETag, "")
	expect("If-Match: current ETag (quoted, from GET)", outcome(err), "200 OK")
	_, err = p.putCond(ctx, bucket, key, ckptBody(400, 4, 2), getETag, "")
	expect("If-Match: stale ETag", outcome(err), "412 PreconditionFailed")
	if got, _, gErr := p.getWithETag(ctx, bucket, key); gErr != nil || !bytes.Equal(got, c2) {
		bad = append(bad, fmt.Sprintf("content changed by a rejected stale If-Match write (err=%v)", gErr))
	}
	// Stale ETag with exactly the bytes already stored (driver's read-back case).
	_, err = p.putCond(ctx, bucket, key, c2, getETag, "")
	expect("If-Match: stale ETag, body identical to stored content", outcome(err), "412 PreconditionFailed")
	// Unquoted current ETag.
	c3 := ckptBody(500, 5, 3)
	e3, err := p.putCond(ctx, bucket, key, c3, strings.Trim(e2, `"`), "")
	expect("If-Match: current ETag without quotes", outcome(err), "200 OK", "412 PreconditionFailed")
	if err != nil {
		e3 = e2
	}
	_, err = p.putCond(ctx, bucket, key, ckptBody(500, 6, 3), `"00000000000000000000000000000000"`, "")
	expect("If-Match: well-formed but wrong ETag", outcome(err), "412 PreconditionFailed")
	_, err = p.putCond(ctx, bucket, key, ckptBody(500, 6, 3), "garbage", "")
	expect("If-Match: garbage ETag", outcome(err), "412 PreconditionFailed")
	// Re-writing identical content keeps the ETag (MD5): an ETag names content, not a version.
	e4, err := p.putCond(ctx, bucket, key, c3, e3, "")
	expect("If-Match: current ETag, identical content", outcome(err), "200 OK")
	fmt.Printf("  ifmatch: ETag after identical re-write: %s (before %s, equal=%v)\n", e4, e3, e4 == e3)
	_, err = p.putCond(ctx, bucket, key, ckptBody(500, 7, 4), "*", "")
	expect("If-Match: * on existing object", outcome(err), "200 OK", "412 PreconditionFailed")
	// Both conditions at once.
	_, cur, _ := p.getWithETag(ctx, bucket, key)
	_, err = p.putCond(ctx, bucket, key, ckptBody(500, 8, 4), cur, "*")
	expect("If-Match: current + If-None-Match: * (both)", outcome(err), "200 OK", "412 PreconditionFailed")

	// Missing objects.
	_, err = p.putCond(ctx, bucket, "never-existed", ckptBody(300, 1, 1), cur, "")
	expect("If-Match: some ETag on a missing object", outcome(err), "412 PreconditionFailed", "404 NoSuchKey")
	_, err = p.putCond(ctx, bucket, "never-existed-2", ckptBody(300, 1, 1), "*", "")
	expect("If-Match: * on a missing object", outcome(err), "412 PreconditionFailed", "404 NoSuchKey")
	if _, gErr := p.get(ctx, bucket, "never-existed"); gErr == nil {
		bad = append(bad, "rejected If-Match on a missing object created it")
	}
	_, cur, _ = p.getWithETag(ctx, bucket, key)
	if _, err := p.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		return err
	}
	_, err = p.putCond(ctx, bucket, key, ckptBody(300, 9, 5), cur, "")
	expect("If-Match: last ETag of a deleted object", outcome(err), "412 PreconditionFailed", "404 NoSuchKey")

	// Body sizes: a rejected conditional PUT is answered before the body is read; find where the
	// 412 stops arriving (connection closed instead), and check the accepted path at each size.
	sizes := []int{200, 512, 1024, 2048, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20}
	var sz []string
	for _, s := range sizes {
		k := fmt.Sprintf("size/%d", s)
		e, err := p.putCond(ctx, bucket, k, ckptBody(s, 1, 1), "", "*")
		if err != nil {
			return fmt.Errorf("create %s: %v", k, err)
		}
		res := map[string]int{}
		for i := 0; i < 10; i++ {
			_, err := p.putCond(ctx, bucket, k, ckptBody(s, 2, i), `"0123456789abcdef0123456789abcdef"`, "")
			res[outcome(err)]++
		}
		ne, err := p.putCond(ctx, bucket, k, ckptBody(s, 3, 1), e, "")
		ok := outcome(err)
		sz = append(sz, fmt.Sprintf("%d: stale %v, current %s", s, res, ok))
		if ok != "200 OK" || ne == "" {
			bad = append(bad, fmt.Sprintf("size %d: If-Match current -> %s", s, ok))
		}
		if s <= 2048 && res["412 PreconditionFailed"] != 10 {
			bad = append(bad, fmt.Sprintf("checkpoint-sized body %d: stale If-Match -> %v", s, res))
		}
		for o := range res {
			if o != "412 PreconditionFailed" && o != "transport" {
				bad = append(bad, fmt.Sprintf("size %d: stale If-Match -> %s", s, o))
			}
		}
	}
	fmt.Printf("  ifmatch: body sizes (10 stale If-Match each, then one with the current ETag):\n    %s\n", strings.Join(sz, "\n    "))
	if len(bad) > 0 {
		return errors.New(strings.Join(bad, "; "))
	}
	return nil
}

func (p *prober) ifMatchRace(ctx context.Context) error {
	bucket, err := p.newBucket(ctx, "ifmrace")
	if err != nil {
		return err
	}
	key := "checkpoint"
	if _, err := p.putCond(ctx, bucket, key, ckptBody(*ifmMinSize, 0, 0), "", "*"); err != nil {
		return err
	}
	counts := map[string]map[string]int{"If-Match": {}, "If-None-Match": {}}
	var bad []string
	var firstOther error
	rounds := map[string]int{}
	for r := 1; r <= *ifmRounds; r++ {
		mode, k := "If-Match", key
		var etag string
		if *ifmFreshEvery > 0 && r%*ifmFreshEvery == 0 {
			mode, k = "If-None-Match", fmt.Sprintf("fresh/%d/checkpoint", r)
		} else {
			_, e, err := p.getWithETag(ctx, bucket, key)
			if err != nil {
				return fmt.Errorf("round %d: GET: %v", r, err)
			}
			etag = e
		}
		rounds[mode]++
		data := make([][]byte, *racers)
		for w := range data {
			data[w] = ckptBody(*ifmMinSize+(r*7919+w*104729)%(*ifmMaxSize-*ifmMinSize+1), w, r)
		}
		errs := make([]error, *racers)
		etags := make([]string, *racers)
		gate := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < *racers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-gate
				if mode == "If-Match" {
					etags[w], errs[w] = p.putCond(ctx, bucket, k, data[w], etag, "")
				} else {
					etags[w], errs[w] = p.putCond(ctx, bucket, k, data[w], "", "*")
				}
			}(w)
		}
		close(gate)
		wg.Wait()
		win, nOK := -1, 0
		for w, e := range errs {
			o := outcome(e)
			counts[mode][o]++
			if e == nil {
				nOK++
				win = w
			} else if o != "412 PreconditionFailed" && firstOther == nil {
				firstOther = e
			}
		}
		if nOK != 1 {
			bad = append(bad, fmt.Sprintf("round %d (%s): %d successes", r, mode, nOK))
		}
		got, ge, err := p.getWithETag(ctx, bucket, k)
		if err != nil {
			bad = append(bad, fmt.Sprintf("round %d: GET after race: %v", r, err))
			continue
		}
		if win >= 0 && (!bytes.Equal(got, data[win]) || ge != etags[win]) {
			bad = append(bad, fmt.Sprintf("round %d (%s): stored content/ETag is not the winner's (writer %d)", r, mode, win))
		}
		// A writer that lost the round and retries with the pre-round ETag must still fail.
		if mode == "If-Match" && win >= 0 {
			_, err := p.putCond(ctx, bucket, k, data[(win+1)%*racers], etag, "")
			counts["stale retry after round"] = addCount(counts["stale retry after round"], outcome(err))
			if err == nil {
				bad = append(bad, fmt.Sprintf("round %d: stale If-Match succeeded after the round", r))
			}
		}
	}
	fmt.Printf("  ifmatch-race: %d rounds x %d writers, bodies %d..%d B, max SDK attempts %d (0=default)\n", *ifmRounds, *racers, *ifmMinSize, *ifmMaxSize, *sdkAttempts)
	modes := make([]string, 0, len(counts))
	for m := range counts {
		modes = append(modes, m)
	}
	sort.Strings(modes)
	for _, m := range modes {
		fmt.Printf("  ifmatch-race: %-24s rounds=%-4d outcomes %v\n", m, rounds[m], counts[m])
	}
	if firstOther != nil {
		fmt.Printf("  ifmatch-race: first non-412 failure: %v\n", firstOther)
	}
	for _, m := range counts {
		for o, n := range m {
			if strings.HasPrefix(o, "409") {
				fmt.Printf("  ifmatch-race: NOTE: %d x %s\n", n, o)
			}
		}
	}
	if len(bad) > 0 {
		if len(bad) > 20 {
			bad = append(bad[:20], fmt.Sprintf("... and %d more", len(bad)-20))
		}
		return errors.New(strings.Join(bad, "\n    "))
	}
	return nil
}

func addCount(m map[string]int, k string) map[string]int {
	if m == nil {
		m = map[string]int{}
	}
	m[k]++
	return m
}
