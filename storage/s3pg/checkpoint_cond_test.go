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

// Tests for conditional checkpoint writes ("F1"): the checkpoint is only replaced if it hasn't
// changed since the publication guard read it, so that a publisher never overwrites a newer
// checkpoint with an older one.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
)

// condBackend is an objStore under test, with direct access to the stored objects.
type condBackend struct {
	name  string
	store objStore
	// raw returns the object stored under key (without the bucket prefix), bypassing store.
	raw func(t *testing.T, key string) ([]byte, bool)
	// isS3 is true for the real S3 store, whose identical-content read-back the in-memory fake
	// doesn't implement.
	isS3   bool
	bucket *testBucket
	prefix string
}

func condBackends(t *testing.T) []condBackend {
	t.Helper()
	m := newMemObjStore()
	r := []condBackend{{
		name:  "mem",
		store: m,
		raw: func(_ *testing.T, key string) ([]byte, bool) {
			d, ok := memContents(m)[key]
			return d, ok
		},
	}}
	if os.Getenv(s3EndpointEnv) == "" {
		return r
	}
	for _, prefix := range []string{"", "pfx/sub"} {
		b := newTestBucket(t)
		reader := &s3Reader{client: b.Client, bucket: b.Name, prefix: prefix}
		r = append(r, condBackend{
			name:  fmt.Sprintf("s3 prefix=%q", prefix),
			store: &s3Storage{s3Client: s3.NewFromConfig(*b.SDKConfig, b.S3Options), bucket: b.Name, bucketPrefix: prefix},
			raw: func(t *testing.T, key string) ([]byte, bool) {
				t.Helper()
				d, err := reader.get(t.Context(), key)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return nil, false
					}
					t.Fatalf("raw get %q: %v", key, err)
				}
				return d, true
			},
			isS3:   true,
			bucket: b,
			prefix: prefix,
		})
	}
	return r
}

// setObjectIfETag: If-None-Match ("" etag) and If-Match semantics, with read-back of identical content.
func TestSetObjectIfETagSemantics(t *testing.T) {
	t.Parallel()
	for _, be := range condBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			const k = layout.CheckpointPath
			v1, v2, v3 := []byte("one\n"), []byte("two\n"), []byte("three\n")
			check := func(step string, key string, want []byte) {
				t.Helper()
				got, ok := be.raw(t, key)
				if want == nil {
					if ok {
						t.Errorf("%s: %q = %q, want absent", step, key, got)
					}
					return
				}
				if !ok || !bytes.Equal(got, want) {
					t.Errorf("%s: %q = (%q, present=%v), want %q", step, key, got, ok, want)
				}
			}
			wantPrecondition := func(step string, err error) {
				t.Helper()
				if !errors.Is(err, errPreconditionFailed) {
					t.Errorf("%s: err = %v, want errPreconditionFailed", step, err)
				}
			}

			if err := be.store.setObjectIfETag(ctx, k, v1, ckptContType, ckptCacheControl, ""); err != nil {
				t.Fatalf("If-None-Match on absent object: %v", err)
			}
			check("If-None-Match on absent object", k, v1)
			got, e1, err := be.store.getObjectWithETag(ctx, k)
			if err != nil || !bytes.Equal(got, v1) || e1 == "" {
				t.Fatalf("getObjectWithETag = (%q, %q, %v), want (%q, non-empty, nil)", got, e1, err, v1)
			}

			wantPrecondition("If-None-Match on present object", be.store.setObjectIfETag(ctx, k, v2, ckptContType, ckptCacheControl, ""))
			check("If-None-Match on present object", k, v1)
			if be.isS3 {
				if err := be.store.setObjectIfETag(ctx, k, v1, ckptContType, ckptCacheControl, ""); err != nil {
					t.Errorf("If-None-Match on present object with identical content: %v, want nil (read back)", err)
				}
				check("If-None-Match, identical content", k, v1)
			}

			if err := be.store.setObjectIfETag(ctx, k, v2, ckptContType, ckptCacheControl, e1); err != nil {
				t.Fatalf("If-Match current: %v", err)
			}
			check("If-Match current", k, v2)
			got, e2, err := be.store.getObjectWithETag(ctx, k)
			if err != nil || !bytes.Equal(got, v2) || e2 == "" || e2 == e1 {
				t.Fatalf("getObjectWithETag after update = (%q, %q, %v), want (%q, new ETag != %q)", got, e2, err, v2, e1)
			}

			wantPrecondition("If-Match stale", be.store.setObjectIfETag(ctx, k, v3, ckptContType, ckptCacheControl, e1))
			check("If-Match stale", k, v2)
			if be.isS3 {
				if err := be.store.setObjectIfETag(ctx, k, v2, ckptContType, ckptCacheControl, e1); err != nil {
					t.Errorf("If-Match stale with identical content: %v, want nil (read back)", err)
				}
				check("If-Match stale, identical content", k, v2)
			}

			// If-Match on an object which doesn't exist (any more) never creates it, and is a
			// precondition failure (MinIO and moto answer 404 NoSuchKey).
			wantPrecondition("If-Match on absent object", be.store.setObjectIfETag(ctx, "gone", v1, ckptContType, ckptCacheControl, e2))
			check("If-Match on absent object", "gone", nil)

			if !be.isS3 {
				return
			}
			var nsk *types.NoSuchKey
			if _, _, err := be.store.getObjectWithETag(ctx, "gone"); !errors.As(err, &nsk) {
				t.Errorf("getObjectWithETag(absent) = %v, want NoSuchKey", err)
			}
			// The ETag is S3's, and the checkpoint is written with its content type and cache control.
			lrs := &logResourceStore{objStore: be.store, entriesPath: layout.EntriesPath}
			if err := lrs.setCheckpointIfETag(ctx, v3, e2); err != nil {
				t.Fatalf("setCheckpointIfETag: %v", err)
			}
			raw, e3, err := lrs.readCheckpointWithETag(ctx)
			if err != nil || !bytes.Equal(raw, v3) {
				t.Fatalf("readCheckpointWithETag = (%q, %q, %v), want %q", raw, e3, err, v3)
			}
			h, err := be.bucket.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(be.bucket.Name), Key: aws.String(strings.TrimPrefix(be.prefix+"/"+k, "/"))})
			if err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			if aws.ToString(h.ETag) != e3 {
				t.Errorf("ETag = %q, S3 says %q", e3, aws.ToString(h.ETag))
			}
			if got := aws.ToString(h.ContentType); got != ckptContType {
				t.Errorf("checkpoint Content-Type = %q, want %q", got, ckptContType)
			}
			if got := aws.ToString(h.CacheControl); got != ckptCacheControl {
				t.Errorf("checkpoint Cache-Control = %q, want %q", got, ckptCacheControl)
			}
		})
	}
}

// readCheckpointWithETag: a missing checkpoint is (nil, "", nil), any other read error is an error.
func TestReadCheckpointWithETag(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	spy := &spyStore{objStore: newMemObjStore()}
	lrs := &logResourceStore{objStore: spy, entriesPath: layout.EntriesPath}
	if raw, etag, err := lrs.readCheckpointWithETag(ctx); raw != nil || etag != "" || err != nil {
		t.Errorf("readCheckpointWithETag(absent) = (%q, %q, %v), want (nil, \"\", nil)", raw, etag, err)
	}
	spy.getErr = func(string) error { return &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"} }
	if raw, _, err := lrs.readCheckpointWithETag(ctx); err == nil || raw != nil {
		t.Errorf("readCheckpointWithETag(AccessDenied) = (%q, %v), want error", raw, err)
	}
}

// The checkpoint changes between the publication guard's read and the conditional write (another
// publisher, whose PubCoord transaction has already ended, writes a newer checkpoint): the write
// fails with a precondition failure, the newer checkpoint stays, PubCoord is not committed, and
// the guards are not tripped. A later publication succeeds and is consistent.
func TestUpdateCheckpointRace(t *testing.T) {
	t.Parallel()
	for _, be := range condBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			env := newPGTestEnv(t)
			seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
			spy := &spyStore{objStore: be.store}
			a, st := newBareAppender(spy, seq, layout.EntriesPath)
			if err := a.init(ctx); err != nil {
				t.Fatalf("init: %v", err)
			}
			publish := func() error {
				_, err := seq.publishCheckpoint(ctx, 0, 0, a.updateCheckpoint)
				return err
			}
			cpNow := func() []byte {
				t.Helper()
				d, ok := be.raw(t, layout.CheckpointPath)
				if !ok {
					t.Fatal("no checkpoint")
				}
				return d
			}
			if err := publish(); err != nil {
				t.Fatalf("publish empty tree: %v", err)
			}
			all := addAndIntegrate(t, ctx, seq, a, "one", 10, 5)
			if err := publish(); err != nil {
				t.Fatalf("publish 10: %v", err)
			}
			all = append(all, addAndIntegrate(t, ctx, seq, a, "two", 10, 5)...)
			three := batchData("three", 10)
			assignData(t, ctx, seq, three...)
			for _, d := range three {
				all = append(all, []byte(d))
			}
			cp30 := mustTestCP(30, rootOf(t, all[:30]))
			pubAt, pubSize := env.pubCoord(t)

			spy.afterCPRead = func(ctx context.Context) {
				// Another instance integrates the next 10 entries and publishes them.
				integrateAll(t, ctx, seq, a)
				if err := be.store.setObject(ctx, layout.CheckpointPath, cp30, ckptContType, ckptCacheControl); err != nil {
					t.Fatalf("concurrent checkpoint write: %v", err)
				}
			}
			err := publish()
			if spy.afterCPRead != nil {
				t.Fatal("the checkpoint was never read with its ETag")
			}
			if err == nil || !strings.Contains(err.Error(), errPreconditionFailed.Error()) {
				t.Fatalf("publication racing a newer checkpoint: err = %v, want precondition failure", err)
			}
			if got := cpNow(); !bytes.Equal(got, cp30) {
				t.Errorf("checkpoint after the failed publication = %q, want the newer %q", got, cp30)
			}
			if at, size := env.pubCoord(t); at != pubAt || size != pubSize {
				t.Errorf("PubCoord = (%d, %v) after the failed publication, want unchanged (%d, %v)", at, size, pubAt, pubSize)
			}
			if err := st.CoordinationError(); err != nil {
				t.Errorf("a lost race tripped the guards: %v", err)
			}

			all = append(all, addAndIntegrate(t, ctx, seq, a, "four", 10, 5)...)
			if err := publish(); err != nil {
				t.Fatalf("later publication: %v", err)
			}
			if got, want := cpNow(), mustTestCP(40, rootOf(t, all)); !bytes.Equal(got, want) {
				t.Errorf("checkpoint = %q, want %q", got, want)
			}
			if _, size := env.pubCoord(t); !size.Valid || size.Int64 != 40 {
				t.Errorf("PubCoord.size = %v, want 40", size)
			}
			pb, err := client.NewProofBuilder(ctx, 40, a.logStore.ReadTile)
			if err != nil {
				t.Fatalf("NewProofBuilder: %v", err)
			}
			for _, from := range []uint64{10, 30} {
				p, err := pb.ConsistencyProof(ctx, from, 40)
				if err != nil {
					t.Fatalf("ConsistencyProof(%d, 40): %v", from, err)
				}
				if err := proof.VerifyConsistency(rfc6962.DefaultHasher, from, 40, p, rootOf(t, all[:from]), rootOf(t, all)); err != nil {
					t.Errorf("VerifyConsistency(%d, 40): %v", from, err)
				}
			}
		})
	}
}
