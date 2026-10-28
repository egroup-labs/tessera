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

import (
	"context"
	"errors"
	"testing"

	"github.com/transparency-dev/tessera/api/layout"
)

// Minor: updateCheckpoint wraps the objStore error with %v ("writeCheckpoint: %v"), so callers
// can't tell a lost race (errPreconditionFailed, documented on objStore.setObjectIfETag) from any
// other write failure with errors.Is.
func TestUpdateCheckpointPreservesPreconditionFailed(t *testing.T) {
	ctx := t.Context()
	m := newMemObjStore()
	spy := &spyStore{objStore: m}
	a, _ := newBareAppender(spy, nil, layout.EntriesPath)
	spy.afterCPRead = func(ctx context.Context) {
		_ = m.setObject(ctx, layout.CheckpointPath, mustTestCP(1, []byte("another publisher's checkpoint")), "", "")
	}
	if err := a.updateCheckpoint(ctx, 0, nil); !errors.Is(err, errPreconditionFailed) {
		t.Errorf("updateCheckpoint racing another publisher = %v, want errors.Is(err, errPreconditionFailed)", err)
	}
}
