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
	"slices"
	"testing"
)

func TestAdditionalSignerKeys(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flagKeys []string
		env      string
		want     []string
	}{
		{name: "none", want: []string{}},
		{name: "flags only", flagKeys: []string{"a", "b"}, want: []string{"a", "b"}},
		{name: "env only", env: "a", want: []string{"a"}},
		{name: "env appended to flags", flagKeys: []string{"f"}, env: "a,b", want: []string{"f", "a", "b"}},
		{name: "newlines", env: "a\nb\r\nc\n", want: []string{"a", "b", "c"}},
		{name: "mixed separators, whitespace and empty entries", env: "\n  a , \n,,\tb\t\n , c,\n", want: []string{"a", "b", "c"}},
		{name: "whitespace only", flagKeys: []string{"f"}, env: " \n , \t", want: []string{"f"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := additionalSignerKeys(tc.flagKeys, tc.env); !slices.Equal(got, tc.want) {
				t.Errorf("additionalSignerKeys(%q, %q) = %q, want %q", tc.flagKeys, tc.env, got, tc.want)
			}
		})
	}
}
