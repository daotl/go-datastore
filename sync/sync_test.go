// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016] as
// part of the original go-datastore project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package sync

import (
	"testing"

	dstest "github.com/bdware/go-datastore/test"
)

func TestSync(t *testing.T) {
	dstest.SubtestAll(t, MutexWrap(dstest.NewMapDatastoreForTest(t)))
}
