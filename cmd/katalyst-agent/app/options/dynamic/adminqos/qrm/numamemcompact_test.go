/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package qrm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	dynamicqrm "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/qrm"
)

func TestNumaMemCompactOptionsDefaultsAndFlags(t *testing.T) {
	t.Parallel()

	as := require.New(t)
	options := NewNumaMemCompactOptions()
	as.False(options.EnableNumaMemCompact)
	as.Equal(1800*time.Second, options.NumaMemCompactInterval)

	fss := cliflag.NamedFlagSets{}
	options.AddFlags(&fss)
	fs := fss.FlagSet("memory_resource_plugin")
	as.NotNil(fs.Lookup("qrm-memory-enable-numa-mem-compact"))
	as.NotNil(fs.Lookup("qrm-memory-numa-mem-compact-interval"))
	as.NoError(fs.Parse([]string{
		"--qrm-memory-enable-numa-mem-compact=true",
		"--qrm-memory-numa-mem-compact-interval=3600s",
	}))

	conf := dynamicqrm.NewNumaMemCompactConfiguration()
	as.NoError(options.ApplyTo(conf))
	as.True(conf.EnableNumaMemCompact)
	as.Equal(3600*time.Second, conf.NumaMemCompactInterval)
}

func TestNumaMemCompactOptionsClampAndDisable(t *testing.T) {
	t.Parallel()

	as := require.New(t)

	// A positive but too-small interval is clamped up to the minimum.
	options := NewNumaMemCompactOptions()
	options.NumaMemCompactInterval = 5 * time.Second
	conf := dynamicqrm.NewNumaMemCompactConfiguration()
	as.NoError(options.ApplyTo(conf))
	as.Equal(600*time.Second, conf.NumaMemCompactInterval)

	// A non-positive interval disables periodic re-compaction and is kept as-is.
	options = NewNumaMemCompactOptions()
	options.NumaMemCompactInterval = 0
	conf = dynamicqrm.NewNumaMemCompactConfiguration()
	as.NoError(options.ApplyTo(conf))
	as.Equal(time.Duration(0), conf.NumaMemCompactInterval)
}
