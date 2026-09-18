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
	"time"

	cliflag "k8s.io/component-base/cli/flag"

	dynamicqrm "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/qrm"
)

type NumaMemCompactOptions struct {
	EnableNumaMemCompact                 bool
	NumaMemCompactInterval               time.Duration
	Order9UnusableIndexDegradedThreshold float64
}

func NewNumaMemCompactOptions() *NumaMemCompactOptions {
	return &NumaMemCompactOptions{
		EnableNumaMemCompact:                 false,
		NumaMemCompactInterval:               1800 * time.Second,
		Order9UnusableIndexDegradedThreshold: 5.0,
	}
}

func (o *NumaMemCompactOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("memory_resource_plugin")
	fs.BoolVar(&o.EnableNumaMemCompact, "qrm-memory-enable-numa-mem-compact",
		o.EnableNumaMemCompact, "if set true, we will proactively compact idle NUMA nodes (those without shared_cores/dedicated_cores pods)")
	fs.DurationVar(&o.NumaMemCompactInterval, "qrm-memory-numa-mem-compact-interval",
		o.NumaMemCompactInterval, "the minimum interval between actual compactions while a NUMA node stays idle; "+
			"after it elapses, order-9 unusable-index degradation is checked each handler cycle and a non-positive value disables subsequent compaction")
	fs.Float64Var(&o.Order9UnusableIndexDegradedThreshold,
		"qrm-memory-numa-mem-compact-order-9-unusable-index-degraded-threshold",
		o.Order9UnusableIndexDegradedThreshold,
		"the minimum increase from the post-compaction order-9 unusable-index baseline that triggers another compaction")
}

func (o *NumaMemCompactOptions) ApplyTo(c *dynamicqrm.NumaMemCompactConfiguration) error {
	c.EnableNumaMemCompact = o.EnableNumaMemCompact
	// Apply the same guard as the AdminQoSConfiguration path so a misconfigured static flag cannot
	// cause an idle NUMA node to be compacted too frequently.
	c.NumaMemCompactInterval = dynamicqrm.ClampNumaMemCompactInterval(o.NumaMemCompactInterval)
	c.Order9UnusableIndexDegradedThreshold = dynamicqrm.ClampOrder9UnusableIndexDegradedThreshold(
		o.Order9UnusableIndexDegradedThreshold)
	return nil
}
