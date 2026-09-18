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
	"math"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
)

type NumaMemCompactConfiguration struct {
	// EnableNumaMemCompact gates proactive compaction so that only NUMA nodes without online
	// business (shared_cores/dedicated_cores) pods are compacted. The idle-NUMA path is
	// edge-triggered: a NUMA node is compacted once right after its business pods leave. It is
	// dynamically configurable (per machine-type via AdminQoSConfiguration nodeLabelSelector) and
	// is independent of the fragmem feature, so both may take effect simultaneously.
	EnableNumaMemCompact bool

	// NumaMemCompactInterval is the minimum interval between two actual compactions while a NUMA
	// node stays idle. Once the interval has elapsed, order-9 readiness is checked on each handler
	// cycle and the node is compacted only when its unusable-index increase from the
	// post-compaction baseline exceeds the threshold. A non-positive value disables subsequent
	// compaction until a business pod is scheduled onto the node and leaves again. Defaults to
	// defaultNumaMemCompactInterval.
	NumaMemCompactInterval time.Duration

	// Order9UnusableIndexDegradedThreshold is the minimum increase from the post-compaction order-9
	// unusable-index baseline that triggers another compaction after NumaMemCompactInterval.
	// Defaults to defaultOrder9UnusableIndexDegradedThreshold.
	Order9UnusableIndexDegradedThreshold float64
}

// defaultNumaMemCompactInterval is the default minimum interval between two actual compactions for
// a NUMA node that stays idle. It is used unless overridden by AdminQoSConfiguration.
const defaultNumaMemCompactInterval = 1800 * time.Second

// minNumaMemCompactInterval is the lower bound for a positive compaction interval. A positive
// configured interval smaller than this is clamped up to it, to guard against a misconfiguration
// (e.g. a few seconds) that would cause an idle NUMA node to be compacted too frequently. A
// non-positive interval is left as-is since it disables subsequent compaction.
const minNumaMemCompactInterval = 600 * time.Second

const (
	defaultOrder9UnusableIndexDegradedThreshold = 5.0
	minOrder9UnusableIndexDegradedThreshold     = 0.0
	maxOrder9UnusableIndexDegradedThreshold     = 100.0
)

// ClampNumaMemCompactInterval guards against a misconfigured compaction interval. A positive but
// too-small interval is clamped up to minNumaMemCompactInterval so an idle NUMA node is not
// compacted too frequently. A non-positive interval is returned as-is since it disables subsequent
// compaction. It is shared by the static-flag and AdminQoSConfiguration paths so both apply the
// same guard.
func ClampNumaMemCompactInterval(interval time.Duration) time.Duration {
	if interval > 0 && interval < minNumaMemCompactInterval {
		return minNumaMemCompactInterval
	}
	return interval
}

// ClampOrder9UnusableIndexDegradedThreshold keeps the threshold within the valid unusable-index
// range. NaN falls back to the default value.
func ClampOrder9UnusableIndexDegradedThreshold(threshold float64) float64 {
	if math.IsNaN(threshold) {
		return defaultOrder9UnusableIndexDegradedThreshold
	}
	if threshold < minOrder9UnusableIndexDegradedThreshold {
		return minOrder9UnusableIndexDegradedThreshold
	}
	if threshold > maxOrder9UnusableIndexDegradedThreshold {
		return maxOrder9UnusableIndexDegradedThreshold
	}
	return threshold
}

func NewNumaMemCompactConfiguration() *NumaMemCompactConfiguration {
	return &NumaMemCompactConfiguration{
		NumaMemCompactInterval:               defaultNumaMemCompactInterval,
		Order9UnusableIndexDegradedThreshold: defaultOrder9UnusableIndexDegradedThreshold,
	}
}

func (c *NumaMemCompactConfiguration) ApplyConfiguration(conf *crd.DynamicConfigCRD) {
	if aqc := conf.AdminQoSConfiguration; aqc != nil &&
		aqc.Spec.Config.QRMPluginConfig != nil &&
		aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig != nil &&
		aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig.NumaMemCompactConfig != nil {
		config := aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig.NumaMemCompactConfig
		if config.EnableNumaMemCompact != nil {
			c.EnableNumaMemCompact = *config.EnableNumaMemCompact
		}
		if config.NumaMemCompactIntervalSeconds != nil {
			c.NumaMemCompactInterval = ClampNumaMemCompactInterval(
				time.Duration(*config.NumaMemCompactIntervalSeconds) * time.Second)
		}
		if config.Order9UnusableIndexDegradedThreshold != nil {
			c.Order9UnusableIndexDegradedThreshold = ClampOrder9UnusableIndexDegradedThreshold(
				*config.Order9UnusableIndexDegradedThreshold)
		}
	}
}
