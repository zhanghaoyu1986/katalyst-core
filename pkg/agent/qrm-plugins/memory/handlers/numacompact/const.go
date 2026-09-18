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

package numacompact

const (
	// numaMemCompactTHPOrder is the buddy allocation order for a 2 MiB THP with 4 KiB base pages.
	numaMemCompactTHPOrder = 9

	// invalidUnusableIndex marks a compaction state whose post-compaction order-9 unusable index
	// could not be read. Valid unusable-index values are in the range [0, 100].
	invalidUnusableIndex float64 = -1
)

const (
	// metricNameMemoryCompact is emitted (with a numa_id tag) whenever the idle-NUMA path
	// actually compacts a NUMA node. It is dedicated to this plugin and intentionally distinct
	// from the fragmem plugin's metric so the two features can be observed separately.
	metricNameMemoryCompact = "numa_memory_compact"

	// metricNameMemoryCompactCost is emitted (with a numa_id tag) after a NUMA node is actually
	// compacted, reporting how long the compaction took in milliseconds.
	metricNameMemoryCompactCost = "numa_memory_compact_cost_ms"

	// metricNameNumaMemCompactOrder9UnusableIndexDiff reports the pre-compaction minus
	// post-compaction order-9 unusable index, tagged with both observed values.
	metricNameNumaMemCompactOrder9UnusableIndexDiff = "numa_memory_compact_order_9_unusable_index_diff"

	// metricNameNumaMemCompactOrder9PlusFreeSizeDiffBytes reports the increase in free memory held
	// by order-9-or-higher buddy blocks after compaction, tagged with the pre/post sizes in bytes.
	metricNameNumaMemCompactOrder9PlusFreeSizeDiffBytes = "numa_memory_compact_order_9_plus_free_size_diff_bytes"

	// metricNameNumaMemCompactEnabled is emitted once per handler cycle to report whether the
	// idle-NUMA compaction feature is currently enabled (value 1) or disabled (value 0) via the
	// dynamic EnableNumaMemCompact switch.
	metricNameNumaMemCompactEnabled = "numa_memory_compact_enabled"

	// metricNameNumaMemCompactOrder9UnusableIndexDegraded reports whether the current order-9
	// unusable index exceeds the allowed delta from the post-compaction baseline.
	metricNameNumaMemCompactOrder9UnusableIndexDegraded = "numa_memory_compact_order_9_unusable_index_degraded"

	// metricNameNumaMemCompactError is emitted whenever the handler hits a runtime error, tagged
	// with a "reason" describing what failed.
	metricNameNumaMemCompactError = "numa_memory_compact_error"

	// metricNameNumaMemCompactTaskDurationSeconds reports how long the current background scan has
	// been running, tagged with ongoing=true.
	metricNameNumaMemCompactTaskDurationSeconds = "numa_memory_compact_task_duration_seconds"
)

const (
	// error reasons tagged on metricNameNumaMemCompactError.
	errReasonNilInput                = "nil_input"
	errReasonDynamicConfMissing      = "dynamic_config_missing"
	errReasonReadStateFailed         = "read_state_failed"
	errReasonReadOrder9UnusableIndex = "read_order_9_unusable_index_failed"
	errReasonReadOrder9PlusFreeSize  = "read_order_9_plus_free_size_failed"
)
