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
	// metricNameMemoryCompact is emitted (with a numa_id tag) whenever the idle-NUMA path
	// actually compacts a NUMA node. It is dedicated to this plugin and intentionally distinct
	// from the fragmem plugin's metric so the two features can be observed separately.
	metricNameMemoryCompact = "numa_memory_compact"

	// metricNameMemoryCompactCost is emitted (with a numa_id tag) after a NUMA node is actually
	// compacted, reporting how long the compaction took in milliseconds.
	metricNameMemoryCompactCost = "numa_memory_compact_cost_ms"

	// metricNameNumaMemCompactEnabled is emitted once per handler cycle to report whether the
	// idle-NUMA compaction feature is currently enabled (value 1) or disabled (value 0) via the
	// dynamic EnableNumaMemCompact switch.
	metricNameNumaMemCompactEnabled = "numa_memory_compact_enabled"

	// metricNameNumaMemCompactError is emitted whenever the handler hits a runtime error, tagged
	// with a "reason" describing what failed.
	metricNameNumaMemCompactError = "numa_memory_compact_error"
)

const (
	// error reasons tagged on metricNameNumaMemCompactError.
	errReasonNilInput           = "nil_input"
	errReasonDynamicConfMissing = "dynamic_config_missing"
	errReasonReadStateFailed    = "read_state_failed"
)
