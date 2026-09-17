//go:build linux
// +build linux

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

import (
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/errors"

	memconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/handlers/compaction"
	coreconfig "github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	dynamicqrm "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/qrm"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

var (
	// numaLastCompact records, per NUMA node, the time of its most recent idle compaction. A NUMA
	// node absent from the map has not been compacted since it last became idle (or has never been
	// observed idle), so it is eligible for the falling-edge compaction. Once compacted, the entry
	// gates periodic re-compaction while the node stays idle (see doNumaMemCompact). When a business
	// pod is present on a node, its entry is removed so the next idle period triggers a fresh
	// falling-edge compaction.
	numaLastCompact   = make(map[int]time.Time)
	numaLastCompactMu sync.RWMutex

	// nowFn returns the current time. It is a variable so tests can control the clock.
	nowFn = time.Now
)

// getNumaLastCompact returns the last idle-compaction time of numaID and whether it has one.
func getNumaLastCompact(numaID int) (time.Time, bool) {
	numaLastCompactMu.RLock()
	defer numaLastCompactMu.RUnlock()
	t, ok := numaLastCompact[numaID]
	return t, ok
}

// setNumaLastCompact records the last idle-compaction time of numaID.
func setNumaLastCompact(numaID int, t time.Time) {
	numaLastCompactMu.Lock()
	defer numaLastCompactMu.Unlock()
	numaLastCompact[numaID] = t
}

// clearNumaLastCompact drops the idle-compaction record of numaID (called when a business pod is
// present), so the next idle period triggers a fresh falling-edge compaction.
func clearNumaLastCompact(numaID int) {
	numaLastCompactMu.Lock()
	defer numaLastCompactMu.Unlock()
	delete(numaLastCompact, numaID)
}

// emitNumaMemCompactError emits a runtime-error metric tagged with the given reason. It is a no-op
// when the emitter is nil.
func emitNumaMemCompactError(emitter metrics.MetricEmitter, reason string) {
	if emitter == nil {
		return
	}
	_ = emitter.StoreInt64(metricNameNumaMemCompactError, 1, metrics.MetricTypeNameRaw,
		metrics.MetricTag{Key: "reason", Val: reason})
}

// getNumaMemCompactConfiguration returns the dynamic NumaMemCompactConfiguration, or nil if the
// dynamic configuration is not available.
func getNumaMemCompactConfiguration(dynamicConf *dynamicconfig.DynamicAgentConfiguration) *dynamicqrm.NumaMemCompactConfiguration {
	if dynamicConf == nil {
		return nil
	}
	conf := dynamicConf.GetDynamicConfiguration()
	if conf == nil {
		return nil
	}
	return conf.NumaMemCompactConfiguration
}

// getMemoryNUMAState returns the memory NUMANodeMap from the memory plugin's readonly state,
// together with whether it was read successfully. It is fetched once per scan cycle (not per NUMA)
// to avoid repeatedly deep-copying the whole machine state.
func getMemoryNUMAState() (state.NUMANodeMap, bool) {
	readonlyState, err := state.GetReadonlyState()
	if err != nil || readonlyState == nil {
		general.Errorf("failed to get readonly memory state: %v", err)
		return nil, false
	}
	return readonlyState.GetMachineState()[v1.ResourceMemory], true
}

// numaHasOnlineBusinessPods reports whether the given NUMA node currently hosts any online
// business pod (shared_cores/dedicated_cores) according to the provided memory NUMANodeMap.
// When the state was not read successfully (stateOK is false), it conservatively returns true so
// that a NUMA node possibly carrying business pods is not compacted.
func numaHasOnlineBusinessPods(machineState state.NUMANodeMap, stateOK bool, numaID int) bool {
	if !stateOK {
		return true
	}
	return machineState[numaID].HasSharedOrDedicatedPods()
}

// doNumaMemCompact proactively compacts memory only on NUMA nodes that carry no online
// business (shared_cores/dedicated_cores) pods. It does not consult the fragmentation score.
//
// The policy is edge-triggered with optional periodic re-compaction: a NUMA node is compacted once
// right after it becomes idle (falling edge). If interval > 0, a still-idle NUMA node is compacted
// again every interval; if interval <= 0, it is compacted only once until a business pod is
// scheduled onto it and leaves again. Per-NUMA last-compaction time is tracked in numaLastCompact;
// the entry is cleared when a business pod is present. The memory NUMA state is fetched once per
// cycle (it is a deep copy) rather than per NUMA node.
func doNumaMemCompact(metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter, interval time.Duration) {
	machineState, stateOK := getMemoryNUMAState()
	if !stateOK {
		// The readonly memory state is unavailable this cycle; every NUMA node is treated as busy
		// below, so nothing is compacted. Surface it as a runtime error for observability.
		emitNumaMemCompactError(emitter, errReasonReadStateFailed)
	}

	for _, numaID := range metaServer.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		// A NUMA node hosting online business pods is dirtied: drop its compaction record so
		// the next idle period triggers a fresh falling-edge compaction, and never compact here.
		if numaHasOnlineBusinessPods(machineState, stateOK, numaID) {
			general.Infof("skip NUMA %d: online business pods present", numaID)
			clearNumaLastCompact(numaID)
			continue
		}

		// The NUMA node is idle now. If it was already compacted while idle, only
		// re-compact when periodic re-compaction is enabled (interval > 0) and the interval has
		// elapsed; otherwise there is nothing new to do, skip it.
		if last, ok := getNumaLastCompact(numaID); ok {
			if interval <= 0 || nowFn().Sub(last) < interval {
				continue
			}
		}

		// Compact this NUMA node unless its own kcompactd is busy (R/D state). Emit the
		// per-compaction metric (tagged with the numa_id) and record the time only when a compaction
		// actually happened, so that while the node stays idle it is not compacted again until the
		// next interval elapses. Also emit how long the compaction took.
		start := nowFn()
		if compaction.TryCompactNUMANode(numaID) {
			costMs := nowFn().Sub(start).Milliseconds()
			numaIDTag := metrics.MetricTag{Key: "numa_id", Val: strconv.Itoa(numaID)}
			_ = emitter.StoreInt64(metricNameMemoryCompact, 1, metrics.MetricTypeNameRaw, numaIDTag)
			_ = emitter.StoreInt64(metricNameMemoryCompactCost, costMs, metrics.MetricTypeNameRaw, numaIDTag)
			setNumaLastCompact(numaID, nowFn())
			general.Infof("NUMA %d memory compaction completed, cost=%dms", numaID, costMs)
		}
	}
}

// NumaMemCompact is the periodical handler that proactively compacts memory on idle NUMA nodes.
// It is a standalone feature, independent of the fragmem handler, and is gated only by the
// dynamically configured EnableNumaMemCompact switch (per machine-type via AdminQoSConfiguration).
func NumaMemCompact(conf *coreconfig.Configuration,
	_ interface{}, dynamicConf *dynamicconfig.DynamicAgentConfiguration,
	emitter metrics.MetricEmitter, metaServer *metaserver.MetaServer,
) {
	general.Infof("NumaMemCompact was called")

	var errList []error
	defer func() {
		_ = general.UpdateHealthzStateByError(memconsts.NumaMemCompact, errors.NewAggregate(errList))
	}()

	if conf == nil || emitter == nil || metaServer == nil {
		general.Errorf("nil input, conf:%v, emitter:%v, metaServer:%v", conf, emitter, metaServer)
		emitNumaMemCompactError(emitter, errReasonNilInput)
		return
	}

	numaMemCompactConf := getNumaMemCompactConfiguration(dynamicConf)
	if numaMemCompactConf == nil {
		general.Infof("dynamic numa mem compact configuration not found")
		emitNumaMemCompactError(emitter, errReasonDynamicConfMissing)
		return
	}

	// Report whether the feature is currently enabled so it can be observed regardless of the
	// switch state.
	enabledValue := int64(0)
	if numaMemCompactConf.EnableNumaMemCompact {
		enabledValue = 1
	}
	_ = emitter.StoreInt64(metricNameNumaMemCompactEnabled, enabledValue, metrics.MetricTypeNameRaw)

	if !numaMemCompactConf.EnableNumaMemCompact {
		general.Infof("NumaMemCompact skipped: EnableNumaMemCompact disabled")
		return
	}

	doNumaMemCompact(metaServer, emitter, numaMemCompactConf.NumaMemCompactInterval)
}
