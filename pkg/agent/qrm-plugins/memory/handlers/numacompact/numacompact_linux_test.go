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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/handlers/compaction"
	coreconfig "github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	metaagent "github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func makeNumaMemCompactCoreConf(enable bool) (*coreconfig.Configuration, *dynamicconfig.DynamicAgentConfiguration) {
	dynamicConf := dynamicconfig.NewDynamicAgentConfiguration()
	dynamicConf.GetDynamicConfiguration().NumaMemCompactConfiguration.EnableNumaMemCompact = enable

	return &coreconfig.Configuration{
		AgentConfiguration: &agent.AgentConfiguration{
			DynamicAgentConfiguration: dynamicConf,
		},
	}, dynamicConf
}

func makeMetaServer() (*metaserver.MetaServer, error) {
	server := &metaserver.MetaServer{
		MetaAgent: &metaagent.MetaAgent{},
	}

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	if err != nil {
		return nil, err
	}

	server.KatalystMachineInfo = &machine.KatalystMachineInfo{
		CPUTopology: cpuTopology,
	}
	server.MetricsFetcher = metric.NewFakeMetricsFetcher(metrics.DummyMetrics{})
	return server, nil
}

// allocationInfoForQoS builds an AllocationInfo with the given QoS level bound to a NUMA node.
func allocationInfoForQoS(qosLevel string, numaID int) *state.AllocationInfo {
	return &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "pod-" + qosLevel,
			PodName:       "pod-" + qosLevel,
			ContainerName: "container",
			ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel:      qosLevel,
			Annotations: map[string]string{
				apiconsts.PodAnnotationQoSLevelKey: qosLevel,
			},
		},
		NumaAllocationResult: machine.NewCPUSet(numaID),
	}
}

// setReadonlyStateWithPods installs a readonly state where each entry maps a NUMA id to the QoS
// level of a pod occupying it. An empty QoS string leaves that NUMA node idle.
func setReadonlyStateWithPods(numaToQoS map[int]string) {
	machineState := state.NUMANodeResourcesMap{
		v1.ResourceMemory: state.NUMANodeMap{},
	}
	for numaID, qosLevel := range numaToQoS {
		ns := &state.NUMANodeState{}
		if qosLevel != "" {
			ns.SetAllocationInfo(allocationInfoForQoS(qosLevel, numaID).PodUid, "container",
				allocationInfoForQoS(qosLevel, numaID))
		}
		machineState[v1.ResourceMemory][numaID] = ns
	}
	state.SetReadonlyState(&fakeReadonlyState{machineState: machineState})
}

// fakeReadonlyState is a minimal ReadonlyState that only serves a fixed machine state.
type fakeReadonlyState struct {
	state.ReadonlyState
	machineState state.NUMANodeResourcesMap
}

func (f *fakeReadonlyState) GetMachineState() state.NUMANodeResourcesMap {
	return f.machineState
}

func resetNumaCompactState() {
	numaLastCompactMu.Lock()
	defer numaLastCompactMu.Unlock()
	numaLastCompact = make(map[int]time.Time)
}

// isNumaCompacted reports whether numaID currently has a recorded idle-compaction time.
func isNumaCompacted(numaID int) bool {
	_, ok := getNumaLastCompact(numaID)
	return ok
}

// makeNUMANodeMap builds a memory NUMANodeMap where each entry maps a NUMA id to the QoS level of
// a pod occupying it. An empty QoS string leaves that NUMA node idle.
func makeNUMANodeMap(numaToQoS map[int]string) state.NUMANodeMap {
	nm := state.NUMANodeMap{}
	for numaID, qosLevel := range numaToQoS {
		ns := &state.NUMANodeState{}
		if qosLevel != "" {
			ns.SetAllocationInfo(allocationInfoForQoS(qosLevel, numaID).PodUid, "container",
				allocationInfoForQoS(qosLevel, numaID))
		}
		nm[numaID] = ns
	}
	return nm
}

// withRecordingCompactFn swaps compaction.CompactMemoryNodeFn for one that records the NUMA ids it
// is called with, restoring it afterwards. It also isolates compaction.ProcFSRoot to an empty temp
// dir (and resets the kcompactd pid cache) so the per-NUMA state check never consults the host
// /proc and always treats kcompactd as not busy. The returned pointer accumulates the compacted ids.
func withRecordingCompactFn(t *testing.T) *[]int {
	t.Helper()
	oldFn := compaction.CompactMemoryNodeFn
	var compacted []int
	compaction.CompactMemoryNodeFn = func(numaID int) {
		compacted = append(compacted, numaID)
	}

	oldRoot := compaction.ProcFSRoot
	compaction.ProcFSRoot = t.TempDir()
	compaction.ResetKcompactdPidsForTest()

	t.Cleanup(func() {
		compaction.CompactMemoryNodeFn = oldFn
		compaction.ProcFSRoot = oldRoot
		compaction.ResetKcompactdPidsForTest()
	})
	return &compacted
}

func TestNumaHasOnlineBusinessPods(t *testing.T) {
	t.Parallel()

	machineState := makeNUMANodeMap(map[int]string{
		0: apiconsts.PodAnnotationQoSLevelSharedCores,
		1: apiconsts.PodAnnotationQoSLevelReclaimedCores,
		2: apiconsts.PodAnnotationQoSLevelSystemCores,
		3: "",
	})

	assert.True(t, numaHasOnlineBusinessPods(machineState, true, 0))  // shared_cores => business
	assert.False(t, numaHasOnlineBusinessPods(machineState, true, 1)) // reclaimed_cores => not business
	assert.False(t, numaHasOnlineBusinessPods(machineState, true, 2)) // system_cores => not business
	assert.False(t, numaHasOnlineBusinessPods(machineState, true, 3)) // idle

	// When the state was not read successfully, conservatively treat the NUMA node as busy.
	assert.True(t, numaHasOnlineBusinessPods(nil, false, 3))
}

func TestNumaMemCompactSkipsBusinessPods(t *testing.T) {
	// mutates package-level numaLastCompact and readonly state, so do not run in parallel.
	resetNumaCompactState()
	compacted := withRecordingCompactFn(t)

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)

	// NUMA 0 has an online business pod, NUMA 1 is idle.
	setReadonlyStateWithPods(map[int]string{
		0: apiconsts.PodAnnotationQoSLevelSharedCores,
		1: "",
	})

	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, 0)

	// Only NUMA 1 (idle) is compacted; NUMA 0 (business) has no compaction record.
	assert.Equal(t, []int{1}, *compacted)
	assert.False(t, isNumaCompacted(0))
	assert.True(t, isNumaCompacted(1))
}

func TestNumaMemCompactEdgeTriggered(t *testing.T) {
	// mutates package-level numaLastCompact and readonly state, so do not run in parallel.
	resetNumaCompactState()
	compacted := withRecordingCompactFn(t)

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)

	// interval = 0 disables periodic re-compaction: pure falling-edge behavior.
	const interval = 0

	// Cycle 1: NUMA 0 idle from the start -> compacted once; NUMA 1 busy -> skipped.
	setReadonlyStateWithPods(map[int]string{
		0: "",
		1: apiconsts.PodAnnotationQoSLevelSharedCores,
	})
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0}, *compacted)

	// Cycle 2: both idle now. NUMA 0 already compacted while idle -> not compacted again;
	// NUMA 1 just became idle after hosting business -> compacted once (the falling edge).
	setReadonlyStateWithPods(map[int]string{0: "", 1: ""})
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 1}, *compacted)

	// Cycle 3: both still idle -> nothing compacted (interval disabled).
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 1}, *compacted)

	// Cycle 4: a business pod is scheduled onto NUMA 0, dropping its record -> not compacted while busy.
	setReadonlyStateWithPods(map[int]string{0: apiconsts.PodAnnotationQoSLevelDedicatedCores, 1: ""})
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 1}, *compacted)
	assert.False(t, isNumaCompacted(0))

	// Cycle 5: the business pod leaves NUMA 0 -> compacted once more on the new falling edge.
	setReadonlyStateWithPods(map[int]string{0: "", 1: ""})
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 1, 0}, *compacted)
}

func TestNumaMemCompactPeriodic(t *testing.T) {
	// mutates package-level numaLastCompact/nowFn and readonly state, so do not run in parallel.
	resetNumaCompactState()
	compacted := withRecordingCompactFn(t)

	// Control the clock.
	base := time.Unix(1_000_000, 0)
	now := base
	oldNow := nowFn
	nowFn = func() time.Time { return now }
	t.Cleanup(func() { nowFn = oldNow })

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)

	const interval = 5 * time.Minute

	// NUMA 0 idle throughout; NUMA 1 busy throughout.
	setReadonlyStateWithPods(map[int]string{
		0: "",
		1: apiconsts.PodAnnotationQoSLevelSharedCores,
	})

	// t=0: falling-edge compaction of NUMA 0.
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0}, *compacted)

	// t=+2m (< interval): not re-compacted.
	now = base.Add(2 * time.Minute)
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0}, *compacted)

	// t=+5m (== interval): re-compacted.
	now = base.Add(5 * time.Minute)
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 0}, *compacted)

	// t=+9m (< interval since last at +5m): not re-compacted.
	now = base.Add(9 * time.Minute)
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 0}, *compacted)

	// t=+10m (interval since last at +5m): re-compacted.
	now = base.Add(10 * time.Minute)
	doNumaMemCompact(metaServer, metrics.DummyMetrics{}, interval)
	assert.Equal(t, []int{0, 0, 0}, *compacted)
}

func TestNumaMemCompact(t *testing.T) {
	resetNumaCompactState()

	// nil inputs are tolerated.
	conf, dynamicConf := makeNumaMemCompactCoreConf(false)
	NumaMemCompact(conf, nil, dynamicConf, nil, nil)
	NumaMemCompact(conf, nil, dynamicConf, metrics.DummyMetrics{}, nil)

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)
	metaServer.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{}}

	// disabled: no NUMA node is compacted.
	compacted := withRecordingCompactFn(t)
	setReadonlyStateWithPods(map[int]string{0: "", 1: ""})
	conf, dynamicConf = makeNumaMemCompactCoreConf(false)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, metrics.DummyMetrics{}, metaServer)
	assert.Empty(t, *compacted)

	// enabled: idle NUMA nodes are compacted once.
	conf, dynamicConf = makeNumaMemCompactCoreConf(true)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, metrics.DummyMetrics{}, metaServer)
	assert.ElementsMatch(t, []int{0, 1}, *compacted)
}

func TestSetHostMemCompact(t *testing.T) {
	t.Parallel()
	compaction.CompactMemoryNodeFn(25535)
}

// recordingEmitter is a metrics.MetricEmitter that records the last int64 value stored per key, and
// counts how many times each (key, reason-tag) pair was stored.
type recordingEmitter struct {
	metrics.DummyMetrics
	values       map[string]int64
	reasonCounts map[string]map[string]int
}

func newRecordingEmitter() *recordingEmitter {
	return &recordingEmitter{
		values:       make(map[string]int64),
		reasonCounts: make(map[string]map[string]int),
	}
}

func (e *recordingEmitter) StoreInt64(key string, val int64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.values[key] = val
	for _, tag := range tags {
		if tag.Key != "reason" {
			continue
		}
		if e.reasonCounts[key] == nil {
			e.reasonCounts[key] = make(map[string]int)
		}
		e.reasonCounts[key][tag.Val]++
	}
	return nil
}

func TestNumaMemCompactEnabledMetric(t *testing.T) {
	resetNumaCompactState()
	withRecordingCompactFn(t)

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)
	metaServer.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{}}
	setReadonlyStateWithPods(map[int]string{0: "", 1: ""})

	// disabled -> enabled metric is 0.
	emitter := newRecordingEmitter()
	conf, dynamicConf := makeNumaMemCompactCoreConf(false)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, emitter, metaServer)
	assert.Equal(t, int64(0), emitter.values[metricNameNumaMemCompactEnabled])

	// enabled -> enabled metric is 1.
	emitter = newRecordingEmitter()
	conf, dynamicConf = makeNumaMemCompactCoreConf(true)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, emitter, metaServer)
	assert.Equal(t, int64(1), emitter.values[metricNameNumaMemCompactEnabled])
}

func TestNumaMemCompactErrorMetric(t *testing.T) {
	resetNumaCompactState()

	metaServer, err := makeMetaServer()
	assert.NoError(t, err)
	metaServer.PodFetcher = &pod.PodFetcherStub{PodList: []*v1.Pod{}}

	// nil metaServer -> nil_input error emitted (emitter is non-nil).
	emitter := newRecordingEmitter()
	conf, dynamicConf := makeNumaMemCompactCoreConf(true)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, emitter, nil)
	assert.Equal(t, 1, emitter.reasonCounts[metricNameNumaMemCompactError][errReasonNilInput])

	// read-state failure -> read_state_failed error emitted. Clearing the readonly state makes
	// getMemoryNUMAState report stateOK=false.
	state.SetReadonlyState(nil)
	emitter = newRecordingEmitter()
	withRecordingCompactFn(t)
	NumaMemCompact(conf, metrics.DummyMetrics{}, dynamicConf, emitter, metaServer)
	assert.Equal(t, 1, emitter.reasonCounts[metricNameNumaMemCompactError][errReasonReadStateFailed])
}
