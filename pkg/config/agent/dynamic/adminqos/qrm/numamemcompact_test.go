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

	apiconfig "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
)

func TestNumaMemCompactConfigurationApplyConfiguration(t *testing.T) {
	t.Parallel()

	as := require.New(t)
	enableNumaCompact := true
	intervalSeconds := int64(900)

	// A freshly constructed configuration defaults the interval to defaultNumaMemCompactInterval.
	as.Equal(defaultNumaMemCompactInterval, NewNumaMemCompactConfiguration().NumaMemCompactInterval)

	conf := NewNumaMemCompactConfiguration()
	conf.ApplyConfiguration(&crd.DynamicConfigCRD{
		AdminQoSConfiguration: &apiconfig.AdminQoSConfiguration{
			Spec: apiconfig.AdminQoSConfigurationSpec{
				Config: apiconfig.AdminQoSConfig{
					QRMPluginConfig: &apiconfig.QRMPluginConfig{
						MemoryPluginConfig: &apiconfig.MemoryPluginConfig{
							NumaMemCompactConfig: &apiconfig.NumaMemCompactConfig{
								EnableNumaMemCompact:          &enableNumaCompact,
								NumaMemCompactIntervalSeconds: &intervalSeconds,
							},
						},
					},
				},
			},
		},
	})

	as.True(conf.EnableNumaMemCompact)
	as.Equal(900*time.Second, conf.NumaMemCompactInterval)
}

func TestNumaMemCompactConfigurationIntervalClamp(t *testing.T) {
	t.Parallel()

	applyInterval := func(seconds int64) time.Duration {
		conf := NewNumaMemCompactConfiguration()
		conf.ApplyConfiguration(&crd.DynamicConfigCRD{
			AdminQoSConfiguration: &apiconfig.AdminQoSConfiguration{
				Spec: apiconfig.AdminQoSConfigurationSpec{
					Config: apiconfig.AdminQoSConfig{
						QRMPluginConfig: &apiconfig.QRMPluginConfig{
							MemoryPluginConfig: &apiconfig.MemoryPluginConfig{
								NumaMemCompactConfig: &apiconfig.NumaMemCompactConfig{
									NumaMemCompactIntervalSeconds: &seconds,
								},
							},
						},
					},
				},
			},
		})
		return conf.NumaMemCompactInterval
	}

	as := require.New(t)
	// A positive interval below the minimum is clamped up to the minimum.
	as.Equal(minNumaMemCompactInterval, applyInterval(1))
	as.Equal(minNumaMemCompactInterval, applyInterval(599))
	// At or above the minimum is kept as configured.
	as.Equal(minNumaMemCompactInterval, applyInterval(600))
	as.Equal(1200*time.Second, applyInterval(1200))
	// A non-positive interval disables periodic re-compaction and is kept as-is.
	as.Equal(time.Duration(0), applyInterval(0))
	as.Equal(-5*time.Second, applyInterval(-5))
}
