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
	"fmt"
	"strings"

	apiconfig "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
)

type THPDefragPolicy string

const (
	THPDefragPolicyAlways       THPDefragPolicy = "always"
	THPDefragPolicyDefer        THPDefragPolicy = "defer"
	THPDefragPolicyDeferMadvise THPDefragPolicy = "defer+madvise"
	THPDefragPolicyMadvise      THPDefragPolicy = "madvise"
	THPDefragPolicyNever        THPDefragPolicy = "never"
)

// ParseTHPDefragPolicy normalizes and validates a THP defrag policy.
// An empty policy uses the default, defer.
func ParseTHPDefragPolicy(policy string) (THPDefragPolicy, error) {
	normalizedPolicy := THPDefragPolicy(strings.TrimSpace(strings.ToLower(policy)))
	if normalizedPolicy == "" {
		return THPDefragPolicyDefer, nil
	}

	switch normalizedPolicy {
	case THPDefragPolicyAlways, THPDefragPolicyDefer, THPDefragPolicyDeferMadvise,
		THPDefragPolicyMadvise, THPDefragPolicyNever:
		return normalizedPolicy, nil
	default:
		return "", fmt.Errorf("invalid THP defrag policy %q, expected one of %q/%q/%q/%q/%q", policy,
			THPDefragPolicyAlways, THPDefragPolicyDefer, THPDefragPolicyDeferMadvise,
			THPDefragPolicyMadvise, THPDefragPolicyNever)
	}
}

type FragMemConfiguration struct {
	EnableFragMem              bool
	MemFragScoreAsync          int
	THPDefaultConfig           apiconfig.THPMode
	THPHighOrderScoreThreshold int
	THPStaticEnableConfig      *THPStaticEnableConfiguration
	// THPDefragPolicy is applied whenever EnableFragMem is true, independently of THP mode tuning.
	THPDefragPolicy THPDefragPolicy
}

type THPStaticEnableConfiguration struct {
	Enable bool
	THP    apiconfig.THPMode
	THPShm apiconfig.THPShmMode
}

func NewFragMemConfiguration() *FragMemConfiguration {
	return &FragMemConfiguration{}
}

func (c *FragMemConfiguration) ApplyConfiguration(conf *crd.DynamicConfigCRD) {
	if aqc := conf.AdminQoSConfiguration; aqc != nil &&
		aqc.Spec.Config.QRMPluginConfig != nil &&
		aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig != nil &&
		aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig.FragMemConfig != nil {
		config := aqc.Spec.Config.QRMPluginConfig.MemoryPluginConfig.FragMemConfig
		if config.EnableFragMem != nil {
			c.EnableFragMem = *config.EnableFragMem
		}
		if config.MemFragScoreAsync != nil {
			c.MemFragScoreAsync = int(*config.MemFragScoreAsync)
		}
		if config.THPDefaultConfig != nil {
			c.THPDefaultConfig = apiconfig.THPMode(strings.TrimSpace(strings.ToLower(*config.THPDefaultConfig)))
		}
		if config.THPHighOrderScoreThreshold != nil {
			c.THPHighOrderScoreThreshold = int(*config.THPHighOrderScoreThreshold)
		}
		if config.THPStaticEnableConfig != nil {
			c.THPStaticEnableConfig = &THPStaticEnableConfiguration{}
			if config.THPStaticEnableConfig.Enable != nil {
				c.THPStaticEnableConfig.Enable = *config.THPStaticEnableConfig.Enable
			}
			if config.THPStaticEnableConfig.THP != nil {
				c.THPStaticEnableConfig.THP = apiconfig.THPMode(strings.TrimSpace(strings.ToLower(string(*config.THPStaticEnableConfig.THP))))
			}
			if config.THPStaticEnableConfig.THPShm != nil {
				c.THPStaticEnableConfig.THPShm = apiconfig.THPShmMode(strings.TrimSpace(strings.ToLower(string(*config.THPStaticEnableConfig.THPShm))))
			}
		} else {
			c.THPStaticEnableConfig = nil
		}
	}
}
