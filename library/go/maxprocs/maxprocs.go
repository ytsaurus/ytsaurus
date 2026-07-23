package maxprocs

import (
	"runtime"
	"strconv"
	"strings"

	"go.ytsaurus.tech/library/go/yandex/deploy/podagent"
	"go.ytsaurus.tech/library/go/yandex/yplite"
)

const (
	SafeProc = 4
	MinProc  = 2

	GoMaxProcEnvName          = "GOMAXPROCS"
	QloudCPUEnvName           = "QLOUD_CPU_GUARANTEE"
	InstancectlCPUEnvName     = "CPU_GUARANTEE"
	DeployCPULimitEnvName     = "DEPLOY_VCPU_LIMIT"
	DeployCPUGuaranteeEnvName = "DEPLOY_VCPU_GUARANTEE"
	DeployBoxIDName           = podagent.EnvBoxIDKey
)

// Adjust adjust the maximum number of CPUs that can be executing.
// Takes a minimum between n and CPU counts and returns the previous setting
func Adjust(n int) int {
	if n < MinProc {
		n = MinProc
	}

	nCPU := runtime.NumCPU()
	if n < nCPU {
		return runtime.GOMAXPROCS(n)
	}

	return runtime.GOMAXPROCS(nCPU)
}

// AdjustAuto automatically adjust the maximum number of CPUs that can be executing to safe value
// and returns the previous setting
func AdjustAuto() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if isCgroupsExists() {
		if prev, ok := tryAdjustCgroup(); ok {
			return prev
		}
	}

	if val, ok := getEnv(InstancectlCPUEnvName); ok {
		if prev, ok := tryApplyFloatStringLimit(strings.TrimRight(val, "c")); ok {
			return prev
		}
	}

	if val, ok := getEnv(QloudCPUEnvName); ok {
		if prev, ok := tryApplyFloatStringLimit(val); ok {
			return prev
		}
	}

	if _, ok := getEnv(DeployBoxIDName); ok {
		if prev, ok := tryAdjustYPBox(); ok {
			return prev
		}
	}

	if yplite.IsAPIAvailable() {
		if prev, ok := tryAdjustYPLite(); ok {
			return prev
		}
	}

	return Adjust(SafeProc)
}

// AdjustQloud automatically adjust the maximum number of CPUs in case of Qloud env
// and returns the previous setting
func AdjustQloud() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if val, ok := getEnv(QloudCPUEnvName); ok {
		if prev, ok := tryApplyFloatStringLimit(val); ok {
			return prev
		}
	}

	return Adjust(SafeProc)
}

// AdjustYP automatically adjust the maximum number of CPUs in case of YP/Y.Deploy/YP.Hard env
// and returns the previous setting
func AdjustYP() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if isCgroupsExists() {
		if prev, ok := tryAdjustCgroup(); ok {
			return prev
		}
	}

	return adjustYPBox()
}

func adjustYPBox() int {
	if prev, ok := tryAdjustYPBox(); ok {
		return prev
	}

	return Adjust(SafeProc)
}

func tryAdjustYPBox() (int, bool) {
	if val, ok := getEnv(DeployCPULimitEnvName); ok {
		cpuGuaranteeMS, err := strconv.Atoi(val)
		if err == nil {
			return Adjust(cpuGuaranteeMS / 1000), true
		}
	}

	if val, ok := getEnv(DeployCPUGuaranteeEnvName); ok {
		cpuGuaranteeMS, err := strconv.Atoi(val)
		if err == nil {
			return Adjust(cpuGuaranteeMS / 1000), true
		}
	}

	return 0, false
}

// AdjustYPLite automatically adjust the maximum number of CPUs in case of YP.Lite env
// and returns the previous setting
func AdjustYPLite() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if prev, ok := tryAdjustYPLite(); ok {
		return prev
	}

	return Adjust(SafeProc)
}

func tryAdjustYPLite() (int, bool) {
	podAttributes, err := yplite.FetchPodAttributes()
	if err != nil {
		return 0, false
	}

	return applyFloatLimit(float64(podAttributes.ResourceRequirements.CPU.Guarantee / 1000)), true
}

// AdjustInstancectl automatically adjust the maximum number of CPUs
// and returns the previous setting
// WARNING: supported only instancectl v1.177+ (https://wiki.yandex-team.ru/runtime-cloud/nanny/instancectl-change-log/#1.177)
func AdjustInstancectl() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if val, ok := getEnv(InstancectlCPUEnvName); ok {
		if prev, ok := tryApplyFloatStringLimit(strings.TrimRight(val, "c")); ok {
			return prev
		}
	}

	return Adjust(SafeProc)
}

// AdjustCgroup automatically adjust the maximum number of CPUs based on the CFS quota
// and returns the previous setting.
func AdjustCgroup() int {
	if val, ok := getEnv(GoMaxProcEnvName); ok {
		if prev, ok := tryApplyIntStringLimit(val); ok {
			return prev
		}
	}

	if prev, ok := tryAdjustCgroup(); ok {
		return prev
	}

	return Adjust(SafeProc)
}

func tryAdjustCgroup() (int, bool) {
	quota, err := currentCFSQuota()
	if err != nil {
		return 0, false
	}

	return applyFloatLimit(quota), true
}
