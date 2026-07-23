package maxprocs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/prometheus/procfs"
	"golang.org/x/exp/slices"
)

const (
	unifiedHierarchy = "unified"
	cpuHierarchy     = "cpu"
)

var (
	ErrNoCgroups  = errors.New("no suitable cgroups were found")
	ErrNoCPUQuota = errors.New("CPU quota is not configured")
)

func isCgroupsExists() bool {
	mounts, err := procfs.GetMounts()
	if err != nil {
		return false
	}

	return hasCgroupMount(mounts)
}

func hasCgroupMount(mounts []*procfs.MountInfo) bool {
	for _, m := range mounts {
		if m.FSType == "cgroup" || m.FSType == "cgroup2" {
			return true
		}
	}

	return false
}

func parseCgroupMounts() (map[string]*procfs.MountInfo, error) {
	mounts, err := procfs.GetMounts()
	if err != nil {
		return nil, err
	}

	out := make(map[string]*procfs.MountInfo)
	for _, mount := range mounts {
		switch mount.FSType {
		case "cgroup2":
			out[unifiedHierarchy] = mount
		case "cgroup":
			for opt := range mount.SuperOptions {
				if opt == cpuHierarchy {
					out[cpuHierarchy] = mount
					break
				}
			}
		}
	}

	return out, nil
}

func currentCFSQuota() (float64, error) {
	self, err := procfs.Self()
	if err != nil {
		return 0, err
	}

	cgroups, err := self.Cgroups()
	if err != nil {
		return 0, fmt.Errorf("parse current process cgroups: %w", err)
	}

	mounts, err := parseCgroupMounts()
	if err != nil {
		return 0, fmt.Errorf("parse cgroups hierarchy: %w", err)
	}

	return parseCFSQuota(cgroups, mounts)
}

func parseCFSQuota(cgroups []procfs.Cgroup, mounts map[string]*procfs.MountInfo) (float64, error) {
	if len(cgroups) == 0 || len(mounts) == 0 {
		return 0, ErrNoCgroups
	}

	var errs []error
	for _, cgroup := range cgroups {
		var mountPoint *procfs.MountInfo
		var parser func(cgroupRoot string) (float64, error)
		switch {
		case cgroup.HierarchyID == 0:
			// for the cgroups v2 hierarchy id is always 0
			mountPoint = mounts[unifiedHierarchy]
			parser = parseV2CPUQuota

		case slices.Contains(cgroup.Controllers, cpuHierarchy):
			mountPoint = mounts[cpuHierarchy]
			parser = parseV1CPUQuota
		}

		if mountPoint == nil || parser == nil {
			continue
		}

		cgroupRoot, err := translateMountPath(mountPoint, cgroup.Path)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid cgroup %q path: %w", cgroup.Path, err))
			continue
		}

		quota, err := parser(cgroupRoot)
		if err != nil {
			errs = append(errs, fmt.Errorf("parse cgroup %q CPU quote: %w", cgroup.Path, err))
			continue
		}

		if quota > 0 {
			return quota, nil
		}
	}

	return 0, errors.Join(errs...)
}

func parseV1CPUQuota(cgroupRoot string) (float64, error) {
	quotaPath := filepath.Join(cgroupRoot, "cpu.cfs_quota_us")
	if err := checkCgroupPath(quotaPath); err != nil {
		return 0, err
	}

	cfsQuota, err := readFileInt(quotaPath)
	if err != nil {
		return -1, fmt.Errorf("parse cpu.cfs_quota_us: %w", err)
	}

	// A value of -1 for cpu.cfs_quota_us indicates that the group does not have any
	// bandwidth restriction in place
	// https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
	if cfsQuota == -1 {
		return float64(runtime.NumCPU()), nil
	}

	periodPath := filepath.Join(cgroupRoot, "cpu.cfs_period_us")
	if err := checkCgroupPath(periodPath); err != nil {
		return 0, err
	}

	cfsPeriod, err := readFileInt(periodPath)
	if err != nil {
		return -1, fmt.Errorf("parse cpu.cfs_period_us: %w", err)
	}

	return float64(cfsQuota) / float64(cfsPeriod), nil
}

func parseV2CPUQuota(cgroupRoot string) (float64, error) {
	cpuMaxPath := filepath.Join(cgroupRoot, "cpu.max")
	if err := checkCgroupPath(cpuMaxPath); err != nil {
		return 0, err
	}

	rawCPUMax, err := os.ReadFile(cpuMaxPath)
	if err != nil {
		return -1, fmt.Errorf("read cpu.max: %w", err)
	}

	/*
		https://www.kernel.org/doc/Documentation/cgroup-v2.txt

		cpu.max
			A read-write two value file which exists on non-root cgroups.
			The default is "max 100000".

			The maximum bandwidth limit.  It's in the following format::
			  $MAX $PERIOD

			which indicates that the group may consume upto $MAX in each
			$PERIOD duration.  "max" for $MAX indicates no limit.  If only
			one number is written, $MAX is updated.
	*/
	parts := strings.Fields(string(rawCPUMax))
	if len(parts) != 2 {
		return -1, fmt.Errorf("invalid cpu.max format: %s", string(rawCPUMax))
	}

	// "max" for $MAX indicates no limit
	if parts[0] == "max" {
		return float64(runtime.NumCPU()), nil
	}

	cpuMax, err := strconv.Atoi(parts[0])
	if err != nil {
		return -1, fmt.Errorf("parse cpu.max[max] (%q): %w", parts[0], err)
	}

	cpuPeriod, err := strconv.Atoi(parts[1])
	if err != nil {
		return -1, fmt.Errorf("parse cpu.max[period] (%q): %w", parts[1], err)
	}

	return float64(cpuMax) / float64(cpuPeriod), nil
}

func checkCgroupPath(p string) error {
	_, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNoCPUQuota
		}
		return err
	}

	return nil
}

func translateMountPath(mount *procfs.MountInfo, target string) (string, error) {
	localPath, err := filepath.Rel(mount.Root, target)
	if err != nil {
		return "", fmt.Errorf("relative from root %q: %w", mount.Root, err)
	}

	if localPath == ".." || strings.HasPrefix(localPath, "../") {
		return "", fmt.Errorf("target path %q is outside root %q", localPath, mount.Root)
	}

	return filepath.Join(mount.MountPoint, localPath), nil
}
