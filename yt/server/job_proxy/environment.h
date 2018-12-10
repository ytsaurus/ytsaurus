#pragma once

#include "public.h"

#include <yt/server/containers/public.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/ytree/public.h>
#include <yt/core/misc/process.h>


namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using TCpuStatistics = NCGroup::TCpuAccounting::TStatistics;
using TBlockIOStatistics = NCGroup::TBlockIO::TStatistics;
using TMemoryStatistics = NCGroup::TMemory::TStatistics;

////////////////////////////////////////////////////////////////////////////////

struct IResourceTracker
    : public virtual TRefCounted
{
    virtual TCpuStatistics GetCpuStatistics() const = 0;
    virtual TBlockIOStatistics GetBlockIOStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceTracker)

////////////////////////////////////////////////////////////////////////////////

struct IUserJobEnvironment
    : public virtual IResourceTracker
{
    virtual TDuration GetBlockIOWatchdogPeriod() const = 0;

    virtual TMemoryStatistics GetMemoryStatistics() const = 0;
    virtual i64 GetMaxMemoryUsage() const = 0;

    virtual void CleanProcesses() = 0;

    virtual void SetIOThrottle(i64 operations) = 0;

    virtual TProcessBasePtr CreateUserJobProcess(const TString& path, int uid, const std::optional<TString>& coreHandlerSocketPath) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyEnvironment
    : public virtual IResourceTracker
{
    virtual void SetCpuShare(double share) = 0;
    virtual void EnablePortoMemoryTracking() = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(
    NYTree::INodePtr config,
    const std::optional<NContainers::TRootFS>& rootFS,
    std::vector<TString> gpuDevices);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
