#pragma once

#include "public.h"

#include <yt/server/lib/containers/public.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/net/address.h>

#include <yt/core/ytree/public.h>

#include <yt/library/process/process.h>


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

    struct TUserJobProcessOptions
    {
        std::optional<TString> CoreWatcherDirectory;
        NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;
        bool EnableCudaGpuCoreDump = false;
    };

    virtual TProcessBasePtr CreateUserJobProcess(
        const TString& path,
        int uid,
        const TUserJobProcessOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyEnvironment
    : public virtual IResourceTracker
{
    virtual void SetCpuShare(double share) = 0;
    virtual void SetCpuLimit(double limit) = 0;
    virtual void EnablePortoMemoryTracking() = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(
    NYTree::INodePtr config,
    const std::optional<NContainers::TRootFS>& rootFS,
    std::vector<TString> gpuDevices,
    const std::vector<NNet::TIP6Address>& networkAddresses,
    const std::optional<TString>& hostName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
