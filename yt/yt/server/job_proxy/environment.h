#pragma once

#include "public.h"

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/ytlib/cgroup/cgroup.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>

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

    virtual std::optional<TMemoryStatistics> GetMemoryStatistics() const = 0;

    virtual void CleanProcesses() = 0;

    virtual void SetIOThrottle(i64 operations) = 0;

    struct TUserJobProcessOptions
    {
        //! Path to core watcher pipes directory relative to user job working directory.
        std::optional<TString> SlotCoreWatcherDirectory;

        //! Path to core watcher pipes directory relative to job proxy working directory.
        std::optional<TString> CoreWatcherDirectory;

        NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;
        bool EnableCudaGpuCoreDump = false;
        std::optional<TString> HostName;
        std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;
    };

    virtual TProcessBasePtr CreateUserJobProcess(
        const TString& path,
        const TUserJobProcessOptions& options) = 0;

    virtual NContainers::IInstancePtr GetUserJobInstance() const = 0;

    //! Returns the list of environment-specific environment variables in key=value format.
    virtual const std::vector<TString>& GetEnvironmentVariables() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyEnvironment
    : public virtual IResourceTracker
{
    virtual void SetCpuShare(double share) = 0;
    virtual void SetCpuLimit(double limit) = 0;
    virtual void EnablePortoMemoryTracking() = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(TGuid jobId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(
    NYTree::INodePtr config,
    const std::optional<NContainers::TRootFS>& rootFS,
    std::vector<TString> gpuDevices);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
