#pragma once

#include "public.h"

#include <yt/yt/library/containers/public.h>
#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/containers/cgroup.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/process/process.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentCpuStatistics
{
    TDuration UserUsageTime;
    TDuration SystemUsageTime;
    TDuration WaitTime;
    TDuration ThrottledTime;
    ui64 ContextSwitchesDelta;
    ui64 PeakThreadCount;
};

void Serialize(const TJobEnvironmentCpuStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentMemoryStatistics
{
    ui64 Rss;
    ui64 MappedFile;
    ui64 MajorPageFaults;
};

void Serialize(const TJobEnvironmentMemoryStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentBlockIOStatistics
{
    ui64 IOReadByte;
    ui64 IOWriteByte;

    ui64 IOReadOps;
    ui64 IOWriteOps;
    ui64 IOOps;
};

void Serialize(const TJobEnvironmentBlockIOStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentNetworkStatistics
{
    ui64 TxBytes;
    ui64 TxPackets;
    ui64 TxDrops;

    ui64 RxBytes;
    ui64 RxPackets;
    ui64 RxDrops;
};

void Serialize(const TJobEnvironmentNetworkStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct IProfiledEnvironment
    : public virtual TRefCounted
{
    virtual TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const = 0;
    virtual TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfiledEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct TUserJobEnvironmentOptions
{
    //! Path to core watcher pipes directory relative to user job working directory.
    std::optional<TString> SlotCoreWatcherDirectory;

    //! Path to core watcher pipes directory relative to job proxy working directory.
    std::optional<TString> CoreWatcherDirectory;

    std::optional<NContainers::TRootFS> RootFS;

    std::vector<TString> GpuDevices;

    std::optional<TString> HostName;
    std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;
    bool EnableNat64;

    bool EnableCudaGpuCoreDump = false;

    bool EnablePortoMemoryTracking = false;

    NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;

    i64 ThreadLimit;
};

////////////////////////////////////////////////////////////////////////////////

struct IUserJobEnvironment
    : public virtual IProfiledEnvironment
{
    virtual TDuration GetBlockIOWatchdogPeriod() const = 0;

    virtual TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetMemoryStatistics() const = 0;

    virtual TErrorOr<std::optional<TJobEnvironmentNetworkStatistics>> GetNetworkStatistics() const = 0;

    virtual void CleanProcesses() = 0;

    virtual void SetIOThrottle(i64 operations) = 0;

    virtual TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory) = 0;

    virtual NContainers::IInstancePtr GetUserJobInstance() const = 0;

    virtual std::optional<pid_t> GetJobRootPid() const = 0;
    virtual std::vector<pid_t> GetJobPids() const = 0;

    virtual bool PidNamespaceIsolationEnabled() const = 0;

    //! Returns the list of environment-specific environment variables in key=value format.
    virtual const std::vector<TString>& GetEnvironmentVariables() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyEnvironment
    : public virtual IProfiledEnvironment
{
    virtual void SetCpuGuarantee(double value) = 0;
    virtual void SetCpuLimit(double value) = 0;
    virtual void SetCpuPolicy(const TString& policy) = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TGuid jobId,
        const TUserJobEnvironmentOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(NYTree::INodePtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
