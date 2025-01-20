#pragma once

#include "public.h"

#include <yt/yt/library/containers/public.h>
#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/containers/cgroup.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentCpuStatistics
{
    std::optional<TDuration> BurstUsageTime;
    std::optional<TDuration> UserUsageTime;
    std::optional<TDuration> SystemUsageTime;
    std::optional<TDuration> WaitTime;
    std::optional<TDuration> ThrottledTime;
    std::optional<TDuration> CfsThrottledTime;
    std::optional<i64> ContextSwitchesDelta;
    std::optional<i64> PeakThreadCount;
};

void Serialize(const TJobEnvironmentCpuStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): All fields are used in memory_tracker
// and thus must be present (unlike block io stats for example)
// thus no optionals.
// TODO(arkady-e1ppa): Maybe use ValueOrDefault in
// ExtractJobEnvironmentMemoryStatistics?
struct TJobEnvironmentMemoryStatistics
{
    i64 ResidentAnon = 0;
    i64 TmpfsUsage = 0;
    i64 MappedFile = 0;
    i64 MajorPageFaults = 0;
};

void Serialize(const TJobEnvironmentMemoryStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentBlockIOStatistics
{
    std::optional<i64> IOReadByte;
    std::optional<i64> IOWriteByte;

    std::optional<i64> IOReadOps;
    std::optional<i64> IOWriteOps;
    std::optional<i64> IOOps;
};

void Serialize(const TJobEnvironmentBlockIOStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentNetworkStatistics
{
    std::optional<i64> TxBytes;
    std::optional<i64> TxPackets;
    std::optional<i64> TxDrops;

    std::optional<i64> RxBytes;
    std::optional<i64> RxPackets;
    std::optional<i64> RxDrops;
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

    std::vector<int> GpuIndexes;

    std::optional<TString> HostName;
    std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;
    bool EnableNat64;
    bool DisableNetwork;

    bool EnableFuse = false;

    bool EnableCudaGpuCoreDump = false;

    bool EnablePortoMemoryTracking = false;

    NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;

    i64 ThreadLimit;
};

////////////////////////////////////////////////////////////////////////////////

struct IUserJobEnvironment
    : public virtual IProfiledEnvironment
{
    virtual std::optional<TDuration> GetBlockIOWatchdogPeriod() const = 0;

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

    virtual bool IsPidNamespaceIsolationEnabled() const = 0;

    //! Returns the list of environment-specific environment variables in key=value format.
    virtual const std::vector<TString>& GetEnvironmentVariables() const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;

    virtual std::optional<i64> GetJobOOMKillCount() const noexcept = 0;
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
        NJobTrackerClient::TJobId jobId,
        const TUserJobEnvironmentOptions& options) = 0;

    // TODO(gritukan, khlebnikov): For now job proxy and user job are run in the same cgroup
    // in CRI environment. This means that their statistics are indisguishable. Remove these methods
    // when separate container for job proxy is implemented.
    virtual std::optional<TJobEnvironmentBlockIOStatistics> GetJobBlockIOStatistics() const noexcept = 0;
    virtual std::optional<TJobEnvironmentMemoryStatistics> GetJobMemoryStatistics() const noexcept = 0;
    virtual std::optional<TJobEnvironmentCpuStatistics> GetJobCpuStatistics() const noexcept = 0;
    virtual std::optional<i64> GetJobOOMKillCount() const noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(NYTree::INodePtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
