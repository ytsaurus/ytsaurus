#pragma once

#include "public.h"

#include <yt/yt/library/containers/cgroup.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

const std::vector<EStatField> InstanceStatFields{
    EStatField::CpuBurstUsage,
    EStatField::CpuUsage,
    EStatField::CpuUserUsage,
    EStatField::CpuSystemUsage,
    EStatField::CpuWait,
    EStatField::CpuThrottled,
    EStatField::CpuCfsThrottled,
    EStatField::ContextSwitches,
    EStatField::ContextSwitchesDelta,
    EStatField::ThreadCount,
    EStatField::CpuLimit,
    EStatField::CpuGuarantee,

    EStatField::ResidentAnon,
    EStatField::TmpfsUsage,
    EStatField::MappedFile,
    EStatField::MajorPageFaults,
    EStatField::MinorPageFaults,
    EStatField::FileCacheUsage,
    EStatField::AnonMemoryUsage,
    EStatField::AnonMemoryLimit,
    EStatField::MemoryUsage,
    EStatField::MemoryGuarantee,
    EStatField::MemoryLimit,
    EStatField::MaxMemoryUsage,
    EStatField::OomKills,
    EStatField::OomKillsTotal,

    EStatField::IOReadByte,
    EStatField::IOWriteByte,
    EStatField::IOBytesLimit,
    EStatField::IOReadOps,
    EStatField::IOWriteOps,
    EStatField::IOOps,
    EStatField::IOOpsLimit,
    EStatField::IOTotalTime,
    EStatField::IOWaitTime,

    EStatField::NetTxBytes,
    EStatField::NetTxPackets,
    EStatField::NetTxDrops,
    EStatField::NetTxLimit,
    EStatField::NetRxBytes,
    EStatField::NetRxPackets,
    EStatField::NetRxDrops,
    EStatField::NetRxLimit,

    EStatField::VolumeCounts,

    EStatField::LayerCounts
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceUsage
{
    struct TTaggedStat
    {
        std::string Tag;
        i64 Value;
    };

    THashMap<EStatField, TErrorOr<i64>> ContainerStats;
    THashMap<EStatField, TErrorOr<std::vector<TTaggedStat>>> ContainerTaggedStats;
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimits
{
    double CpuLimit;
    double CpuGuarantee;
    i64 Memory;
};

////////////////////////////////////////////////////////////////////////////////

struct IInstanceLauncher
    : public TRefCounted
{
    virtual bool HasRoot() const = 0;
    virtual const std::string& GetName() const = 0;

    virtual void SetStdIn(const std::string& inputPath) = 0;
    virtual void SetStdOut(const std::string& outPath) = 0;
    virtual void SetStdErr(const std::string& errorPath) = 0;
    virtual void SetCwd(const std::string& pwd) = 0;

    // Null core dump handler implies disabled core dumps.
    virtual void SetCoreDumpHandler(const std::optional<std::string>& handler) = 0;
    virtual void SetRoot(const TRootFS& rootFS) = 0;

    virtual void SetCpuWeight(double cpuWeight) = 0;
    virtual void SetThreadLimit(i64 threadLimit) = 0;
    virtual void SetDevices(const std::vector<TDevice>& devices) = 0;

    virtual void SetEnablePorto(EEnablePorto enablePorto) = 0;
    virtual void SetIsolate(bool isolate) = 0;
    virtual void SetEnableFuse(bool enableFuse) = 0;
    virtual void EnableMemoryTracking() = 0;
    virtual void SetGroup(int groupId) = 0;
    virtual void SetUser(const std::string& user) = 0;
    virtual void SetNetworkInterface(const std::string& networkInterface) = 0;
    virtual void SetIPAddresses(
        const std::vector<NNet::TIP6Address>& addresses,
        bool enableNat64 = false) = 0;
    virtual void DisableNetwork() = 0;
    virtual void SetHostName(const std::string& hostName) = 0;
    virtual void SetPlaces(const std::vector<std::string>& places) = 0;

    virtual TFuture<IInstancePtr> Launch(
        const std::string& path,
        const std::vector<std::string>& args,
        const THashMap<std::string, std::string>& env) = 0;
    virtual TFuture<IInstancePtr> LaunchMeta(
        const THashMap<std::string, std::string>& env) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstanceLauncher)

#ifdef _linux_
IInstanceLauncherPtr CreatePortoInstanceLauncher(std::string_view name, IPortoExecutorPtr executor);
#endif

////////////////////////////////////////////////////////////////////////////////

struct IInstance
    : public TRefCounted
{
    virtual void Kill(int signal) = 0;
    virtual void Stop() = 0;
    virtual void Destroy() = 0;
    virtual void Respawn() = 0;

    virtual TResourceUsage GetResourceUsage(
        const std::vector<EStatField>& fields = InstanceStatFields) const = 0;
    virtual TResourceLimits GetResourceLimits() const = 0;
    virtual void SetCpuGuarantee(double cores) = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetCpuWeight(double weight) = 0;
    virtual void SetIOWeight(double weight) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual void SetMemoryGuarantee(i64 memoryGuarantee) = 0;

    virtual std::string GetStdout() const = 0;
    virtual std::string GetStderr() const = 0;

    virtual std::string GetName() const = 0;
    virtual std::optional<std::string> GetParentName() const = 0;
    virtual std::optional<std::string> GetRootName() const = 0;

    //! Returns externally visible pid of the root process inside container.
    //! Throws if container is not running.
    virtual pid_t GetPid() const = 0;
    //! Returns the list of externally visible pids of processes running inside container.
    virtual std::vector<pid_t> GetPids() const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;
    virtual double GetCpuGuarantee() const = 0;

    //! Future is set when container reaches terminal state (stopped or dead).
    //! Resulting error is OK iff container exited with code 0.
    virtual TFuture<void> Wait() = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstance)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
std::string GetSelfContainerName(const IPortoExecutorPtr& executor);

IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor);
IInstancePtr GetRootPortoInstance(IPortoExecutorPtr executor);
IInstancePtr GetPortoInstance(IPortoExecutorPtr executor, const std::string& name, const std::optional<std::string>& networkInterface = std::nullopt);

//! Works only in Yandex.Deploy pod environment where env DEPLOY_VCPU_LIMIT is set.
//! Throws if this env is absent.
double GetSelfPortoInstanceVCpuFactor();
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
