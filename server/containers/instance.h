#pragma once

#ifndef __linux__
#error Platform must be linux to include this
#endif

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/fs.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStatField,
    (CpuUsageUser)
    (CpuUsageSystem)
    (CpuStolenTime)
    (Rss)
    (MappedFiles)
    (MajorFaults)
    (MinorFaults)
    (MaxMemoryUsage)
    (IOReadByte)
    (IOWriteByte)
    (IOOperations)
);

using TUsage = TEnumIndexedVector<TErrorOr<ui64>, EStatField>;

struct TResourceLimits
{
    double Cpu;
    i64 Memory;
};

////////////////////////////////////////////////////////////////////////////////

struct IInstance
    :  public TRefCounted
{
    virtual void SetStdIn(const TString& inputPath) = 0;
    virtual void SetStdOut(const TString& outPath) = 0;
    virtual void SetStdErr(const TString& errorPath) = 0;
    virtual void SetCwd(const TString& pwd) = 0;
    virtual void SetCoreDumpHandler(const TString& handler) = 0;
    virtual void SetRoot(const TRootFS& rootFS) = 0;

    virtual bool HasRoot() const = 0;

    virtual void Kill(int signal) = 0;
    virtual void Destroy() = 0;
    virtual TUsage GetResourceUsage(const std::vector<EStatField>& fields) const = 0;
    virtual TResourceLimits GetResourceLimits() const = 0;
    virtual TResourceLimits GetResourceLimitsRecursive() const = 0;
    virtual void SetCpuShare(double cores) = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetIOWeight(double weight) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual void SetMemoryGuarantee(i64 memoryGuarantee) = 0;
    virtual void SetDevices(const std::vector<TDevice>& devices) = 0;
    virtual TString GetName() const = 0;
    virtual TString GetAbsoluteName() const = 0;
    virtual void SetIsolate() = 0;
    virtual void EnableMemoryTracking() = 0;

    //! Returns externally visible pid of the root proccess inside container.
    //! Throws if container is not running.
    virtual pid_t GetPid() const = 0;
    virtual TFuture<int> Exec(
        const std::vector<const char*>& argv,
        const std::vector<const char*>& env) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstance)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
IInstancePtr CreatePortoInstance(const TString& name, IPortoExecutorPtr executor, bool autoDestroy = true);
IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor);
IInstancePtr GetPortoInstance(IPortoExecutorPtr executor, const TString& name);
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
