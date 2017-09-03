#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/fs.h>

namespace NYT {
namespace NContainers {

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

////////////////////////////////////////////////////////////////////////////////

struct IInstance
    :  public TRefCounted
{
    virtual void SetStdIn(const TString& inputPath) = 0;
    virtual void SetStdOut(const TString& outPath) = 0;
    virtual void SetStdErr(const TString& errorPath) = 0;
    virtual void SetCwd(const TString& pwd) = 0;
    virtual void SetCoreDumpHandler(const TString& handler) = 0;
    virtual void Kill(int signal) = 0;
    virtual void Destroy() = 0;
    virtual TUsage GetResourceUsage(const std::vector<EStatField>& fields) const = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetCpuShare(double cores) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual TString GetName() const = 0;

    //! Returns externally visible pid of the root proccess inside container.
    //! Throws if container is not running.
    virtual pid_t GetPid() const = 0;
    virtual TFuture<int> Exec(
        const std::vector<const char*>& argv,
        const std::vector<const char*>& env) = 0;
    virtual void MountTmpfs(const TString& path, size_t size, const TString& user) = 0;
    virtual void Umount(const TString& path) = 0;
    virtual std::vector<NFS::TMountPoint> ListVolumes() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstance)

////////////////////////////////////////////////////////////////////////////////

IInstancePtr CreatePortoInstance(const TString& name, IPortoExecutorPtr executor);
IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
