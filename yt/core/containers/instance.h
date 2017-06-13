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
    virtual void SetStdIn(const Stroka& inputPath) = 0;
    virtual void SetStdOut(const Stroka& outPath) = 0;
    virtual void SetStdErr(const Stroka& errorPath) = 0;
    virtual void SetCwd(const Stroka& pwd) = 0;
    virtual void Kill(int signal) = 0;
    virtual void Destroy() = 0;
    virtual TUsage GetResourceUsage(const std::vector<EStatField>& fields) const = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetCpuShare(double cores) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual Stroka GetName() const = 0;
    virtual pid_t GetPid() const = 0;
    virtual TFuture<int> Exec(
        const std::vector<const char*>& argv,
        const std::vector<const char*>& env) = 0;
    virtual void MountTmpfs(const Stroka& path, size_t size, const Stroka& user) = 0;
    virtual void Umount(const Stroka& path) = 0;
    virtual std::vector<NFS::TMountPoint> ListVolumes() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstance)

////////////////////////////////////////////////////////////////////////////////

#if defined(_linux_)
IInstancePtr CreatePortoInstance(const Stroka& name, IPortoExecutorPtr executor);
IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor);
#else
inline IInstancePtr CreatePortoInstance(const Stroka& /*name*/, IPortoExecutorPtr /*executor*/)
{
    Y_UNIMPLEMENTED();
    return nullptr;
}

inline IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr /*executor*/)
{
    Y_UNIMPLEMENTED();
    return nullptr;
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
