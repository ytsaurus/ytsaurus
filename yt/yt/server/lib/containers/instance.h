#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

using TResourceUsage = THashMap<EStatField, TErrorOr<ui64>>;

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
    virtual void Stop() = 0;
    virtual void Destroy() = 0;
    virtual TString GetRoot() = 0;
    virtual TResourceUsage GetResourceUsage(const std::vector<EStatField>& fields) const = 0;
    virtual TResourceLimits GetResourceLimits() const = 0;
    virtual TResourceLimits GetResourceLimitsRecursive() const = 0;
    virtual void SetCpuGuarantee(double cores) = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetCpuWeight(double weight) = 0;
    virtual void SetIOWeight(double weight) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual void SetMemoryGuarantee(i64 memoryGuarantee) = 0;
    virtual void SetDevices(const std::vector<TDevice>& devices) = 0;
    virtual TString GetName() const = 0;
    virtual TString GetAbsoluteName() const = 0;
    virtual TString GetParentName() const = 0;
    virtual TString GetStderr() const = 0;
    virtual void SetEnablePorto(EEnablePorto enablePorto) = 0;
    virtual void SetIsolate(bool isolate) = 0;
    virtual void EnableMemoryTracking() = 0;
    virtual void SetGroup(int groupId) = 0;
    virtual void SetUser(const TString& user) = 0;
    virtual void SetIPAddresses(const std::vector<NNet::TIP6Address>& addresses) = 0;
    virtual void SetHostName(const TString& hostName) = 0;

    //! Adds a record into /etc/hosts file of instance.
    virtual void AddHostsRecord(const TString& host, const NNet::TIP6Address& address) = 0;

    //! Returns externally visible pid of the root proccess inside container.
    //! Throws if container is not running.
    virtual pid_t GetPid() const = 0;
    //! Returns the list of externally visible pids of processes running inside container.
    virtual std::vector<pid_t> GetPids() const = 0;
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
