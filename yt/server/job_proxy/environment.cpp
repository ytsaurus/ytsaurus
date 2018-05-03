#include "environment.h"

#include <yt/server/exec_agent/config.h>

#include <yt/ytlib/job_proxy/private.h>

#ifdef _linux_
#include <yt/server/containers/porto_executor.h>
#include <yt/server/containers/instance.h>

#include <yt/server/misc/process.h>
#endif

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NJobProxy {

using namespace NConcurrency;
using namespace NContainers;
using namespace NCGroup;
using namespace NExecAgent;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// Option cpu.share is limited to [2, 1024], see http://git.kernel.org/cgit/linux/kernel/git/tip/tip.git/tree/kernel/sched/sched.h#n279
// To overcome this limitation we consider one cpu_limit unit as ten cpu.shares units.
static constexpr int CpuShareMultiplier = 10;

static const NLogging::TLogger Logger("ResourceController");

////////////////////////////////////////////////////////////////////////////////

struct TCGroups
{
    explicit TCGroups(const TString& name)
        : Freezer(name)
        , CpuAccounting(name)
        , BlockIO(name)
        , Cpu(name)
    { }

    TFreezer Freezer;
    TCpuAccounting CpuAccounting;
    TBlockIO BlockIO;
    TCpu Cpu;
};

////////////////////////////////////////////////////////////////////////////////

class TMemoryTrackerBase
{
protected:
    mutable i64 MaxMemoryUsage_ = 0;
    mutable i64 PageFaultCount_ = 0;
    int UserId_ = -1;
    TProcessBasePtr Process_;

    virtual ~TMemoryTrackerBase() = default;

    TMemoryStatistics GetMemoryStatistics() const
    {
        TMemoryStatistics memoryStatistics;
        memoryStatistics.Rss = 0;
        memoryStatistics.MappedFile = 0;
        memoryStatistics.MajorPageFaults = 0;

        if (!Process_) {
            return memoryStatistics;
        }

        for (auto pid : GetPidsByUid(UserId_)) {
            try {
                auto memoryUsage = GetProcessMemoryUsage(pid);
                // RSS from /proc/pid/statm includes all pages resident to current process,
                // including memory-mapped files and shared memory.
                // Since we want to account shared memory separately, let's subtract it here.

                memoryStatistics.Rss += memoryUsage.Rss - memoryUsage.Shared;
                memoryStatistics.MappedFile += memoryUsage.Shared;

                LOG_DEBUG("Memory statistics collected (Pid: %v, ProcessName: %v, Rss: %v, Shared: %v)",
                    pid,
                    GetProcessName(pid),
                    memoryStatistics.Rss,
                    memoryStatistics.MappedFile);

            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get memory usage (Pid: %v)", pid);
            }
        }

        try {
            PageFaultCount_ = GetProcessCumulativeMajorPageFaults(Process_->GetProcessId());
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Failed to get page fault count (Pid: %v)", Process_->GetProcessId());
        }

        memoryStatistics.MajorPageFaults = PageFaultCount_;

        if (memoryStatistics.Rss + memoryStatistics.MappedFile > MaxMemoryUsage_) {
            MaxMemoryUsage_ = memoryStatistics.Rss + memoryStatistics.MappedFile;
        }

        return memoryStatistics;
    }
};


////////////////////////////////////////////////////////////////////////////////

class TCGroupsResourceTracker
    : public virtual IResourceTracker
{
public:
    TCGroupsResourceTracker(
        TCGroupJobEnvironmentConfigPtr config,
        const TString& path)
    : CGroupsConfig_(config)
    , Path_(path)
    , CGroups_(path)
    { }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        if (CGroupsConfig_->IsCGroupSupported(TCpuAccounting::Name)) {
            return CGroups_.CpuAccounting.GetStatistics();
        }
        THROW_ERROR_EXCEPTION("Cpu accounting cgroup is not supported");
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
            return CGroups_.BlockIO.GetStatistics();
        }
        THROW_ERROR_EXCEPTION("Block io cgroup is not supported");
    }

protected:
    const TCGroupJobEnvironmentConfigPtr CGroupsConfig_;
    const TString Path_;
    TCGroups CGroups_;
};

////////////////////////////////////////////////////////////////////////////////

class TCGroupsUserJobEnvironment
    : public TCGroupsResourceTracker
    , public IUserJobEnvironment
    , private TMemoryTrackerBase
{
public:
    TCGroupsUserJobEnvironment(
        TCGroupJobEnvironmentConfigPtr config,
        const TString& path)
        : TCGroupsResourceTracker(config, path)
    { }

    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        return CGroupsConfig_->BlockIOWatchdogPeriod;
    }

    virtual TMemoryStatistics GetMemoryStatistics() const
    {
        return TMemoryTrackerBase::GetMemoryStatistics();
    }

    virtual i64 GetMaxMemoryUsage() const override
    {
        return MaxMemoryUsage_;
    }

    virtual void CleanProcesses() override
    {
        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may still be alive.
            RunKiller(CGroups_.Freezer.GetFullPath());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to kill user processes");
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
            CGroups_.BlockIO.ThrottleOperations(operations);
        }
    }

    virtual TProcessBasePtr CreateUserJobProcess(const TString& path, int uid, const TNullable<TString>& coreHandlerSocketPath) override
    {
        Y_UNUSED(coreHandlerSocketPath);
        YCHECK(!Process_);

        UserId_ = uid;
        Process_ =  New<TSimpleProcess>(path, false);
        try {
            {
                CGroups_.Freezer.Create();
                Process_->AddArguments({"--cgroup", CGroups_.Freezer.GetFullPath()});
            }

            if (CGroupsConfig_->IsCGroupSupported(TCpuAccounting::Name)) {
                CGroups_.CpuAccounting.Create();
                Process_->AddArguments({"--cgroup", CGroups_.CpuAccounting.GetFullPath()});
                Process_->AddArguments({"--env", Format("YT_CGROUP_CPUACCT=%v", CGroups_.CpuAccounting.GetFullPath())});
            }

            if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
                CGroups_.BlockIO.Create();
                Process_->AddArguments({"--cgroup", CGroups_.BlockIO.GetFullPath()});
                Process_->AddArguments({"--env", Format("YT_CGROUP_BLKIO=%v", CGroups_.BlockIO.GetFullPath())});
            }

            Process_->AddArguments({"--uid", ::ToString(uid)});

        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create required cgroups");
        }
        return Process_;
    }
};

DECLARE_REFCOUNTED_CLASS(TCGroupsUserJobEnvironment);
DEFINE_REFCOUNTED_TYPE(TCGroupsUserJobEnvironment);

////////////////////////////////////////////////////////////////////////////////

class TCGroupsJobProxyEnvironment
    : public TCGroupsResourceTracker
    , public IJobProxyEnvironment
{
public:
    TCGroupsJobProxyEnvironment(TCGroupJobEnvironmentConfigPtr config)
        : TCGroupsResourceTracker(config, "")
    { }

    virtual void SetCpuShare(double share) override
    {
        if (CGroupsConfig_->IsCGroupSupported(TCpu::Name)) {
            CGroups_.Cpu.SetShare(share * CpuShareMultiplier);
        }
    }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) override
    {
        return New<TCGroupsUserJobEnvironment>(CGroupsConfig_, "user_job_" + jobId);
    }
};

DECLARE_REFCOUNTED_CLASS(TCGroupsJobProxyEnvironment);
DEFINE_REFCOUNTED_TYPE(TCGroupsJobProxyEnvironment);

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

template <class ...Args>
static TError CheckErrors(const TUsage& stats, const Args&... args)
{
    std::vector<EStatField> fields = {args...};
    TError error;
    for (const auto& field : fields) {
        if (!stats[field].IsOK()) {
            if (error.IsOK()) {
                error = stats[field];
            } else {
                error << stats[field];
            }
        }
    }
    return error;
}

////////////////////////////////////////////////////////////////////////////////

class TPortoResourceTracker
    : public virtual IResourceTracker
{
public:
    TPortoResourceTracker(IInstancePtr instance, TDuration statUpdatePeriod)
        : Instance_(std::move(instance))
        , StatUpdatePeriod_(statUpdatePeriod)
    { }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        UpdateResourceUsage();

        auto guard = Guard(SpinLock_);
        auto error = CheckErrors(ResourceUsage_,
            EStatField::CpuUsageSystem,
            EStatField::CpuUsageUser);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to get cpu statistics");
        TCpuStatistics cpuStatistic;
        // porto returns nanosecond
        cpuStatistic.SystemTime = TDuration().MicroSeconds(ResourceUsage_[EStatField::CpuUsageSystem].Value() / 1000);
        cpuStatistic.UserTime = TDuration().MicroSeconds(ResourceUsage_[EStatField::CpuUsageUser].Value() / 1000);
        return cpuStatistic;
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        UpdateResourceUsage();

        auto guard = Guard(SpinLock_);
        auto error = CheckErrors(ResourceUsage_,
            EStatField::IOReadByte,
            EStatField::IOWriteByte,
            EStatField::IOOperations);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to get io statistics");
        TBlockIOStatistics blockIOStatistics;
        blockIOStatistics.BytesRead = ResourceUsage_[EStatField::IOReadByte].Value();
        blockIOStatistics.BytesWritten = ResourceUsage_[EStatField::IOWriteByte].Value();
        blockIOStatistics.IOTotal = ResourceUsage_[EStatField::IOOperations].Value();
        return blockIOStatistics;
    }

private:
    IInstancePtr Instance_;
    const TDuration StatUpdatePeriod_;

    TSpinLock SpinLock_;
    mutable TInstant LastUpdateTime_ = TInstant::Zero();
    mutable TUsage ResourceUsage_;

    void UpdateResourceUsage() const
    {
        auto now = TInstant::Now();
        if (now > LastUpdateTime_ && now - LastUpdateTime_ > StatUpdatePeriod_) {
            auto resourceUsage = Instance_->GetResourceUsage({
                EStatField::CpuUsageUser,
                EStatField::CpuUsageSystem,
                EStatField::IOReadByte,
                EStatField::IOWriteByte,
                EStatField::IOOperations,
            });

            auto guard = Guard(SpinLock_);
            ResourceUsage_ = resourceUsage;
            LastUpdateTime_ = now;
        }
    }
};

DECLARE_REFCOUNTED_TYPE(TPortoResourceTracker)
DEFINE_REFCOUNTED_TYPE(TPortoResourceTracker)

////////////////////////////////////////////////////////////////////////////////

class TPortoUserJobEnvironment
    : public IUserJobEnvironment
    , private TMemoryTrackerBase
{
public:
    TPortoUserJobEnvironment(
        const TString& slotAbsoluteName,
        IPortoExecutorPtr portoExecutor,
        IInstancePtr instance,
        TDuration blockIOWatchdogPeriod)
        : SlotAbsoluteName_(slotAbsoluteName)
        , BlockIOWatchdogPeriod_(blockIOWatchdogPeriod)
        , PortoExecutor_(std::move(portoExecutor))
        , Instance_(std::move(instance))
        , ResourceTracker_(New<TPortoResourceTracker>(Instance_, TDuration::MilliSeconds(100)))
    { }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        return ResourceTracker_->GetCpuStatistics();
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        return ResourceTracker_->GetBlockIOStatistics();
    }

    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        return BlockIOWatchdogPeriod_;
    }

    virtual void CleanProcesses() override
    {
        Instance_->Kill(SIGKILL);
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        Instance_->SetIOThrottle(operations);
    }

    virtual TMemoryStatistics GetMemoryStatistics() const
    {
        return TMemoryTrackerBase::GetMemoryStatistics();
    }

    virtual i64 GetMaxMemoryUsage() const override
    {
        return MaxMemoryUsage_;
    }

    virtual TProcessBasePtr CreateUserJobProcess(const TString& path, int uid, const TNullable<TString>& coreHandlerSocketPath) override
    {
        static const TString RootFSBinaryDirectory("/ext_bin/");

        if (coreHandlerSocketPath) {
            // We do not want to rely on passing PATH environment to core handler container.
            auto binaryPathOrError = Instance_->HasRoot()
                ? TErrorOr<TString>(RootFSBinaryDirectory + "ytserver-core-forwarder")
                : ResolveBinaryPath("ytserver-core-forwarder");

            if (binaryPathOrError.IsOK()) {
                auto coreHandler = binaryPathOrError.Value() + " \"${CORE_PID}\" 0 \"${CORE_TASK_NAME}\""
                    " 1 /dev/null /dev/null " + coreHandlerSocketPath.Get();

                LOG_DEBUG("Enable core forwarding for porto container (CoreHandler: %v)",
                    coreHandler);
                Instance_->SetCoreDumpHandler(coreHandler);
            } else {
                LOG_ERROR(binaryPathOrError,
                    "Failed to resolve path for ytserver-core-forwarder");
            }
        }

        Instance_->SetIsolate();
        auto adjustedPath = Instance_->HasRoot()
            ? RootFSBinaryDirectory + path
            : path;

        UserId_ = uid;
        Process_ = New<TPortoProcess>(adjustedPath, Instance_, false);
        Process_->AddArguments({"--uid", ::ToString(uid)});

        return Process_;
    }

private:
    const TString SlotAbsoluteName_;
    const TDuration BlockIOWatchdogPeriod_;
    IPortoExecutorPtr PortoExecutor_;
    IInstancePtr Instance_;
    TPortoResourceTrackerPtr ResourceTracker_;
};

DEFINE_REFCOUNTED_TYPE(TPortoUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TPortoJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    TPortoJobProxyEnvironment(TPortoJobEnvironmentConfigPtr config, const TNullable<TRootFS>& rootFS)
        : RootFS_(rootFS)
        , BlockIOWatchdogPeriod_(config->BlockIOWatchdogPeriod)
        , PortoExecutor_(CreatePortoExecutor(config->PortoWaitTime, config->PortoPollPeriod))
        , Self_(GetSelfPortoInstance(PortoExecutor_))
        , ResourceTracker_(New<TPortoResourceTracker>(Self_, TDuration::MilliSeconds(100)))
    {
        PortoExecutor_->SubscribeFailed(BIND(&TPortoJobProxyEnvironment::OnFatalError, MakeWeak(this)));

        auto absoluteName = Self_->GetAbsoluteName();
        // ../yt_jobs_meta/slot_meta_N/job_proxy_ID
        auto jobProxyStart = absoluteName.find_last_of('/');

        SlotAbsoluteName_ = absoluteName.substr(0, jobProxyStart);
    }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        return ResourceTracker_->GetCpuStatistics();
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        return ResourceTracker_->GetBlockIOStatistics();
    }

    virtual void SetCpuShare(double share) override
    {
        WaitFor(PortoExecutor_->SetProperty(SlotAbsoluteName_, "cpu_guarantee", ToString(share) + "c"))
            .ThrowOnError();
    }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) override
    {
        auto containerName = Format("%v/user_job_%v", SlotAbsoluteName_, jobId);
        auto instance = CreatePortoInstance(containerName, PortoExecutor_);
        if (RootFS_) {
            instance->SetRoot(*RootFS_);
        }

        return New<TPortoUserJobEnvironment>(
            SlotAbsoluteName_,
            PortoExecutor_,
            std::move(instance),
            BlockIOWatchdogPeriod_);
    }

private:
    const TNullable<TRootFS> RootFS_;
    const TDuration BlockIOWatchdogPeriod_;
    TString SlotAbsoluteName_;
    IPortoExecutorPtr PortoExecutor_;
    IInstancePtr Self_;
    TPortoResourceTrackerPtr ResourceTracker_;

    void OnFatalError(const TError& error)
    {
        // We cant abort user job (the reason is we need porto to do it),
        // so we will abort job proxy
        LOG_ERROR(error, "Fatal error during porto polling");
        NLogging::TLogManager::Get()->Shutdown();
        _exit(static_cast<int>(EJobProxyExitCode::PortoManagmentFailed));
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoJobProxyEnvironment)

#endif

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(NYTree::INodePtr config, const TNullable<TRootFS>& rootFS)
{

    auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(config);
    switch (environmentConfig->Type) {
        case EJobEnvironmentType::Cgroups:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("Cgroups job environment does not support custom root FS");
            }
            return New<TCGroupsJobProxyEnvironment>(ConvertTo<TCGroupJobEnvironmentConfigPtr>(config));

#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return New<TPortoJobProxyEnvironment>(ConvertTo<TPortoJobEnvironmentConfigPtr>(config), rootFS);
#endif

        case EJobEnvironmentType::Simple:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("Simple job environment does not support custom root FS");
            }
            return nullptr;

        default:
            THROW_ERROR_EXCEPTION("Unable to create resource controller for %Qlv environment",
                environmentConfig->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

