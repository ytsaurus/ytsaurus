#include "resource_controller.h"

#include <yt/server/exec_agent/config.h>

#ifdef _linux_
#include <yt/server/containers/container_manager.h>
#include <yt/server/containers/instance.h>

#include <yt/server/misc/process.h>
#endif

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/proc.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NJobProxy {


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

class TCGroupResourceController
    : public IResourceController
{
public:
    TCGroupResourceController(
        TCGroupJobEnvironmentConfigPtr config,
        const TString& path = TString())
        : CGroupsConfig_(config)
        , CGroups_(path)
        , Path_(path)
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

    virtual TMemoryStatistics GetMemoryStatistics() const override
    {
        auto tasks = CGroups_.Freezer.GetTasks();
        TMemoryStatistics memoryStatistics = {0, 0, 0};
        for (auto task : tasks) {
            try {
                auto memoryUsage = GetProcessMemoryUsage(task);
                memoryStatistics.Rss += memoryUsage.Rss;
                memoryStatistics.MappedFile += memoryUsage.Shared;
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get memory usage (Pid %v)", task);
            }
        }

        if (Process_ && Process_->GetProcessId() > 0) {
            try {
                PageFaultCount_ = GetProcessCumulativeMajorPageFaults(Process_->GetProcessId());
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get page fault count (Pid %v)", Process_->GetProcessId());
            }
        }

        memoryStatistics.MajorPageFaults = PageFaultCount_;
        return memoryStatistics;
    }

    virtual i64 GetMaxMemoryUsage() const override
    {
        return MaxMemoryUsage;
    }

    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        return CGroupsConfig_->BlockIOWatchdogPeriod;
    }

    virtual void KillAll() override
    {
        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may still be alive.
            RunKiller(CGroups_.Freezer.GetFullPath());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to kill user processes");
        }
    }

    virtual void SetCpuShare(double share) override
    {
        if (CGroupsConfig_->IsCGroupSupported(TCpu::Name)) {
            CGroups_.Cpu.SetShare(share * CpuShareMultiplier);
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
            CGroups_.BlockIO.ThrottleOperations(operations);
        }
    }

    virtual IResourceControllerPtr CreateSubcontroller(const TString& name) override
    {
        return New<TCGroupResourceController>(CGroupsConfig_, Path_ + name);
    }

    virtual TProcessBasePtr CreateControlledProcess(const TString& path, const TNullable<TString>& coreDumpHandler) override
    {
        YCHECK(!coreDumpHandler);
        YCHECK(!Process_);

        Process_ = New<TSimpleProcess>(path, false);
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
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create required cgroups");
        }
        return Process_;
    }

private:
    const TCGroupJobEnvironmentConfigPtr CGroupsConfig_;

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
    } CGroups_;

    const TString Path_;
    TIntrusivePtr<TSimpleProcess> Process_;
    i64 MaxMemoryUsage = 0;
    mutable i64 PageFaultCount_ = 0;
};

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

class TPortoResourceController
    : public IResourceController
{
public:
    static IResourceControllerPtr Create(TPortoJobEnvironmentConfigPtr config, const TNullable<TRootFS>& rootFS)
    {
        auto resourceController = New<TPortoResourceController>(config->BlockIOWatchdogPeriod, config->UseResourceLimits, rootFS);
        resourceController->Init(config->PortoWaitTime, config->PortoPollPeriod);
        return resourceController;
    }

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

    virtual TMemoryStatistics GetMemoryStatistics() const override
    {
        UpdateResourceUsage();

        auto guard = Guard(SpinLock_);
        auto error = CheckErrors(ResourceUsage_,
            EStatField::Rss,
            EStatField::MappedFiles,
            EStatField::MajorFaults);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to get memory statistics");
        TMemoryStatistics memoryStatistics;
        memoryStatistics.Rss = ResourceUsage_[EStatField::Rss].Value();
        memoryStatistics.MappedFile = ResourceUsage_[EStatField::MappedFiles].Value();
        memoryStatistics.MajorPageFaults = ResourceUsage_[EStatField::MajorFaults].Value();
        return memoryStatistics;
    }

    virtual i64 GetMaxMemoryUsage() const override
    {
        UpdateResourceUsage();

        auto guard = Guard(SpinLock_);
        auto error = CheckErrors(ResourceUsage_,
            EStatField::MaxMemoryUsage);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to get max memory usage");
        return ResourceUsage_[EStatField::MaxMemoryUsage].Value();
    }

    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        return BlockIOWatchdogPeriod_;
    }

    virtual void KillAll() override
    {
        // Kill only first process in container,
        // others will be killed automaticaly
        try {
            Container_->Kill(SIGKILL);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Failed to kill user container");
        }
    }

    virtual void SetCpuShare(double share) override
    {
        if (UseResourceLimits_) {
            Container_->SetCpuShare(share);
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        if (UseResourceLimits_) {
            Container_->SetIOThrottle(operations);
        }
    }

    virtual IResourceControllerPtr CreateSubcontroller(const TString& name) override
    {
        auto instance = ContainerManager_->CreateInstance();
        if (RootFS_) {
            instance->SetRoot(*RootFS_);
        }
        return New<TPortoResourceController>(ContainerManager_, instance, BlockIOWatchdogPeriod_, UseResourceLimits_);
    }

    virtual TProcessBasePtr CreateControlledProcess(const TString& path, const TNullable<TString>& coreDumpHandler) override
    {
        if (coreDumpHandler) {
            LOG_DEBUG("Enable core forwarding for porto container (CoreHandler: %v)",
                coreDumpHandler.Get());
            Container_->SetCoreDumpHandler(coreDumpHandler.Get());
        }
        return New<TPortoProcess>(path, Container_, false);
    }

private:
    TSpinLock SpinLock_;
    IContainerManagerPtr ContainerManager_;
    IInstancePtr Container_;
    mutable TInstant LastUpdateTime_ = TInstant::Zero();
    mutable TUsage ResourceUsage_;

    const TDuration StatUpdatePeriod_;
    const TDuration BlockIOWatchdogPeriod_;
    const bool UseResourceLimits_;
    const TNullable<TRootFS> RootFS_;

    TPortoResourceController(
        TDuration blockIOWatchdogPeriod,
        bool useResourceLimits,
        const TNullable<TRootFS>& rootFS)
        : StatUpdatePeriod_(TDuration::MilliSeconds(100))
        , BlockIOWatchdogPeriod_(blockIOWatchdogPeriod)
        , UseResourceLimits_(useResourceLimits)
        , RootFS_(rootFS)
    { }

    TPortoResourceController(
        IContainerManagerPtr containerManager,
        IInstancePtr instance,
        TDuration blockIOWatchdogPeriod,
        bool useResourceLimits)
        : ContainerManager_(containerManager)
        , Container_(instance)
        , BlockIOWatchdogPeriod_(blockIOWatchdogPeriod)
        , UseResourceLimits_(useResourceLimits)
    { }

    void UpdateResourceUsage() const
    {
        auto now = TInstant::Now();
        if (now > LastUpdateTime_ && now - LastUpdateTime_ > StatUpdatePeriod_) {
            auto resourceUsage = Container_->GetResourceUsage({
                EStatField::CpuUsageUser,
                EStatField::CpuUsageSystem,
                EStatField::IOReadByte,
                EStatField::IOWriteByte,
                EStatField::IOOperations,
                EStatField::Rss,
                EStatField::MappedFiles,
                EStatField::MajorFaults,
                EStatField::MaxMemoryUsage
            });

            auto guard = Guard(SpinLock_);
            ResourceUsage_ = resourceUsage;
            LastUpdateTime_ = now;
        }
    }

    void OnFatalError(const TError& error)
    {
        // We cant abort user job (the reason is we need porto to do it),
        // so we will abort job proxy
        LOG_ERROR(error, "Fatal error during porto polling");
        NLogging::TLogManager::Get()->Shutdown();
        _exit(static_cast<int>(EJobProxyExitCode::PortoManagmentFailed));
    }

    void Init(TDuration waitTime, TDuration pollPeriod)
    {
        auto errorHandler = BIND(&TPortoResourceController::OnFatalError, MakeStrong(this));
        ContainerManager_ = CreatePortoManager(
            "",
            Null,
            errorHandler,
            { ECleanMode::None,
            waitTime,
            pollPeriod });
        Container_ = ContainerManager_->GetSelfInstance();
        UpdateResourceUsage();
    }

    DECLARE_NEW_FRIEND();
};

#endif

////////////////////////////////////////////////////////////////////////////////

IResourceControllerPtr CreateResourceController(NYTree::INodePtr config, const TNullable<TRootFS>& rootFS)
{
    auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(config);
    switch (environmentConfig->Type) {
        case EJobEnvironmentType::Cgroups:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("Cgroups job environment does not support custom root FS");
            }
            return New<TCGroupResourceController>(ConvertTo<TCGroupJobEnvironmentConfigPtr>(config));

#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return TPortoResourceController::Create(ConvertTo<TPortoJobEnvironmentConfigPtr>(config), rootFS);
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

