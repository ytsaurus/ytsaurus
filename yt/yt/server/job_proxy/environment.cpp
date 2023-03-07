#include "environment.h"

#include <yt/server/lib/core_dump/public.h>

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/ytlib/job_proxy/private.h>

#include <util/system/fs.h>

#ifdef _linux_
#include <yt/server/lib/containers/porto_executor.h>
#include <yt/server/lib/containers/instance.h>

#include <yt/server/lib/misc/process.h>
#endif

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/ytree/convert.h>

#include <sys/stat.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NContainers;
using namespace NCGroup;
using namespace NExecAgent;
using namespace NJobAgent;
using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// Option cpu.share is limited to [2, 1024], see http://git.kernel.org/cgit/linux/kernel/git/tip/tip.git/tree/kernel/sched/sched.h#n279
// To overcome this limitation we consider one cpu_limit unit as ten cpu.shares units.
static constexpr int CpuShareMultiplier = 10;
static const TString RootFSBinaryDirectory("/ext_bin/");

static const NLogging::TLogger Logger("JobProxyEnvironment");

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

                YT_LOG_DEBUG("Memory statistics collected (Pid: %v, ProcessName: %v, Rss: %v, Shared: %v)",
                    pid,
                    GetProcessName(pid),
                    memoryStatistics.Rss,
                    memoryStatistics.MappedFile);

            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to get memory usage (Pid: %v)", pid);
            }
        }

        try {
            PageFaultCount_ = GetProcessCumulativeMajorPageFaults(Process_->GetProcessId());
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to get page fault count (Pid: %v)", Process_->GetProcessId());
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
        : CGroupsConfig_(std::move(config))
        , Path_(path)
        , CGroups_(path)
    { }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        if (CGroupsConfig_->IsCGroupSupported(TCpuAccounting::Name)) {
            return CGroups_.CpuAccounting.GetStatistics();
        }
        THROW_ERROR_EXCEPTION("CPU accounting cgroup is not supported");
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
            return CGroups_.BlockIO.GetStatistics();
        }
        THROW_ERROR_EXCEPTION("Block IO cgroup is not supported");
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
            YT_LOG_FATAL(ex, "Failed to kill user processes");
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        if (CGroupsConfig_->IsCGroupSupported(TBlockIO::Name)) {
            CGroups_.BlockIO.ThrottleOperations(operations);
        }
    }

    virtual TProcessBasePtr CreateUserJobProcess(
        const TString& path,
        int uid,
        const TUserJobProcessOptions& /*options*/) override
    {
        YT_VERIFY(!Process_);

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
            YT_LOG_FATAL(ex, "Failed to create required cgroups");
        }
        return Process_;
    }

    virtual IInstancePtr GetUserJobInstance() const override
    {
        return nullptr;
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

    virtual void SetCpuLimit(double share) override
    { }

    virtual void EnablePortoMemoryTracking() override
    { }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) override
    {
        return New<TCGroupsUserJobEnvironment>(CGroupsConfig_, "user_job_" + jobId);
    }
};

DECLARE_REFCOUNTED_CLASS(TCGroupsJobProxyEnvironment)
DEFINE_REFCOUNTED_TYPE(TCGroupsJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoResourceTracker
    : public virtual IResourceTracker
{
public:
    TPortoResourceTracker(IInstancePtr instance, TDuration updatePeriod)
        : Instance_(std::move(instance))
        , UpdatePeriod_(updatePeriod)
    { }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        return GetStatistics(
            CachedCpuStatistics_,
            "CPU",
            [&] {
                return TCpuStatistics{
                    .UserTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuUsageUser) / 1000),
                    .SystemTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuUsageSystem) / 1000),
                    .WaitTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuWait) / 1000),
                    .ThrottledTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuThrottled) / 1000),
                    .ContextSwitches = GetFieldOrThrow(ResourceUsage_, EStatField::ContextSwitches)
                };
            });
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        return GetStatistics(
            CachedBlockIOStatistics_,
            "block IO",
            [&] {
                return TBlockIOStatistics{
                    .BytesRead = GetFieldOrThrow(ResourceUsage_, EStatField::IOReadByte),
                    .BytesWritten = GetFieldOrThrow(ResourceUsage_, EStatField::IOWriteByte),
                    .IOTotal = GetFieldOrThrow(ResourceUsage_, EStatField::IOOperations)
                };
            });
    }

    TMemoryStatistics GetMemoryStatistics() const
    {
        return GetStatistics(
            CachedMemoryStatistics_,
            "memory",
            [&] {
                return TMemoryStatistics{
                    .Rss = GetFieldOrThrow(ResourceUsage_, EStatField::Rss),
                    .MappedFile = GetFieldOrThrow(ResourceUsage_, EStatField::MappedFiles),
                    .MajorPageFaults = GetFieldOrThrow(ResourceUsage_, EStatField::MajorFaults)
                };
            });
    }

private:
    const IInstancePtr Instance_;
    const TDuration UpdatePeriod_;

    mutable std::atomic<TInstant> LastUpdateTime_ = {};

    mutable TSpinLock SpinLock_;
    mutable TResourceUsage ResourceUsage_;
    mutable std::optional<TCpuStatistics> CachedCpuStatistics_;
    mutable std::optional<TMemoryStatistics> CachedMemoryStatistics_;
    mutable std::optional<TBlockIOStatistics> CachedBlockIOStatistics_;

    static ui64 GetFieldOrThrow(const TResourceUsage& usage, EStatField field)
    {
        auto it = usage.find(field);
        if (it == usage.end()) {
            THROW_ERROR_EXCEPTION("Resource usage is missing %Qlv field",
                field);
        }
        const auto& errorOrValue = it->second;
        if (errorOrValue.FindMatching(NContainers::EPortoErrorCode::NotSupported)) {
            return 0;
        }
        if (!errorOrValue.IsOK()) {
            THROW_ERROR_EXCEPTION("Error getting %Qlv resource usage field",
                field)
                << errorOrValue;
        }
        return errorOrValue.Value();
    }

    template <class T, class F>
    T GetStatistics(
        std::optional<T>& cachedStatistics,
        const TString& statisticsKind,
        F func) const
    {
        UpdateResourceUsage();

        auto guard = Guard(SpinLock_);
        try {
            auto newStatistics = func();
            cachedStatistics = newStatistics;
            return newStatistics;
        } catch (const std::exception& ex) {
            if (!cachedStatistics) {
                THROW_ERROR_EXCEPTION("Unable to get %v statistics",
                    statisticsKind)
                    << ex;
            }
            YT_LOG_WARNING(ex, "Unable to get %v statistics; using the last one",
                statisticsKind);
            return *cachedStatistics;
        }
    }

    void UpdateResourceUsage() const
    {
        if (TInstant::Now() - LastUpdateTime_.load() > UpdatePeriod_) {
            DoUpdateResourceUsage();
            LastUpdateTime_.store(TInstant::Now());
        }
    }

    void DoUpdateResourceUsage() const
    {
        auto resourceUsage = Instance_->GetResourceUsage({
            EStatField::CpuUsageUser,
            EStatField::CpuUsageSystem,
            EStatField::CpuWait,
            EStatField::CpuThrottled,
            EStatField::ContextSwitches,
            EStatField::IOReadByte,
            EStatField::IOWriteByte,
            EStatField::IOOperations,
            EStatField::Rss,
            EStatField::MappedFiles,
            EStatField::MajorFaults
        });

        {
            auto guard = Guard(SpinLock_);
            ResourceUsage_ = resourceUsage;
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
        TDuration blockIOWatchdogPeriod,
        bool usePortoMemoryTracking)
        : SlotAbsoluteName_(slotAbsoluteName)
        , BlockIOWatchdogPeriod_(blockIOWatchdogPeriod)
        , UsePortoMemoryTracking_(usePortoMemoryTracking)
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
        try {
            Instance_->Stop();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to stop user container");
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        Instance_->SetIOThrottle(operations);
    }

    virtual TMemoryStatistics GetMemoryStatistics() const
    {
        if (UsePortoMemoryTracking_) {
            auto memoryStatistics = ResourceTracker_->GetMemoryStatistics();
            if (memoryStatistics.Rss + memoryStatistics.MappedFile > MaxMemoryUsage_) {
                MaxMemoryUsage_ = memoryStatistics.Rss + memoryStatistics.MappedFile;
            }
            return memoryStatistics;
        } else {
            return TMemoryTrackerBase::GetMemoryStatistics();
        }
    }

    virtual i64 GetMaxMemoryUsage() const override
    {
        return MaxMemoryUsage_;
    }

    virtual TProcessBasePtr CreateUserJobProcess(
        const TString& path,
        int uid,
        const TUserJobProcessOptions& options) override
    {
        TString gpuCorePipeFile;

        if (options.CoreWatcherDirectory) {
            // NB: Core watcher expects core info file to be created before
            // core pipe file.
            auto coreDirectory = *options.CoreWatcherDirectory;
            auto coreInfoFile = coreDirectory + "/core_\"${CORE_PID}\".info";
            auto corePipeFile = coreDirectory + "/core_\"${CORE_PID}\".pipe";
            auto bashCoreHandler =
                "echo \"${CORE_TASK_NAME}\" >" + coreInfoFile + " && " +
                "echo \"${CORE_PID}\" >>" + coreInfoFile + " && " +
                "echo \"${CORE_TID}\" >>" + coreInfoFile + " && " +
                "echo \"${CORE_SIG}\" >>" + coreInfoFile + " && " +
                "echo \"${CORE_CONTAINER}\" >>" + coreInfoFile + " && " +
                "echo \"${CORE_DATETIME}\" >>" + coreInfoFile + " && " +
                "mkfifo " + corePipeFile + " && " +
                "cat >" + corePipeFile;
            auto coreHandler = "bash -c \'" + bashCoreHandler + "\'";
            YT_LOG_DEBUG("Enabling core forwarding for Porto container (CoreHandler: %v)",
                coreHandler);
            Instance_->SetCoreDumpHandler(coreHandler);

            if (options.EnableCudaGpuCoreDump) {
                gpuCorePipeFile = NFS::CombinePaths(coreDirectory, NCoreDump::CudaGpuCoreDumpPipeName);
                YT_LOG_DEBUG("Creating pipe for GPU core dumps (GpuCorePipeFile: %v)",
                    gpuCorePipeFile);
                if (mkfifo(gpuCorePipeFile.c_str(), 0666) == -1) {
                    THROW_ERROR_EXCEPTION("Failed to create CUDA GPU core dump pipe")
                        << TErrorAttribute("path", gpuCorePipeFile)
                        << TError::FromSystem();
                }
            }
        }

        Instance_->SetEnablePorto(options.EnablePorto);
        if (options.EnablePorto == EEnablePorto::Full) {
            Instance_->SetIsolate(false);
        } else {
            Instance_->SetIsolate(true);
        }

        if (UsePortoMemoryTracking_) {
            // NB(psushin): typically we don't use memory cgroups for memory usage tracking, since memory cgroups are expensive and
            // shouldn't be created too often. But for special reasons (e.g. Nirvana) we still make a backdoor to track memory via cgroups.
            // More about malicious cgroups here https://st.yandex-team.ru/YTADMIN-8554#1516791797000.
            // Future happiness here https://st.yandex-team.ru/KERNEL-141.
            Instance_->EnableMemoryTracking();
        }

        auto adjustedPath = Instance_->HasRoot()
            ? RootFSBinaryDirectory + path
            : path;

        UserId_ = uid;
        Process_ = New<TPortoProcess>(adjustedPath, Instance_, false);
        Process_->AddArguments({"--uid", ::ToString(uid)});
        if (options.EnableCudaGpuCoreDump) {
            Process_->AddArguments({"--env", "CUDA_ENABLE_COREDUMP_ON_EXCEPTION=1"});
            Process_->AddArguments({"--env", Format("CUDA_COREDUMP_FILE=%v", gpuCorePipeFile)});
        }

        return Process_;
    }

    virtual IInstancePtr GetUserJobInstance() const override
    {
        return Instance_;
    }

private:
    const TString SlotAbsoluteName_;
    const TDuration BlockIOWatchdogPeriod_;
    const bool UsePortoMemoryTracking_;
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr Instance_;
    const TPortoResourceTrackerPtr ResourceTracker_;
};

DECLARE_REFCOUNTED_CLASS(TPortoUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TPortoUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TPortoJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    TPortoJobProxyEnvironment(
        TPortoJobEnvironmentConfigPtr config,
        const std::optional<TRootFS>& rootFS,
        std::vector<TString> gpuDevices,
        std::vector<TIP6Address> networkAddresses,
        const std::optional<TString>& hostName)
        : RootFS_(rootFS)
        , GpuDevices_(std::move(gpuDevices))
        , BlockIOWatchdogPeriod_(config->BlockIOWatchdogPeriod)
        , PortoExecutor_(CreatePortoExecutor(config->PortoExecutor, "environ"))
        , Self_(GetSelfPortoInstance(PortoExecutor_))
        , ResourceTracker_(New<TPortoResourceTracker>(Self_, TDuration::MilliSeconds(100)))
        , NetworkAddresses_(std::move(networkAddresses))
        , SlotAbsoluteName_(GetAbsoluteName(Self_))
        , HostName_(hostName)
    {
        PortoExecutor_->SubscribeFailed(BIND(&TPortoJobProxyEnvironment::OnFatalError, MakeWeak(this)));
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
        WaitFor(PortoExecutor_->SetContainerProperty(SlotAbsoluteName_, "cpu_guarantee", ToString(share) + "c"))
            .ThrowOnError();
    }

    virtual void SetCpuLimit(double share) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotAbsoluteName_, "cpu_limit", ToString(share) + "c"))
            .ThrowOnError();
    }

    virtual void EnablePortoMemoryTracking() override
    {
        UsePortoMemoryTracking_ = true;
    }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TString& jobId) override
    {
        auto containerName = Format("%v/uj_%v", SlotAbsoluteName_, jobId);
        auto instance = CreatePortoInstance(containerName, PortoExecutor_);
        if (RootFS_) {
            auto newPath = NFS::CombinePaths(RootFS_->RootPath, "slot");
            YT_LOG_INFO("Mount slot directory into container (Path: %v)", newPath);

            THashMap<TString, TString> properties;
            properties["backend"] = "rbind";
            properties["storage"] = NFs::CurrentWorkingDirectory();
            auto volumePath = WaitFor(PortoExecutor_->CreateVolume(newPath, properties))
                .ValueOrThrow();

            RootFS_->Binds.emplace_back(TBind {
                ResolveBinaryPath(ExecProgramName).ValueOrThrow(),
                RootFSBinaryDirectory + ExecProgramName,
                true});

            instance->SetRoot(*RootFS_);
        }

        std::vector<TDevice> devices;
        for (const auto& descriptor : ListGpuDevices()) {
            const auto& deviceName = descriptor.DeviceName;
            if (std::find(GpuDevices_.begin(), GpuDevices_.end(), deviceName) == GpuDevices_.end()) {
                devices.emplace_back(TDevice{deviceName, false});
            }
        }

        if (HostName_) {
            instance->SetHostName(*HostName_);
        }

        if (!NetworkAddresses_.empty()) {
            instance->SetNet("L3 veth0");

            TString ipProperty;
            for (const auto& address : NetworkAddresses_) {
                if (!ipProperty.empty()) {
                    ipProperty += ";";
                }
                ipProperty += "veth0 " + ToString(address);
            }
            instance->SetIP(ipProperty);
        }

        // Restrict access to devices, that are not explicitly granted.
        instance->SetDevices(std::move(devices));

        return New<TPortoUserJobEnvironment>(
            SlotAbsoluteName_,
            PortoExecutor_,
            std::move(instance),
            BlockIOWatchdogPeriod_,
            UsePortoMemoryTracking_);
    }

private:
    std::optional<TRootFS> RootFS_;
    const std::vector<TString> GpuDevices_;
    const TDuration BlockIOWatchdogPeriod_;
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr Self_;
    const TPortoResourceTrackerPtr ResourceTracker_;
    const std::vector<TIP6Address> NetworkAddresses_;
    const TString SlotAbsoluteName_;
    const std::optional<TString> HostName_;

    bool UsePortoMemoryTracking_ = false;

    static TString GetAbsoluteName(const IInstancePtr& instance)
    {
        auto absoluteName = instance->GetAbsoluteName();
        // ../yt_jobs_meta/slot_meta_N/job_proxy_ID
        auto jobProxyStart = absoluteName.find_last_of('/');
        return absoluteName.substr(0, jobProxyStart);
    }

    void OnFatalError(const TError& error)
    {
        // We can't abort the user job (the reason is we need Porto to do this),
        // so we will abort the job proxy.
        YT_LOG_ERROR(error, "Fatal error during Porto polling");
        NLogging::TLogManager::Get()->Shutdown();
        _exit(static_cast<int>(EJobProxyExitCode::PortoManagementFailed));
    }
};

DECLARE_REFCOUNTED_CLASS(TPortoJobProxyEnvironment)
DEFINE_REFCOUNTED_TYPE(TPortoJobProxyEnvironment)

#endif

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(
    NYTree::INodePtr config,
    const std::optional<TRootFS>& rootFS,
    std::vector<TString> gpuDevices,
    const std::vector<TIP6Address>& networkAddresses,
    const std::optional<TString>& hostName)
{

    auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(config);
    switch (environmentConfig->Type) {
        case EJobEnvironmentType::Cgroups:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("CGroups job environment does not support custom root FS");
            }

            if (!gpuDevices.empty()) {
                YT_LOG_WARNING("CGroups job environment does not support GPU device isolation (Devices: %v)", gpuDevices);
            }

            if (!networkAddresses.empty() || hostName) {
                YT_LOG_WARNING("CGroups job environment does not support network isolation (NetworkAddresses: %v, Hostname: %v)",
                    networkAddresses,
                    hostName);
            }

            return New<TCGroupsJobProxyEnvironment>(ConvertTo<TCGroupJobEnvironmentConfigPtr>(config));

#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return New<TPortoJobProxyEnvironment>(
                ConvertTo<TPortoJobEnvironmentConfigPtr>(config),
                rootFS,
                std::move(gpuDevices),
                networkAddresses,
                hostName);
#endif

        case EJobEnvironmentType::Simple:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("Simple job environment does not support custom root FS");
            }

            if (!gpuDevices.empty()) {
                YT_LOG_WARNING("Simple job environment does not support GPU device isolation (Devices: %v)", gpuDevices);
            }

            if (!networkAddresses.empty() || hostName) {
                YT_LOG_WARNING("Simple job environment does not support network isolation (NetworkAddresses: %v, HostName: %v)",
                    networkAddresses,
                    hostName);
            }
            return nullptr;

        default:
            THROW_ERROR_EXCEPTION("Unable to create resource controller for %Qlv environment",
                environmentConfig->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

