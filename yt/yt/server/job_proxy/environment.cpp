#include "environment.h"

#include <yt/yt/server/lib/core_dump/public.h>

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <util/system/fs.h>

#ifdef _linux_
#include <yt/yt/server/lib/containers/porto_executor.h>
#include <yt/yt/server/lib/containers/instance.h>
#endif

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/convert.h>

#include <sys/stat.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NContainers;
using namespace NCGroup;
using namespace NExecNode;
using namespace NJobAgent;
using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TString RootFSBinaryDirectory("/ext_bin/");

#ifdef _linux_
static constexpr auto ResourceUsageUpdatePeriod = TDuration::MilliSeconds(1000);
#endif

static const NLogging::TLogger Logger("JobProxyEnvironment");

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
                // NB: Job proxy uses last sample of CPU statistics but we are interested in peak thread count value.
                PeakThreadCount_ = std::max<ui64>(PeakThreadCount_, GetFieldOrThrow(ResourceUsage_, EStatField::ThreadCount));

                return TCpuStatistics{
                    .UserTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuUsageUser) / 1000),
                    .SystemTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuUsageSystem) / 1000),
                    .WaitTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuWait) / 1000),
                    .ThrottledTime = TDuration::MicroSeconds(GetFieldOrThrow(ResourceUsage_, EStatField::CpuThrottled) / 1000),
                    .ContextSwitches = GetFieldOrThrow(ResourceUsage_, EStatField::ContextSwitches),
                    .PeakThreadCount = PeakThreadCount_,
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

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    mutable TResourceUsage ResourceUsage_;
    mutable std::optional<TCpuStatistics> CachedCpuStatistics_;
    mutable std::optional<TMemoryStatistics> CachedMemoryStatistics_;
    mutable std::optional<TBlockIOStatistics> CachedBlockIOStatistics_;

    mutable ui64 PeakThreadCount_ = 0;

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
            EStatField::MajorFaults,
            EStatField::ThreadCount,
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
{
public:
    TPortoUserJobEnvironment(
        TGuid jobId,
        TPortoJobEnvironmentConfigPtr config,
        const TUserJobEnvironmentOptions& options,
        const TString& slotContainerName,
        IPortoExecutorPtr portoExecutor)
        : JobId_(jobId)
        , Config_(std::move(config))
        , Options_(options)
        , SlotContainerName_(slotContainerName)
        , PortoExecutor_(std::move(portoExecutor))
    { 
        if (Options_.EnableCudaGpuCoreDump && Options_.SlotCoreWatcherDirectory) {
            auto slotGpuCorePipeFile = NFS::CombinePaths(*Options_.SlotCoreWatcherDirectory, NCoreDump::CudaGpuCoreDumpPipeName);
            Envirnoment_.push_back("CUDA_ENABLE_COREDUMP_ON_EXCEPTION=1");
            Envirnoment_.push_back(Format("CUDA_COREDUMP_FILE=%v", slotGpuCorePipeFile));
        }
        for (const auto& networkAddress : Options_.NetworkAddresses) {
            Envirnoment_.push_back(Format("YT_IP_ADDRESS_%v=%v", to_upper(networkAddress->Name), networkAddress->Address));
        }
    }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        if (auto resourceTracker = ResourceTracker_.Load()) {
            return resourceTracker->GetCpuStatistics();
        } else {
            return {};
        }
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        if (auto resourceTracker = ResourceTracker_.Load()) {
            return resourceTracker->GetBlockIOStatistics();
        } else {
            return {};
        }
    }

    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        return Config_->BlockIOWatchdogPeriod;
    }

    virtual void CleanProcesses() override
    {
        try {
            if (auto instance = GetUserJobInstance()) {
                instance->Stop();    
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to stop user container");
        }
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        if (auto instance = GetUserJobInstance()) {
            instance->SetIOThrottle(operations);    
        }
    }

    virtual std::optional<TMemoryStatistics> GetMemoryStatistics() const override
    {
        auto resourceTracker = ResourceTracker_.Load();
        if (Options_.EnablePortoMemoryTracking && resourceTracker) {
            return resourceTracker->GetMemoryStatistics();
        } else {
            return std::nullopt;
        }
    }

    virtual TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory) override
    {
        auto containerName = Config_->UseShortContainerNames
            ? Format("%v/uj", SlotContainerName_)
            : Format("%v/uj_%v-%v", SlotContainerName_, IntToString<16>(JobId_.Parts32[3]), IntToString<16>(JobId_.Parts32[2]));

        auto launcher = CreatePortoInstanceLauncher(containerName, PortoExecutor_);

        auto portoUser = *WaitFor(PortoExecutor_->GetContainerProperty(SlotContainerName_, "user"))
            .ValueOrThrow();
        launcher->SetUser(portoUser);

        if (Options_.RootFS) {
            auto rootFS = *Options_.RootFS;
            auto newPath = NFS::CombinePaths(rootFS.RootPath, "slot");
            YT_LOG_INFO("Mount slot directory into container (Path: %v)", newPath);

            THashMap<TString, TString> properties;
            properties["backend"] = "rbind";
            properties["storage"] = NFs::CurrentWorkingDirectory();
            auto volumePath = WaitFor(PortoExecutor_->CreateVolume(newPath, properties))
                .ValueOrThrow();

            // TODO(gritukan): ytserver-exec can be resolved into something strange in tests,
            // so let's live with exec in layer for a while.
            if (!Config_->UseExecFromLayer) {
                rootFS.Binds.push_back(TBind {
                    ResolveBinaryPath(ExecProgramName).ValueOrThrow(),
                    RootFSBinaryDirectory + ExecProgramName,
                    true});
            }

            launcher->SetRoot(rootFS);
        }

        std::vector<TDevice> devices;
        for (const auto& descriptor : ListGpuDevices()) {
            const auto& deviceName = descriptor.DeviceName;
            if (std::find(Options_.GpuDevices.begin(), Options_.GpuDevices.end(), deviceName) == Options_.GpuDevices.end()) {
                devices.push_back(TDevice{
                    .DeviceName = deviceName,
                    .Enabled = false});
            }
        }

        if (!devices.empty() && NFS::Exists("/dev/kvm")) {	
            devices.push_back(TDevice{
                .DeviceName = "/dev/kvm",
                .Enabled = true});
        }

        // Restrict access to devices, that are not explicitly granted.
        launcher->SetDevices(std::move(devices));

        if (Options_.SlotCoreWatcherDirectory) {
            // NB: Core watcher expects core info file to be created before
            // core pipe file.
            auto slotCoreDirectory = *Options_.SlotCoreWatcherDirectory;
            auto coreDirectory = *Options_.CoreWatcherDirectory;
            auto slotCoreInfoFile = slotCoreDirectory + "/core_\"${CORE_PID}\".info";
            auto slotCorePipeFile = slotCoreDirectory + "/core_\"${CORE_PID}\".pipe";
            auto bashCoreHandler =
                "echo \"${CORE_TASK_NAME}\" >" + slotCoreInfoFile + " && " +
                "echo \"${CORE_PID}\" >>" + slotCoreInfoFile + " && " +
                "echo \"${CORE_TID}\" >>" + slotCoreInfoFile + " && " +
                "echo \"${CORE_SIG}\" >>" + slotCoreInfoFile + " && " +
                "echo \"${CORE_CONTAINER}\" >>" + slotCoreInfoFile + " && " +
                "echo \"${CORE_DATETIME}\" >>" + slotCoreInfoFile + " && " +
                "mkfifo " + slotCorePipeFile + " && " +
                "cat >" + slotCorePipeFile;
            auto coreHandler = "bash -c \'" + bashCoreHandler + "\'";
            YT_LOG_DEBUG("Enabling core forwarding for Porto container (CoreHandler: %v)",
                coreHandler);
            launcher->SetCoreDumpHandler(coreHandler);

            if (Options_.EnableCudaGpuCoreDump) {
                auto slotGpuCorePipeFile = NFS::CombinePaths(slotCoreDirectory, NCoreDump::CudaGpuCoreDumpPipeName);
                auto gpuCorePipeFile = NFS::CombinePaths(coreDirectory, NCoreDump::CudaGpuCoreDumpPipeName);
                YT_LOG_DEBUG("Creating pipe for GPU core dumps (SlotGpuCorePipeFile: %v, GpuCorePipeFile: %v)",
                    slotGpuCorePipeFile,
                    gpuCorePipeFile);
                if (mkfifo(gpuCorePipeFile.c_str(), 0666) == -1) {
                    THROW_ERROR_EXCEPTION("Failed to create CUDA GPU core dump pipe")
                        << TErrorAttribute("path", gpuCorePipeFile)
                        << TError::FromSystem();
                }
            }
        }

        if (Options_.HostName) {
            launcher->SetHostName(*Options_.HostName);
        }

        //! There is no HBF in test environment, so setting IP addresses to
        //! user job will cause multiple problems during container startup.
        if (!Options_.NetworkAddresses.empty()) {
            std::vector<TIP6Address> addresses;
            addresses.reserve(Options_.NetworkAddresses.size());
            for (const auto& address : Options_.NetworkAddresses) {
                addresses.push_back(address->Address);
            }

            launcher->SetIPAddresses(addresses);
        }

        launcher->SetEnablePorto(Options_.EnablePorto);
        launcher->SetIsolate(Options_.EnablePorto != EEnablePorto::Full);
        
        if (Options_.EnablePortoMemoryTracking) {
            // NB(psushin): typically we don't use memory cgroups for memory usage tracking, since memory cgroups are expensive and
            // shouldn't be created too often. But for special reasons (e.g. Nirvana) we still make a backdoor to track memory via cgroups.
            // More about malicious cgroups here https://st.yandex-team.ru/YTADMIN-8554#1516791797000.
            // Future happiness here https://st.yandex-team.ru/KERNEL-141.
            launcher->EnableMemoryTracking();
        }

        launcher->SetThreadLimit(Options_.ThreadLimit);
        launcher->SetCwd(workingDirectory);

        auto adjustedPath = launcher->HasRoot()
            ? RootFSBinaryDirectory + path
            : ResolveBinaryPath(path).ValueOrThrow();

        auto instance = WaitFor(launcher->Launch(adjustedPath, arguments, {}))
            .ValueOrThrow();

        auto finishedFuture = instance->Wait();

        Instance_.Store(instance);

        // Now instance is finally created and we can instantiate resource tracker.
        ResourceTracker_.Store(New<TPortoResourceTracker>(instance, ResourceUsageUpdatePeriod));
        return finishedFuture;
    }

    virtual IInstancePtr GetUserJobInstance() const override
    {
        return Instance_.Load();
    }

    const std::vector<TString>& GetEnvironmentVariables() const override
    {
        return Envirnoment_;
    }

    std::vector<pid_t> GetJobPids() const override
    {
        if (auto instance = GetUserJobInstance()) {
            return instance->GetPids();
        } else {
            return {};
        }
    }

private:
    const TGuid JobId_;
    const TPortoJobEnvironmentConfigPtr Config_;
    const TUserJobEnvironmentOptions Options_;
    const TString SlotContainerName_;
    const IPortoExecutorPtr PortoExecutor_;
    
    std::vector<TString> Envirnoment_;

    TAtomicObject<IInstancePtr> Instance_;
    TAtomicObject<TPortoResourceTrackerPtr> ResourceTracker_;
};

DECLARE_REFCOUNTED_CLASS(TPortoUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TPortoUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TPortoJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    TPortoJobProxyEnvironment(TPortoJobEnvironmentConfigPtr config)
        : Config_(std::move(config))
        , PortoExecutor_(CreatePortoExecutor(Config_->PortoExecutor, "environ"))
        , Self_(GetSelfPortoInstance(PortoExecutor_))
        , ResourceTracker_(New<TPortoResourceTracker>(Self_, ResourceUsageUpdatePeriod))
        , SlotContainerName_(*Self_->GetParentName())
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

    virtual void SetCpuGuarantee(double value) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotContainerName_, "cpu_guarantee", ToString(value) + "c"))
            .ThrowOnError();
    }

    virtual void SetCpuLimit(double value) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotContainerName_, "cpu_limit", ToString(value) + "c"))
            .ThrowOnError();
    }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TGuid jobId, 
        const TUserJobEnvironmentOptions& options) override
    {  
        return New<TPortoUserJobEnvironment>(
            jobId,
            Config_,
            options,
            SlotContainerName_,
            PortoExecutor_);
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr Self_;
    const TPortoResourceTrackerPtr ResourceTracker_;
    const TString SlotContainerName_;


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

class TSimpleUserJobEnvironment
    : public IUserJobEnvironment
{
public:
    virtual TDuration GetBlockIOWatchdogPeriod() const override
    {
        // No IO watchdog for simple job environment.
        return TDuration::Max();
    }

    virtual std::optional<TMemoryStatistics> GetMemoryStatistics() const override
    {
        return std::nullopt;
    }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        return {};
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        return {};
    }

    virtual void CleanProcesses() override 
    {
        if (auto process = Process_.Load()) {
            process->Kill(9);
        }
    }

    virtual void SetIOThrottle(i64 /* operations */) override
    {
        // Noop.
    }

    virtual TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory) override
    {
        auto process = New<TSimpleProcess>(path, false);
        process->AddArguments(arguments);
        process->SetWorkingDirectory(workingDirectory);
        Process_.Store(process);
        return process->Spawn();
    }

    virtual NContainers::IInstancePtr GetUserJobInstance() const override
    {
        return nullptr;
    }

    virtual std::vector<pid_t> GetJobPids() const override
    {
        if (auto process = Process_.Load()) {
            auto pid = process->GetProcessId();
            return GetPidsUnderParent(pid);
        }
        
        return {};
    }

    //! Returns the list of environment-specific environment variables in key=value format.
    virtual const std::vector<TString>& GetEnvironmentVariables() const override
    {
        static std::vector<TString> emptyEnvironment;
        return emptyEnvironment;
    }

private:
    TAtomicObject<TProcessBasePtr> Process_;
};

DECLARE_REFCOUNTED_CLASS(TSimpleUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TSimpleUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    virtual void SetCpuGuarantee(double /* value */) override
    {
        YT_LOG_WARNING("Cpu guarantees are not supported in simple job environment");
    }
    
    virtual void SetCpuLimit(double /* value */) override
    {
        YT_LOG_WARNING("Cpu limits are not supported in simple job environment");
    }

    virtual TCpuStatistics GetCpuStatistics() const override
    {
        return {};
    }

    virtual TBlockIOStatistics GetBlockIOStatistics() const override
    {
        return {};
    }

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TGuid /* jobId */,
        const TUserJobEnvironmentOptions& options) override
    {
        if (options.RootFS) {
            THROW_ERROR_EXCEPTION("Root FS isolation is not supported in simple job environment");
        }

        if (!options.GpuDevices.empty()) {
            // This could only happen in tests, e.g. TestSchedulerGpu.
            YT_LOG_WARNING("GPU devices are not supported in simple job environment");
        }

        if (options.EnablePortoMemoryTracking) {
            THROW_ERROR_EXCEPTION("Porto memory tracking is not supported in simple job environment");
        }

        return New<TSimpleUserJobEnvironment>();
    }
};

DECLARE_REFCOUNTED_CLASS(TSimpleJobProxyEnvironment)
DEFINE_REFCOUNTED_TYPE(TSimpleJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(NYTree::INodePtr config)
{
    auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(config);
    switch (environmentConfig->Type) {
#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return New<TPortoJobProxyEnvironment>(ConvertTo<TPortoJobEnvironmentConfigPtr>(config));
#endif

        case EJobEnvironmentType::Simple:
            return New<TSimpleJobProxyEnvironment>();

        default:
            THROW_ERROR_EXCEPTION("Unable to create resource controller for %Qlv environment",
                environmentConfig->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
