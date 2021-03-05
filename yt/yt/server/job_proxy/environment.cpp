#include "environment.h"

#include <yt/yt/server/lib/core_dump/public.h>

#include <yt/yt/server/lib/exec_agent/config.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <util/system/fs.h>

#ifdef _linux_
#include <yt/yt/server/lib/containers/porto_executor.h>
#include <yt/yt/server/lib/containers/instance.h>

#include <yt/yt/server/lib/misc/process.h>
#endif

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/convert.h>

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

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
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
{
public:
    TPortoUserJobEnvironment(
        TPortoJobEnvironmentConfigPtr config,
        const TString& slotAbsoluteName,
        IPortoExecutorPtr portoExecutor,
        IInstancePtr instance,
        bool usePortoMemoryTracking)
        : Config_(std::move(config))
        , SlotAbsoluteName_(slotAbsoluteName)
        , UsePortoMemoryTracking_(usePortoMemoryTracking)
        , PortoExecutor_(std::move(portoExecutor))
        , Instance_(std::move(instance))
        , ResourceTracker_(New<TPortoResourceTracker>(Instance_, ResourceUsageUpdatePeriod))
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
        return Config_->BlockIOWatchdogPeriod;
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

    virtual std::optional<TMemoryStatistics> GetMemoryStatistics() const
    {
        if (UsePortoMemoryTracking_) {
            return ResourceTracker_->GetMemoryStatistics();
        } else {
            return std::nullopt;
        }
    }

    virtual TProcessBasePtr CreateUserJobProcess(
        const TString& path,
        const TUserJobProcessOptions& options) override
    {
        TString slotGpuCorePipeFile;

        if (options.SlotCoreWatcherDirectory) {
            // NB: Core watcher expects core info file to be created before
            // core pipe file.
            auto slotCoreDirectory = *options.SlotCoreWatcherDirectory;
            auto coreDirectory = *options.CoreWatcherDirectory;
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
            Instance_->SetCoreDumpHandler(coreHandler);

            if (options.EnableCudaGpuCoreDump) {
                slotGpuCorePipeFile = NFS::CombinePaths(slotCoreDirectory, NCoreDump::CudaGpuCoreDumpPipeName);
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

        if (options.HostName) {
            const auto& hostName = *options.HostName;
            Instance_->SetHostName(hostName);
            if (!options.NetworkAddresses.empty()) {
                const auto& address = options.NetworkAddresses[0]->Address;
                Instance_->AddHostsRecord(hostName, address);
            }
        }

        //! There is no HBF in test environment, so setting IP addresses to
        //! user job will cause multiple problems during container startup.
        if (!options.NetworkAddresses.empty() && !Config_->TestNetwork) {
            std::vector<TIP6Address> addresses;
            addresses.reserve(options.NetworkAddresses.size());
            for (const auto& address : options.NetworkAddresses) {
                addresses.push_back(address->Address);
            }

            Instance_->SetIPAddresses(addresses);
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

        Process_ = New<TPortoProcess>(adjustedPath, Instance_, false);
        if (options.EnableCudaGpuCoreDump) {
            Envirnoment_.push_back("CUDA_ENABLE_COREDUMP_ON_EXCEPTION=1");
            Envirnoment_.push_back(Format("CUDA_COREDUMP_FILE=%v", slotGpuCorePipeFile));
        }
        for (const auto& networkAddress : options.NetworkAddresses) {
            Envirnoment_.push_back(Format("YT_IP_ADDRESS_%v=%v", to_upper(networkAddress->Name), networkAddress->Address));
        }

        return Process_;
    }

    virtual IInstancePtr GetUserJobInstance() const override
    {
        return Instance_;
    }

    const std::vector<TString>& GetEnvironmentVariables() const override
    {
        return Envirnoment_;
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    const TString SlotAbsoluteName_;
    const bool UsePortoMemoryTracking_;
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr Instance_;
    const TPortoResourceTrackerPtr ResourceTracker_;
    std::vector<TString> Envirnoment_;

    TProcessBasePtr Process_;
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
        std::vector<TString> gpuDevices)
        : Config_(std::move(config))
        , RootFS_(rootFS)
        , GpuDevices_(std::move(gpuDevices))
        , PortoExecutor_(CreatePortoExecutor(Config_->PortoExecutor, "environ"))
        , Self_(GetSelfPortoInstance(PortoExecutor_))
        , ResourceTracker_(New<TPortoResourceTracker>(Self_, ResourceUsageUpdatePeriod))
        , SlotAbsoluteName_(GetAbsoluteName(Self_))
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

    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(TGuid jobId) override
    {
        auto containerName = Config_->UseShortContainerNames
            ? Format("%v/uj", SlotAbsoluteName_)
            : Format("%v/uj_%v-%v", SlotAbsoluteName_, IntToString<16>(jobId.Parts32[3]), IntToString<16>(jobId.Parts32[2]));

        auto instance = CreatePortoInstance(containerName, PortoExecutor_);

        auto portoUser = *WaitFor(PortoExecutor_->GetContainerProperty(SlotAbsoluteName_, "user"))
            .ValueOrThrow();
        instance->SetUser(portoUser);

        if (RootFS_) {
            auto newPath = NFS::CombinePaths(RootFS_->RootPath, "slot");
            YT_LOG_INFO("Mount slot directory into container (Path: %v)", newPath);

            THashMap<TString, TString> properties;
            properties["backend"] = "rbind";
            properties["storage"] = NFs::CurrentWorkingDirectory();
            auto volumePath = WaitFor(PortoExecutor_->CreateVolume(newPath, properties))
                .ValueOrThrow();

            // TODO(gritukan): ytserver-exec can be resolved into something strange in tests,
            // so let's live with exec in layer for a while.
            if (!Config_->UseExecFromLayer) {
                RootFS_->Binds.emplace_back(TBind {
                    ResolveBinaryPath(ExecProgramName).ValueOrThrow(),
                    RootFSBinaryDirectory + ExecProgramName,
                    true});
            }

            instance->SetRoot(*RootFS_);
        }

        std::vector<TDevice> devices;
        for (const auto& descriptor : ListGpuDevices()) {
            const auto& deviceName = descriptor.DeviceName;
            if (std::find(GpuDevices_.begin(), GpuDevices_.end(), deviceName) == GpuDevices_.end()) {
                devices.emplace_back(TDevice{deviceName, false});
            }
        }

        // Restrict access to devices, that are not explicitly granted.
        instance->SetDevices(std::move(devices));

        return New<TPortoUserJobEnvironment>(
            Config_,
            SlotAbsoluteName_,
            PortoExecutor_,
            std::move(instance),
            UsePortoMemoryTracking_);
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    std::optional<TRootFS> RootFS_;
    const std::vector<TString> GpuDevices_;
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr Self_;
    const TPortoResourceTrackerPtr ResourceTracker_;
    const TString SlotAbsoluteName_;

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
    std::vector<TString> gpuDevices)
{

    auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(config);
    switch (environmentConfig->Type) {
#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return New<TPortoJobProxyEnvironment>(
                ConvertTo<TPortoJobEnvironmentConfigPtr>(config),
                rootFS,
                std::move(gpuDevices));
#endif

        case EJobEnvironmentType::Simple:
            if (rootFS) {
                THROW_ERROR_EXCEPTION("Simple job environment does not support custom root FS");
            }

            if (!gpuDevices.empty()) {
                YT_LOG_WARNING("Simple job environment does not support GPU device isolation (Devices: %v)", gpuDevices);
            }

            return nullptr;

        default:
            THROW_ERROR_EXCEPTION("Unable to create resource controller for %Qlv environment",
                environmentConfig->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
