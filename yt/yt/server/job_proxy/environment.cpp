#include "environment.h"
#include "job.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/gpu_helpers.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/ytlib/job_proxy/private.h>
#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/library/containers/public.h>

#include <util/system/fs.h>
#include <util/system/user.h>
#include <util/string/split.h>

#ifdef _linux_
#include <yt/yt/library/containers/cgroup.h>
#include <yt/yt/library/containers/cgroups_new.h>
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>
#endif

#include <yt/yt/library/containers/cri/config.h>
#include <yt/yt/library/containers/cri/image_cache.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/exit.h>

#include <sys/stat.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NContainers;
using namespace NExecNode;
using namespace NJobAgent;
using namespace NNet;
using namespace NTools;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "JobProxyEnvironment");

#ifdef _linux_
static constexpr auto ResourceUsageUpdatePeriod = TDuration::MilliSeconds(1000);
#endif

template <class T>
std::optional<T> ValueOrNullopt(const TErrorOr<T>& result) noexcept
{
    if (result.IsOK()) {
        return std::optional(result.Value());
    }

    // NB(arkady-e1ppa): Currently this method is only
    // used on fields obtained via GetFieldOrError
    // method from resource tracker, which print
    // field name in the error message.
    YT_LOG_WARNING(
        result,
        "Failed to extract value");

    return std::nullopt;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEnvironmentCpuStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("burst", statistics.BurstUsageTime)
            .OptionalItem("user", statistics.UserUsageTime)
            .OptionalItem("system", statistics.SystemUsageTime)
            .OptionalItem("wait", statistics.WaitTime)
            .OptionalItem("throttled", statistics.ThrottledTime)
            .OptionalItem("cfs_throttled", statistics.CfsThrottledTime)
            .OptionalItem("context_switches", statistics.ContextSwitchesDelta)
            .OptionalItem("peak_thread_count", statistics.PeakThreadCount)
        .EndMap();
}

TErrorOr<TJobEnvironmentCpuStatistics> ExtractJobEnvironmentCpuStatistics(const TCpuStatistics& statistics)
{
    try {
        return TJobEnvironmentCpuStatistics {
            .BurstUsageTime = ValueOrNullopt(statistics.BurstUsageTime),
            .UserUsageTime = ValueOrNullopt(statistics.UserUsageTime),
            .SystemUsageTime = ValueOrNullopt(statistics.SystemUsageTime),
            .WaitTime = ValueOrNullopt(statistics.WaitTime),
            .ThrottledTime = ValueOrNullopt(statistics.ThrottledTime),
            .CfsThrottledTime = ValueOrNullopt(statistics.CfsThrottledTime),
            .ContextSwitchesDelta = ValueOrNullopt(statistics.ContextSwitchesDelta),
            .PeakThreadCount = ValueOrNullopt(statistics.PeakThreadCount),
        };
    } catch (const std::exception& ex) {
        return TError("Extract job cpu statistics failed")
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEnvironmentMemoryStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    //FIXME(khlebnikov): Give "rss" proper name.
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("rss").Value(statistics.ResidentAnon)
            .Item("mapped_file").Value(statistics.MappedFile)
            .Item("major_page_faults").Value(statistics.MajorPageFaults)
        .EndMap();
}

TErrorOr<TJobEnvironmentMemoryStatistics> ExtractJobEnvironmentMemoryStatistics(const TMemoryStatistics& statistics)
{
    try {
        return TJobEnvironmentMemoryStatistics {
            .ResidentAnon = statistics.ResidentAnon.ValueOrThrow(),
            .MappedFile = statistics.MappedFile.ValueOrThrow(),
            .MajorPageFaults = statistics.MajorPageFaults.ValueOrThrow()
        };
    } catch (const std::exception& ex) {
        return TError("Extract job memory statistics failed")
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEnvironmentBlockIOStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("bytes_read", statistics.IOReadByte)
            .OptionalItem("bytes_written", statistics.IOWriteByte)
            .OptionalItem("io_read", statistics.IOReadOps)
            .OptionalItem("io_write", statistics.IOWriteOps)
            .OptionalItem("io_total", statistics.IOOps)
        .EndMap();
}

TErrorOr<TJobEnvironmentBlockIOStatistics> ExtractJobEnvironmentBlockIOStatistics(const TBlockIOStatistics& statistics) noexcept
{
    return TJobEnvironmentBlockIOStatistics {
        .IOReadByte = ValueOrNullopt(statistics.TotalIOStatistics.IOReadByte),
        .IOWriteByte = ValueOrNullopt(statistics.TotalIOStatistics.IOWriteByte),
        .IOReadOps = ValueOrNullopt(statistics.TotalIOStatistics.IOReadOps),
        .IOWriteOps = ValueOrNullopt(statistics.TotalIOStatistics.IOWriteOps),
        .IOOps = ValueOrNullopt(statistics.TotalIOStatistics.IOOps),
    };
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEnvironmentNetworkStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("tx_bytes", statistics.TxBytes)
            .OptionalItem("tx_packets", statistics.TxPackets)
            .OptionalItem("tx_drops", statistics.TxDrops)
            .OptionalItem("rx_bytes", statistics.RxBytes)
            .OptionalItem("rx_packets", statistics.RxPackets)
            .OptionalItem("rx_drops", statistics.RxDrops)
        .EndMap();
}

TErrorOr<TJobEnvironmentNetworkStatistics> ExtractJobEnvironmentNetworkStatistics(const TNetworkStatistics& statistics)
{
    return TJobEnvironmentNetworkStatistics {
        .TxBytes = ValueOrNullopt(statistics.TxBytes),
        .TxPackets = ValueOrNullopt(statistics.TxPackets),
        .TxDrops = ValueOrNullopt(statistics.TxDrops),
        .RxBytes = ValueOrNullopt(statistics.RxBytes),
        .RxPackets = ValueOrNullopt(statistics.RxPackets),
        .RxDrops = ValueOrNullopt(statistics.RxDrops),
    };
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoUserJobEnvironment
    : public IUserJobEnvironment
{
public:
    TPortoUserJobEnvironment(
        TJobId jobId,
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
            auto slotGpuCorePipeFile = NFS::CombinePaths(*Options_.SlotCoreWatcherDirectory, CudaGpuCoreDumpPipeName);
            Environment_.push_back("CUDA_ENABLE_COREDUMP_ON_EXCEPTION=1");
            Environment_.push_back(Format("CUDA_COREDUMP_FILE=%v", slotGpuCorePipeFile));
        }
        for (const auto& networkAddress : Options_.NetworkAddresses) {
            Environment_.push_back(Format("YT_IP_ADDRESS_%v=%v", to_upper(networkAddress->Name), networkAddress->Address));
        }
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        if (auto resourceTracker = ResourceTracker_.Acquire()) {
            return ExtractJobEnvironmentCpuStatistics(resourceTracker->GetCpuStatistics());
        } else {
            return {};
        }
    }

    TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetMemoryStatistics() const override
    {
        auto resourceTracker = ResourceTracker_.Acquire();
        if (Options_.EnablePortoMemoryTracking && resourceTracker) {
            return ExtractJobEnvironmentMemoryStatistics(resourceTracker->GetMemoryStatistics());
        } else {
            return {};
        }
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        if (auto resourceTracker = ResourceTracker_.Acquire()) {
            return ExtractJobEnvironmentBlockIOStatistics(resourceTracker->GetBlockIOStatistics());
        } else {
            return {};
        }
    }

    TErrorOr<std::optional<TJobEnvironmentNetworkStatistics>> GetNetworkStatistics() const override
    {
        auto resourceTracker = ResourceTracker_.Acquire();
        if (!Options_.NetworkAddresses.empty() && resourceTracker) {
            return ExtractJobEnvironmentNetworkStatistics(resourceTracker->GetNetworkStatistics());
        } else {
            return {};
        }
    }

    std::optional<TDuration> GetBlockIOWatchdogPeriod() const override
    {
        return Config_->BlockIOWatchdogPeriod;
    }

    void CleanProcesses() override
    {
        try {
            if (auto instance = GetUserJobInstance()) {
                instance->Stop();
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to stop user container");
        }
    }

    void SetIOThrottle(i64 operations) override
    {
        if (auto instance = GetUserJobInstance()) {
            instance->SetIOThrottle(operations);
        }
    }

    TString HandleVolumeCreationError(TErrorOr<TString> volumeCreationResult)
    {
        if (!volumeCreationResult.IsOK()) {
            THROW_ERROR_EXCEPTION(
                NJobProxy::EErrorCode::UserJobPortoApiError,
                "Creation of user job volume failed")
                << std::move(volumeCreationResult);
        } else {
            return volumeCreationResult.Value();
        }
    }

    TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory,
        std::optional<int> userId) override
    {
        auto jobIdAsGuid = JobId_.Underlying();
        auto containerName = Config_->UseShortContainerNames
            ? Format("%v/uj", SlotContainerName_)
            : Format("%v/uj-%x-%x", SlotContainerName_, jobIdAsGuid.Parts32[3], jobIdAsGuid.Parts32[2]);

        auto launcher = CreatePortoInstanceLauncher(containerName, PortoExecutor_);

        auto portoUser = *WaitFor(PortoExecutor_->GetContainerProperty(SlotContainerName_, "user"))
            .ValueOrThrow();
        launcher->SetUser(portoUser);

        if (Options_.RootFS) {
            auto rootFS = *Options_.RootFS;
            auto newPath = NFS::CombinePaths(rootFS.RootPath, "slot");

            if (Options_.EnableRootVolumeDiskQuota) {
                YT_LOG_INFO("Prepare rootFS for binds (Path: %v)", newPath);

                PrepareRootFS(
                    rootFS.RootPath,
                    userId);
            } else {
                YT_LOG_INFO("Mount slot directory into container (Path: %v)", newPath);

                THashMap<TString, TString> properties;
                properties["backend"] = "rbind";
                properties["storage"] = NFs::CurrentWorkingDirectory();
                auto volumeCreationResult = WaitFor(PortoExecutor_->CreateVolume(
                    newPath,
                    properties));
                auto volumePath = HandleVolumeCreationError(volumeCreationResult);
            }

            launcher->SetRoot(rootFS);
        }

        std::vector<TDevice> devices;
        for (const auto& descriptor : ListGpuDevices()) {
            int deviceIndex = descriptor.DeviceIndex;
            if (std::find(Options_.GpuIndexes.begin(), Options_.GpuIndexes.end(), deviceIndex) == Options_.GpuIndexes.end()) {
                devices.push_back(TDevice{
                    .DeviceName = GetGpuDeviceName(deviceIndex),
                    .Access = "-",
                });
            }
        }

        if (NFS::Exists("/dev/kvm")) {
            devices.push_back(TDevice{
                .DeviceName = "/dev/kvm",
                .Access = "rw",
            });
        }

        if (NFS::Exists("/dev/net/tun")) {
            devices.push_back(TDevice{
                .DeviceName = "/dev/net/tun",
                .Access = "rw",
            });
        }

        if (Options_.EnableFuse && NFS::Exists("/dev/fuse")) {
            launcher->SetEnableFuse(true);
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
                auto slotGpuCorePipeFile = NFS::CombinePaths(slotCoreDirectory, CudaGpuCoreDumpPipeName);
                auto gpuCorePipeFile = NFS::CombinePaths(coreDirectory, CudaGpuCoreDumpPipeName);
                YT_LOG_DEBUG("Creating pipe for GPU core dumps (SlotGpuCorePipeFile: %v, GpuCorePipeFile: %v)",
                    slotGpuCorePipeFile,
                    gpuCorePipeFile);
                if (mkfifo(gpuCorePipeFile.c_str(), 0666) == -1) {
                    THROW_ERROR_EXCEPTION("Failed to create CUDA GPU core dump pipe")
                        << TErrorAttribute("path", gpuCorePipeFile)
                        << TError::FromSystem();
                }
            }
        } else {
            launcher->SetCoreDumpHandler(std::nullopt);
        }

        if (Options_.HostName) {
            launcher->SetHostName(*Options_.HostName);
        }

        //! There is no HBF in test environment, so setting IP addresses to
        //! user job will cause multiple problems during container startup.
        if (Options_.DisableNetwork) {
            launcher->DisableNetwork();
        } else if (!Options_.NetworkAddresses.empty()) {
            std::vector<TIP6Address> addresses;
            addresses.reserve(Options_.NetworkAddresses.size());
            for (const auto& address : Options_.NetworkAddresses) {
                addresses.push_back(address->Address);
            }

            launcher->SetIPAddresses(addresses, Options_.EnableNat64);
        }

        launcher->SetNetworkInterface(TString(JobNetworkInterface));

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

    IInstancePtr GetUserJobInstance() const override
    {
        return Instance_.Acquire();
    }

    const std::vector<TString>& GetEnvironmentVariables() const override
    {
        return Environment_;
    }

    std::vector<pid_t> GetJobPids() const override
    {
        if (auto instance = GetUserJobInstance()) {
            return instance->GetPids();
        } else {
            return {};
        }
    }

    std::optional<pid_t> GetJobRootPid() const override
    {
        if (auto instance = GetUserJobInstance()) {
            return instance->GetPid();
        } else {
            return std::nullopt;
        }
    }

    bool IsPidNamespaceIsolationEnabled() const override
    {
        return true;
    }

    i64 GetMajorPageFaultCount() const override
    {
        return GetUserJobInstance()->GetMajorPageFaultCount();
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        return std::nullopt;
    }

    bool HasRootFS() const override
    {
        return Options_.RootFS.has_value();
    }

private:
    const TJobId JobId_;
    const TPortoJobEnvironmentConfigPtr Config_;
    const TUserJobEnvironmentOptions Options_;
    const TString SlotContainerName_;
    const IPortoExecutorPtr PortoExecutor_;

    std::vector<TString> Environment_;

    TAtomicIntrusivePtr<IInstance> Instance_;
    TAtomicIntrusivePtr<TPortoResourceTracker> ResourceTracker_;

    void PrepareRootFS(
        const TString& rootPath,
        std::optional<int> userId)
    {
        int nodeUid = getuid();

        auto rootConfig = New<TRootDirectoryConfig>();
        rootConfig->SlotPath = rootPath;
        rootConfig->UserId = nodeUid;
        rootConfig->Permissions = 0777;

        // TODO(artemagafonov): Decide which directories need to be created here.
        // NB: Paths are relative and ordered in the creation sequence. Must create directory before its subdirectory.
        static const std::vector<TString> directoryPaths{
            "slot",
            "slot/sandbox",
            "slot/tmp",
            "tmp",
            "var",
            "var/tmp",
        };

        for (const auto& directoryPath : directoryPaths) {
            auto directory = New<TDirectoryConfig>();

            directory->Path = NFS::CombinePaths(
                rootPath,
                directoryPath);
            directory->UserId = userId;
            directory->Permissions = 0777;
            directory->RemoveIfExists = false;

            rootConfig->Directories.push_back(std::move(directory));
        }

        auto directoryBuilderConfig = New<TDirectoryBuilderConfig>();
        directoryBuilderConfig->NodeUid = nodeUid;
        directoryBuilderConfig->NeedRoot = true;
        directoryBuilderConfig->RootDirectoryConfigs.push_back(std::move(rootConfig));

        RunTool<TRootDirectoryBuilderTool>(directoryBuilderConfig);
    }
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
        PortoExecutor_->SubscribeFailed(BIND_NO_PROPAGATE(&TPortoJobProxyEnvironment::OnFatalError, MakeWeak(this)));
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        return ExtractJobEnvironmentCpuStatistics(ResourceTracker_->GetCpuStatistics());
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        return ExtractJobEnvironmentBlockIOStatistics(ResourceTracker_->GetBlockIOStatistics());
    }

    std::optional<TJobEnvironmentMemoryStatistics> GetJobMemoryStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<TJobEnvironmentBlockIOStatistics> GetJobBlockIOStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<TJobEnvironmentCpuStatistics> GetJobCpuStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        return std::nullopt;
    }

    void SetCpuGuarantee(double value) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotContainerName_, "cpu_guarantee", ToString(value) + "c"))
            .ThrowOnError();
    }

    void SetCpuLimit(double value) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotContainerName_, "cpu_limit", ToString(value) + "c"))
            .ThrowOnError();
    }

    void SetCpuPolicy(const TString& policy) override
    {
        WaitFor(PortoExecutor_->SetContainerProperty(SlotContainerName_, "cpu_policy", policy))
            .ThrowOnError();
    }

    bool UseExecFromLayer() const override
    {
        return Config_->UseExecFromLayer;
    }

    IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TJobId jobId,
        const TUserJobEnvironmentOptions& options) override
    {
        return New<TPortoUserJobEnvironment>(
            jobId,
            Config_,
            options,
            SlotContainerName_,
            PortoExecutor_);
    }

    void StartSidecars(IJobHostPtr /*host*/,
        const NControllerAgent::NProto::TJobSpecExt& /*jobSpecExt*/,
        std::function<void(TError)> /*failJobCallback*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Porto job proxy environment");
    }

    void StartSidecar(const std::string& /*name*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Porto job proxy environment");
    }

    void SidecarFinished(const std::string& /*sidecarName*/, const TErrorOr<void> &/*value*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Porto job proxy environment");
    }

    void KillSidecars() override
    {
        YT_LOG_INFO("Sidecars are not supported in Porto job proxy environment");
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
        AbortProcessSilently(EJobProxyExitCode::PortoManagementFailed);
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
    std::optional<TDuration> GetBlockIOWatchdogPeriod() const override
    {
        // No IO watchdog for simple job environment.
        return std::nullopt;
    }

    TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetMemoryStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentNetworkStatistics>> GetNetworkStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        return {};
    }

    void CleanProcesses() override
    {
        if (auto process = Process_.Acquire()) {
            auto pids = GetJobPids();
            process->Kill(9);
            for (auto pid : pids) {
                if (pid != process->GetProcessId()) {
                    if (TryKillProcessByPid(pid, 9)) {
                        YT_LOG_DEBUG("Child job process killed (Pid: %v)", pid);
                    } else {
                        YT_LOG_DEBUG("Failed to kill child job process (Pid: %v)", pid);
                    }
                }
            }
        }
    }

    void SetIOThrottle(i64 /*operations*/) override
    {
        // Noop.
    }

    TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory,
        std::optional<int> /*userId*/) override
    {
        auto process = New<TSimpleProcess>(path, false);
        process->AddArguments(arguments);
        process->SetWorkingDirectory(workingDirectory);
        Process_.Store(process);
        return process->Spawn();
    }

    NContainers::IInstancePtr GetUserJobInstance() const override
    {
        return nullptr;
    }

    std::optional<pid_t> GetJobRootPid() const override
    {
        if (auto process = Process_.Acquire()) {
            return process->GetProcessId();
        } else {
            return std::nullopt;
        }
    }

    std::vector<pid_t> GetJobPids() const override
    {
        if (auto process = Process_.Acquire()) {
            auto pid = process->GetProcessId();
            return GetPidsUnderParent(pid);
        }

        return {};
    }

    bool IsPidNamespaceIsolationEnabled() const override
    {
        return false;
    }

    //! Returns the list of environment-specific environment variables in key=value format.
    const std::vector<TString>& GetEnvironmentVariables() const override
    {
        static std::vector<TString> emptyEnvironment;
        return emptyEnvironment;
    }

    i64 GetMajorPageFaultCount() const override
    {
        return 0;
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        return std::nullopt;
    }

    bool HasRootFS() const override
    {
        return false;
    }

private:
    TAtomicIntrusivePtr<TProcessBase> Process_;
};

DECLARE_REFCOUNTED_CLASS(TSimpleUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TSimpleUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    void SetCpuGuarantee(double /*value*/) override
    {
        YT_LOG_WARNING("CPU guarantees are not supported in simple job environment");
    }

    void SetCpuLimit(double /*value*/) override
    {
        YT_LOG_WARNING("CPU limits are not supported in simple job environment");
    }

    void SetCpuPolicy(const TString& /*value*/) override
    {
        YT_LOG_WARNING("CPU policy is not supported in simple job environment");
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        return {};
    }

    std::optional<TJobEnvironmentMemoryStatistics> GetJobMemoryStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<TJobEnvironmentBlockIOStatistics> GetJobBlockIOStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<TJobEnvironmentCpuStatistics> GetJobCpuStatistics() const noexcept override
    {
        return std::nullopt;
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        return std::nullopt;
    }

    bool UseExecFromLayer() const override
    {
        return false;
    }

    IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TJobId /*jobId*/,
        const TUserJobEnvironmentOptions& options) override
    {
        if (options.RootFS) {
            THROW_ERROR_EXCEPTION("Root FS isolation is not supported in simple job environment");
        }

        if (!options.GpuIndexes.empty()) {
            // This could only happen in tests, e.g. TestSchedulerGpu.
            YT_LOG_WARNING("GPU devices are not supported in simple job environment");
        }

        if (options.EnablePortoMemoryTracking) {
            THROW_ERROR_EXCEPTION("Porto memory tracking is not supported in simple job environment");
        }

        return New<TSimpleUserJobEnvironment>();
    }

    void StartSidecars(
        IJobHostPtr /*host*/,
        const NControllerAgent::NProto::TJobSpecExt& /*jobSpecExt*/,
        std::function<void(TError)> /*failJobCallback*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Simple job proxy environment");
    }

    void StartSidecar(const std::string& /*name*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Simple job proxy environment");
    }

    void SidecarFinished(const std::string& /*sidecarName*/, const TErrorOr<void> &/*value*/) override
    {
        YT_LOG_INFO("Sidecars are not supported in Simple job proxy environment");
    }

    void KillSidecars() override
    {
        YT_LOG_INFO("Sidecars are not supported in Simple job proxy environment");
    }
};

DECLARE_REFCOUNTED_CLASS(TSimpleJobProxyEnvironment)
DEFINE_REFCOUNTED_TYPE(TSimpleJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TTestingUserJobEnvironment
    : public TSimpleUserJobEnvironment
{
public:
    explicit TTestingUserJobEnvironment(
        TTestingJobEnvironmentConfigPtr config)
        : Config_(std::move(config))
    { }

    i64 GetMajorPageFaultCount() const override
    {
        if (Config_->TestingJobEnvironmentScenario == ETestingJobEnvironmentScenario::IncreasingMajorPageFaultCount) {
            MajorPageFaultCount_ += 1000;
        }

        return MajorPageFaultCount_;
    }

private:
    const TTestingJobEnvironmentConfigPtr Config_;

    mutable i64 MajorPageFaultCount_ = 0;
};

DECLARE_REFCOUNTED_CLASS(TTestingUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TTestingUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

class TTestingJobProxyEnvironment
    : public TSimpleJobProxyEnvironment
{
public:
    explicit TTestingJobProxyEnvironment(
        TTestingJobEnvironmentConfigPtr config)
        : Config_(std::move(config))
    { }

    IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TJobId /*jobId*/,
        const TUserJobEnvironmentOptions& options) override
    {
        if (options.RootFS) {
            THROW_ERROR_EXCEPTION("Root FS isolation is not supported in testing job environment");
        }

        if (!options.GpuIndexes.empty()) {
            // This could only happen in tests, e.g. TestSchedulerGpu.
            YT_LOG_WARNING("GPU devices are not supported in testing job environment");
        }

        if (options.EnablePortoMemoryTracking) {
            THROW_ERROR_EXCEPTION("Porto memory tracking is not supported in testing job environment");
        }

        return New<TTestingUserJobEnvironment>(Config_);
    }

private:
    const TTestingJobEnvironmentConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

class TCriUserJobEnvironment
    : public IUserJobEnvironment
{
public:
    TCriUserJobEnvironment(IJobProxyEnvironmentPtr jobProxyEnvironment)
        : JobProxyEnvironment_(std::move(jobProxyEnvironment))
    {
        auto username = ::GetUsername();
        Environment_.push_back("USER=" + username);
        Environment_.push_back("LOGNAME=" + username);
    }

    std::optional<TDuration> GetBlockIOWatchdogPeriod() const override
    {
        // No IO watchdog for simple job environment.
        return std::nullopt;
    }

    TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetMemoryStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentNetworkStatistics>> GetNetworkStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        return {};
    }

    void CleanProcesses() override
    {
        if (auto process = Process_.Acquire()) {
            auto pids = GetJobPids();
            if (process->IsStarted()) {
                process->Kill(SIGKILL);
            }
            for (auto pid : pids) {
                if (pid != process->GetProcessId()) {
                    if (TryKillProcessByPid(pid, SIGKILL)) {
                        YT_LOG_DEBUG("Child job process killed (Pid: %v)", pid);
                    } else {
                        YT_LOG_DEBUG("Failed to kill child job process (Pid: %v)", pid);
                    }
                }
            }
        }
    }

    void SetIOThrottle(i64 /*operations*/) override
    {
        // Noop.
    }

    TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory,
        std::optional<int> /*userId*/) override
    {
        auto process = New<TSimpleProcess>(path, /*copyEnv*/ false);
        process->AddArguments(arguments);
        process->SetWorkingDirectory(workingDirectory);
        Process_.Store(process);
        return process->Spawn();
    }

    NContainers::IInstancePtr GetUserJobInstance() const override
    {
        return nullptr;
    }

    std::optional<pid_t> GetJobRootPid() const override
    {
        if (auto process = Process_.Acquire()) {
            return process->GetProcessId();
        } else {
            return std::nullopt;
        }
    }

    std::vector<pid_t> GetJobPids() const override
    {
        if (auto process = Process_.Acquire()) {
            auto pid = process->GetProcessId();
            return GetPidsUnderParent(pid);
        }

        return {};
    }

    bool IsPidNamespaceIsolationEnabled() const override
    {
        return false;
    }

    //! Returns the list of environment-specific environment variables in key=value format.
    const std::vector<TString>& GetEnvironmentVariables() const override
    {
        return Environment_;
    }

    i64 GetMajorPageFaultCount() const override
    {
        return 0;
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        return JobProxyEnvironment_->GetJobOomKillCount();
    }

    bool HasRootFS() const override
    {
        return false;
    }

private:
    const IJobProxyEnvironmentPtr JobProxyEnvironment_;
    TAtomicIntrusivePtr<TProcessBase> Process_;
    std::vector<TString> Environment_;
};

DECLARE_REFCOUNTED_CLASS(TCriUserJobEnvironment)
DEFINE_REFCOUNTED_TYPE(TCriUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct TSidecarCriConfig {
    NScheduler::TSidecarJobSpecPtr JobSpec;
    NCri::TCriContainerSpecPtr ContainerSpec;
    TString Command;
    std::vector<TString> Arguments;
};

class TCriJobProxyEnvironment
    : public IJobProxyEnvironment
{
public:
    explicit TCriJobProxyEnvironment(
        TJobProxyInternalConfigPtr jobProxyConfig,
        TCriJobEnvironmentConfigPtr config)
        : JobProxyConfig_(std::move(jobProxyConfig))
        , Config_(std::move(config))
        , Executor_(CreateCriExecutor(Config_->CriExecutor))
        , ImageCache_(NContainers::NCri::CreateCriImageCache(Config_->CriImageCache, Executor_))
    { }

    void SetCpuGuarantee(double /*value*/) override
    {
        YT_LOG_WARNING("CPU guarantees are not supported in CRI job environment");
    }

    void SetCpuLimit(double /*value*/) override
    {
        YT_LOG_WARNING("CPU limits are not supported in CRI job environment");
    }

    void SetCpuPolicy(const TString& /*value*/) override
    {
        YT_LOG_WARNING("CPU policy is not supported in CRI job environment");
    }

    TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const override
    {
        return {};
    }

    TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const override
    {
        return {};
    }

    std::optional<TJobEnvironmentMemoryStatistics> GetJobMemoryStatistics() const noexcept override
    {
        auto statistics = StatisticsFetcher_.GetMemoryStatistics();
        return TJobEnvironmentMemoryStatistics{
            .ResidentAnon = statistics.ResidentAnon,
            .TmpfsUsage = statistics.TmpfsUsage,
            .MappedFile = statistics.MappedFile,
            .MajorPageFaults = statistics.MajorPageFaults,
        };
    }

    std::optional<TJobEnvironmentBlockIOStatistics> GetJobBlockIOStatistics() const noexcept override
    {
        auto statistics = StatisticsFetcher_.GetBlockIOStatistics();
        return TJobEnvironmentBlockIOStatistics{
            .IOReadByte = statistics.IOReadByte,
            .IOWriteByte = statistics.IOWriteByte,
            .IOReadOps = statistics.IOReadOps,
            .IOWriteOps = statistics.IOWriteOps,
        };
    }

    std::optional<TJobEnvironmentCpuStatistics> GetJobCpuStatistics() const noexcept override
    {
        auto statistics = StatisticsFetcher_.GetCpuStatistics();
        return TJobEnvironmentCpuStatistics{
            .UserUsageTime = statistics.UserTime,
            .SystemUsageTime = statistics.SystemTime,
        };
    }

    std::optional<i64> GetJobOomKillCount() const noexcept override
    {
        try {
            return StatisticsFetcher_.GetOomKillCount();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get OOM kill count");
            return std::nullopt;
        }
    }

    bool UseExecFromLayer() const override
    {
        return false;
    }

    IUserJobEnvironmentPtr CreateUserJobEnvironment(
        TJobId /*jobId*/,
        const TUserJobEnvironmentOptions& options) override
    {
        if (options.RootFS) {
            THROW_ERROR_EXCEPTION("Root FS isolation is not supported in CRI job environment");
        }

        if (!options.GpuIndexes.empty()) {
            // This could only happen in tests, e.g. TestSchedulerGpu.
            YT_LOG_WARNING("GPU devices are not supported in CRI job environment");
        }

        if (options.EnablePortoMemoryTracking) {
            THROW_ERROR_EXCEPTION("Porto memory tracking is not supported in CRI job environment");
        }

        return New<TCriUserJobEnvironment>(this);
    }

    void StartSidecars(
        IJobHostPtr host,
        const NControllerAgent::NProto::TJobSpecExt& jobSpecExt,
        std::function<void(TError)> failJobCallback) override
    {
        if (!jobSpecExt.has_user_job_spec()) {
            return;
        }
        Host_ = std::move(host);
        FailJobCallback_ = std::move(failJobCallback);

        const auto& podDescriptor = Config_->PodDescriptor;
        const auto& podSpec = Config_->PodSpec;
        for (const auto& [name, sidecar]: jobSpecExt.user_job_spec().sidecars()) {
            // Prepare the sidecar job spec.
            auto sidecarSpec = New<NScheduler::TSidecarJobSpec>();
            sidecarSpec->Command = sidecar.command();
            if (sidecar.has_cpu_limit()) {
                sidecarSpec->CpuLimit = sidecar.cpu_limit();
            }
            if (sidecar.has_memory_limit()) {
                sidecarSpec->MemoryLimit = sidecar.memory_limit();
            }
            if (sidecar.has_docker_image()) {
                sidecarSpec->DockerImage = sidecar.docker_image();
            }
            sidecarSpec->RestartPolicy = ConvertTo<NScheduler::ESidecarRestartPolicy>(sidecar.restart_policy());

            // Prepare the sidecar container spec.
            auto containerSpec = New<NCri::TCriContainerSpec>();
            containerSpec->Resources = New<NCri::TCriContainerResources>();

            containerSpec->Name = Format("sidecar-%v-%v-%v", podDescriptor->Name, podSpec->Name, name);

            // If no Docker image is provided, use the one from the main job.
            containerSpec->Image.Image = sidecarSpec->DockerImage.value_or(Config_->JobProxyImage);

            containerSpec->Resources->CpuLimit = sidecarSpec->CpuLimit;
            containerSpec->Resources->MemoryLimit = sidecarSpec->MemoryLimit;

            const auto& cpusetCpu = podSpec->Resources->CpusetCpus;
            if (cpusetCpu != EmptyCpuSet) {
                containerSpec->Resources->CpusetCpus = cpusetCpu;
            }

            containerSpec->CapabilitiesToAdd.push_back("SYS_PTRACE");

            containerSpec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = JobProxyConfig_->SlotPath,
                .HostPath = JobProxyConfig_->SlotPath,
                .ReadOnly = false,
            });

            containerSpec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = "/slot",
                .HostPath = JobProxyConfig_->SlotPath,
                .ReadOnly = false,
            });

            for (const auto& bindMount: Config_->JobProxyBindMounts) {
                containerSpec->BindMounts.push_back(NCri::TCriBindMount{
                    .ContainerPath = bindMount->InternalPath,
                    .HostPath = bindMount->ExternalPath,
                    .ReadOnly = bindMount->ReadOnly,
                });
            }

            for (const auto& bind : JobProxyConfig_->Binds) {
                containerSpec->BindMounts.push_back(NCri::TCriBindMount{
                    .ContainerPath = bind->InternalPath,
                    .HostPath = bind->ExternalPath,
                    .ReadOnly = bind->ReadOnly,
                });
            }

            containerSpec->Credentials.Uid = ::getuid();
            containerSpec->Credentials.Gid = ::getgid();

            std::vector<TString> commandSplit;
            StringSplitter(sidecarSpec->Command).Split(' ').Collect(&commandSplit);
            auto command = commandSplit[0];
            commandSplit.erase(commandSplit.begin());

            // Save them for later use.
            SidecarsConfigs_[name] = TSidecarCriConfig{
                std::move(sidecarSpec),
                std::move(containerSpec),
                std::move(command),
                std::move(commandSplit)
            };
            StartSidecar(name);
        }
    }

    void StartSidecar(const std::string& name) override
    {
        auto sidecarIt = SidecarsConfigs_.find(name);
        if (sidecarIt == SidecarsConfigs_.end()) {
            YT_LOG_ERROR("Cannot start the sidecar: no such sidecar found (Name: %v)", name);
            return;
        }
        const auto& sidecarConfig = sidecarIt->second;

        auto process = Executor_->CreateProcess(
            sidecarConfig.Command,
            sidecarConfig.ContainerSpec,
            Config_->PodDescriptor,
            Config_->PodSpec
        );
        process->AddArguments(sidecarConfig.Arguments);
        process->SetWorkingDirectory(NFS::CombinePaths(Host_->GetSlotPath(), GetSandboxRelPath(ESandboxKind::User)));

        auto sidecarFuture = BIND([=] {
                return process->Spawn();
            })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
        sidecarFuture.Subscribe(BIND(&TCriJobProxyEnvironment::SidecarFinished, MakeStrong(this), name));
        SidecarsRunning_[name] = {process, std::move(sidecarFuture)};
    }

    void SidecarFinished(const std::string& sidecarName, const TErrorOr<void> &exitValue) override
    {
        using namespace NScheduler;

        YT_LOG_DEBUG("Sidecar has finished the execution (SidecarName: %v, ExitValue: %v)", sidecarName, exitValue);

        // When a sidecar exits, we need to take an appropriate action depending on the RestartPolicy.
        auto sidecarIt = SidecarsConfigs_.find(sidecarName);
        if (sidecarIt == SidecarsConfigs_.end()) {
            YT_LOG_ERROR("Cannot process sidecar's exit event: no such sidecar found (SidecarName: %v)", sidecarName);
            return;
        }
        const auto restartPolicy = sidecarIt->second.JobSpec->RestartPolicy;

        switch (restartPolicy) {
            case ESidecarRestartPolicy::Always:
                // Restart in any case.
                break;
            case ESidecarRestartPolicy::OnFailure:
                // Restart only if sidecar failed.
                if (exitValue.IsOK()) {
                    YT_LOG_DEBUG("Not restarting a sidecar (SidecarName: %v)", sidecarName);
                    return;
                }
                break;
            case ESidecarRestartPolicy::FailOnError:
                // Do not restart in case of success, fail the whole job otherwise.
                if (exitValue.IsOK()) {
                    YT_LOG_DEBUG("Not restarting a sidecar (SidecarName: %v)", sidecarName);
                    return;
                }

                YT_LOG_DEBUG("Sidecar has failed, exiting the main job (SidecarName: %v, Policy: %v, ExitValue: %v)",
                    sidecarName, restartPolicy, exitValue);
                KillSidecars();
                FailJobCallback_(TError("Failing the job because sidecar with FailOnError policy has failed")
                    << TErrorAttribute("SidecarName", sidecarName)
                    << TErrorAttribute("SidecarExitValue", exitValue));
                return;
        }

        YT_LOG_DEBUG("Restarting a sidecar as part of the exit event processing (SidecarName: %v, Policy: %v, ExitValue: %v)",
            sidecarName, restartPolicy, exitValue);
        StartSidecar(sidecarName);
    }

    void KillSidecars() override
    {
        for (auto& [name, sidecar]: SidecarsRunning_) {
            if (!sidecar.first->IsFinished()) {
                YT_LOG_DEBUG("Killing a CRI sidecar (name: %v)", name);
                sidecar.first->Kill(SIGKILL);
            }
        }
    }

private:
    const TJobProxyInternalConfigPtr JobProxyConfig_;
    const TCriJobEnvironmentConfigPtr Config_;
    const NContainers::NCri::ICriExecutorPtr Executor_;
    const NContainers::NCri::ICriImageCachePtr ImageCache_;

    IJobHostPtr Host_;
    std::function<void(TError)> FailJobCallback_;
    THashMap<TString, TSidecarCriConfig> SidecarsConfigs_;
    THashMap<TString, std::pair<TProcessBasePtr, TFuture<void>>> SidecarsRunning_;

    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("JobProxyEnvironment");

    NCGroups::TSelfCGroupsStatisticsFetcher StatisticsFetcher_;
};

DECLARE_REFCOUNTED_CLASS(TCriJobProxyEnvironment)
DEFINE_REFCOUNTED_TYPE(TCriJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(
    TJobProxyInternalConfigPtr config)
{
    switch (config->JobEnvironment.GetCurrentType()) {
#ifdef _linux_
        case EJobEnvironmentType::Porto:
            return New<TPortoJobProxyEnvironment>(config->JobEnvironment.TryGetConcrete<TPortoJobEnvironmentConfig>());
#endif

        case EJobEnvironmentType::Simple:
            return New<TSimpleJobProxyEnvironment>();

        case EJobEnvironmentType::Testing:
            return New<TTestingJobProxyEnvironment>(config->JobEnvironment.TryGetConcrete<TTestingJobEnvironmentConfig>());

        case EJobEnvironmentType::Cri:
            return New<TCriJobProxyEnvironment>(
                config,
                config->JobEnvironment.TryGetConcrete<TCriJobEnvironmentConfig>());

        default:
            THROW_ERROR_EXCEPTION("Unable to create resource controller for %Qlv environment",
                config->JobEnvironment.GetCurrentType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
