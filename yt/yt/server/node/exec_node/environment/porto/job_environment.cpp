#include "job_environment.h"

#include "slot.h"

#include "job_directory_manager.h"
#include "job_fs_secretary.h"
#include "job_workspace_builder.h"
#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/environment/common/job_environment_base.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/gpu_helpers.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/library/containers/helpers.h>
#include <yt/yt/library/containers/process.h>

#ifdef _linux_
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>

#endif

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/string/string.h>

#include <util/generic/guid.h>

#include <util/system/execpath.h>
#include <util/system/user.h>

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NContainers;
using namespace NDataNode;
using namespace NYTree;
using namespace NTools;
using namespace NServer;

using NNet::TIP6Address;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxCommandOutputSizeInError = 500;

constexpr double CpuUpdatePrecision = 0.01;

class TPortoJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TPortoJobEnvironment(
        TJobEnvironmentConfig config,
        TNonNullPtr<IBootstrap> bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , ConcreteConfig_(config.GetConcrete<TPortoJobEnvironmentConfig>())
        , PortoExecutor_(CreatePortoExecutor(
            ConcreteConfig_->PortoExecutor,
            "env_spawn",
            JobEnvironmentProfiler().WithPrefix("/porto")))
        , PortoCreateExecutor_(CreatePortoExecutor(
            ConcreteConfig_->PortoExecutor,
            "env_create",
            JobEnvironmentProfiler().WithPrefix("/porto_create")))
        , PortoDestroyExecutor_(CreatePortoExecutor(
            ConcreteConfig_->PortoExecutor,
            "env_destroy",
            JobEnvironmentProfiler().WithPrefix("/porto_destroy")))
        , ContainerDestroyFailureCounter_(JobEnvironmentProfiler().Counter("/container_destroy_failures"))
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto slotManagerConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;
        ShouldCloseDescriptors_.store(slotManagerConfig->ShouldCloseDescriptors);
    }

    TJobEnvironmentCapabilities GetCapabilities() const override
    {
        return {
            .SupportsVolumeReuse = true,
            .SupportsGpuLayers = true,
            .SupportsSidecarDockerImages = false,
        };
    }

    void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig) override
    {
        auto environmentConfig = newConfig->JobEnvironment;

        if (auto portoConfig = environmentConfig.TryGetConcrete<EJobEnvironmentType::Porto>()) {
            if (auto executor = PortoExecutor_) {
                executor->OnDynamicConfigChanged(portoConfig->PortoExecutor);
            }

            if (auto executor = PortoDestroyExecutor_) {
                executor->OnDynamicConfigChanged(portoConfig->PortoExecutor);
            }

            if (auto executor = PortoCreateExecutor_) {
                executor->OnDynamicConfigChanged(portoConfig->PortoExecutor);
            }

            ConcreteConfig_ = std::move(portoConfig);
        }

        ShouldCloseDescriptors_.store(newConfig->ShouldCloseDescriptors);

        UpdateIdleCpuFraction(newConfig->IdleCpuFraction);

        if (oldConfig->EnableNumaNodeScheduling && !newConfig->EnableNumaNodeScheduling) {
            ClearSlotCpuSets();
        }
    }

    void AddArguments(TProcessBasePtr process, int /*slotIndex*/) override
    {
        if (!ShouldCloseDescriptors_.load()) {
            process->AddArguments({"--do-not-close-descriptors"});
        }
    }

    void CleanProcesses(int slotIndex, ESlotType slotType) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Start clean processes")
            .With("SlotIndex", slotIndex);

        try {
            EnsureJobProxyFinished(slotIndex, true);

            auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);

            YT_TLOG_DEBUG("Destroy job subcontainers for slot")
                .With("SlotContainer", slotContainer)
                .With("SlotIndex", slotIndex);

            DestroyAllSubcontainers(slotContainer);

            YT_TLOG_DEBUG("Reset CPU limit and guarantee")
                .With("SlotContainer", slotContainer)
                .With("SlotIndex", slotIndex);

            // Reset CPU guarantee.
            WaitFor(PortoExecutor_->SetContainerProperty(
                slotContainer,
                "cpu_guarantee",
                "0.05c"))
                .ThrowOnError();

            // Reset CPU limit.
            WaitFor(PortoExecutor_->SetContainerProperty(
                slotContainer,
                "cpu_limit",
                "0"))
                .ThrowOnError();

            YT_TLOG_DEBUG("Drop reference to a job proxy process")
                .With("SlotContainer", slotContainer)
                .With("SlotIndex", slotIndex);

            // Drop reference to a process if there were any.
            JobProxyProcesses_.erase(slotIndex);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes")
                .With("slot_index", slotIndex)
                .With("slot_type", slotType)
                .With(ex);

            Disable(error);
            THROW_ERROR error;
        }
    }

    int GetUserId(int slotIndex) const override
    {
        return ConcreteConfig_->StartUid + slotIndex;
    }

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const std::string& path, int locationIndex) override
    {
        return CreatePortoJobDirectoryManager(
            Bootstrap_->GetDynamicConfig()->ExecNode->SlotManager->JobDirectoryManager,
            Bootstrap_->GetConfig()->DataNode->VolumeManager->EnableDiskQuota,
            path,
            locationIndex);
    }

    TFuture<std::vector<TShellCommandResult>> RunCommands(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const TRootFS& rootFS,
        const std::string& user,
        const std::optional<std::vector<TDevice>>& devices,
        const std::optional<std::string>& hostName,
        const std::vector<TIP6Address>& ipAddresses,
        std::string tag,
        bool throwOnFailedCommand) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto creationState = GetOrCrash(ProcessCreationStates_, slotIndex);
        return BIND([this, this_ = MakeStrong(this), slotIndex, slotType, jobId, commands, rootFS, user, devices, hostName, ipAddresses, tag, throwOnFailedCommand, creationState] {
            std::vector<TShellCommandResult> outputs;
            outputs.reserve(commands.size());

            for (int index = 0; index < std::ssize(commands); ++index) {
                const auto& command = commands[index];
                YT_TLOG_DEBUG("Running command")
                    .With("JobId", jobId)
                    .With("Path", command->Path)
                    .With("Args", command->Args)
                    .With("User", user)
                    .With("Devices", devices);

                auto launcher = CreatePortoInstanceLauncher(
                    Format("%v_%v_%v",
                        GetFullJobContainerName(slotIndex, slotType, CommandContainerPrefix, jobId),
                        tag,
                        index),
                    PortoExecutor_);
                launcher->SetRoot(rootFS);
                launcher->SetUser(user);
                if (devices) {
                    launcher->SetDevices(*devices);
                }

                if (hostName) {
                    launcher->SetHostName(*hostName);
                    launcher->SetIPAddresses(ipAddresses, /*enableNat64*/ false);
                }

                auto guard = WaitFor(AcquireProcessCreationGuard(creationState))
                    .ValueOrThrow();

                auto instanceOrError = WaitFor(launcher->Launch(command->Path, command->Args, command->EnvironmentVariables));
                guard->Release();

                YT_TLOG_WARNING_IF(!instanceOrError.IsOK(), "Failed to launch command")
                    .With("JobId", jobId)
                    .With(instanceOrError);
                const auto& instance = instanceOrError.ValueOrThrow();

                auto error = WaitFor(instance->Wait());

                TShellCommandResult instanceResult{
                    .Stdout = instance->GetStdout(),
                    .Stderr = instance->GetStderr(),
                };

                if (!error.IsOK()) {
                    YT_TLOG_WARNING("Command failed")
                        .With("JobId", jobId)
                        .With("Stderr", instanceResult.Stderr)
                        .With("Stdout", instanceResult.Stdout)
                        .With(error);

                    error.Add("stdout", TruncateString(instanceResult.Stdout, MaxCommandOutputSizeInError));
                    error.Add("stderr", TruncateString(instanceResult.Stderr, MaxCommandOutputSizeInError));

                    if (throwOnFailedCommand) {
                        THROW_ERROR error;
                    }
                    instanceResult.Error = error;
                }

                outputs.push_back(instanceResult);

                YT_TLOG_DEBUG("Command finished")
                    .With("JobId", jobId)
                    .With("Path", command->Path)
                    .With("Args", command->Args)
                    .With("EnvironmentVariables", command->EnvironmentVariables);
            }

            return outputs;
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
    }

private:
    static constexpr TStringBuf JobsMetaContainerName = "jm";
    static constexpr TStringBuf JobsMetaContainerIdleSuffix = "_idle";
    static constexpr TStringBuf JobProxyContainerPrefix = "/jp";
    static constexpr TStringBuf CommandContainerPrefix = "/cm";

    TPortoJobEnvironmentConfigPtr ConcreteConfig_;

    //! Main Porto connection for lightweight operations.
    IPortoExecutorPtr PortoExecutor_;

    //! Porto connection for container creation that could be long.
    IPortoExecutorPtr PortoCreateExecutor_;

    //! Porto connection used for container destruction, which is
    //! possibly a long operation and requires additional retries.
    IPortoExecutorPtr PortoDestroyExecutor_;
    NProfiling::TCounter ContainerDestroyFailureCounter_;

    IInstancePtr SelfInstance_;
    IInstancePtr MetaInstance_;
    IInstancePtr MetaIdleInstance_;

    std::atomic<bool> ShouldCloseDescriptors_;

    double CpuLimit_ = 0.0;
    double IdleCpuLimit_ = 0.0;
    double IdleCpuFraction_ = 0.0;
    int SlotCount_ = 0;

    void DestroyAllSubcontainers(const std::string& rootContainer)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Start destroy subcontainers")
            .With("RootContainer", rootContainer);

        NProfiling::TWallTimer timer;

        // Retry destruction until success.
        bool destroyed = false;
        while (!destroyed) {
            YT_TLOG_DEBUG("Start container destruction attempt")
                .With("RootContainer", rootContainer);

            auto containers = WaitFor(PortoDestroyExecutor_->ListSubcontainers(rootContainer, false))
                .ValueOrThrow();

            std::vector<TFuture<void>> futures;
            for (const auto& container : containers) {
                YT_TLOG_DEBUG("Destroy subcontainer")
                    .With("Container", container);
                futures.push_back(PortoDestroyExecutor_->DestroyContainer(container));
            }

            auto result = WaitFor(AllSet(futures));
            if (result.IsOK()) {
                destroyed = true;
                for (const auto& error : result.Value()) {
                    if (!error.IsOK() &&
                        !error.FindMatching(EPortoErrorCode::ContainerDoesNotExist))
                    {
                        destroyed = false;
                        YT_TLOG_WARNING("Failed to destroy subcontainers")
                            .With("Container", rootContainer)
                            .With(error);
                    }
                }
            } else {
                YT_TLOG_WARNING("Failed to destroy subcontainers")
                    .With("Container", rootContainer)
                    .With(result);
            }

            if (!destroyed) {
                ContainerDestroyFailureCounter_.Increment(1);
                TDelayedExecutor::WaitForDuration(ConcreteConfig_->ContainerDestructionBackoff);
            }
        }

        YT_TLOG_DEBUG("Finish destroy subcontainers")
            .With("RootContainer", rootContainer)
            .With("TimeElapsed", timer.GetElapsedTime());
    }

    void DoInit(int slotCount, double cpuLimit, double idleCpuFraction) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto gpuConfig = Bootstrap_->GetConfig()->ExecNode->GpuManager;
        YT_TLOG_FATAL_IF(gpuConfig->Enable && gpuConfig->GpuFlavor != EGpuFlavor::Nvidia,
            "Porto job environment does not support non-Nvidia GPU flavor")
            .With("GpuFlavor", gpuConfig->GpuFlavor);

        // We use weak ptr to avoid cyclic references between container manager and job environment.
        auto portoFatalErrorHandler = BIND(&TPortoJobEnvironment::Disable, MakeWeak(this));

        PortoExecutor_->SubscribeFailed(portoFatalErrorHandler);
        SelfInstance_ = GetSelfPortoInstance(PortoExecutor_);

        CpuLimit_ = cpuLimit;
        IdleCpuFraction_ = idleCpuFraction;
        IdleCpuLimit_ = CalculateIdleCpuLimit();
        SlotCount_ = slotCount;

        YT_VERIFY(!ConcreteConfig_->UseDaemonSubcontainer || SelfInstance_->GetParentName());
        auto baseContainer = ConcreteConfig_->UseDaemonSubcontainer
            ? *SelfInstance_->GetParentName()
            : SelfInstance_->GetName();

        // If we are in the top-level container of current namespace, use names without prefix.
        auto metaInstanceName = baseContainer.empty()
            ? std::string(JobsMetaContainerName)
            : std::string(Format("%v/%v", baseContainer, JobsMetaContainerName));

        auto metaIdleInstanceName = Format("%v%v", metaInstanceName, JobsMetaContainerIdleSuffix);

        auto createContainer = [this] (const std::string& name) {
            try {
                // Cleanup leftovers during restart.
                WaitFor(PortoExecutor_->DestroyContainer(name))
                    .ThrowOnError();
            } catch (const TErrorException& ex) {
                // If container doesn't exist it's ok.
                if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                    throw;
                }
            }

            WaitFor(PortoExecutor_->CreateContainer(name))
                .ThrowOnError();
        };

        createContainer(metaInstanceName);
        createContainer(metaIdleInstanceName);

        MetaInstance_ = GetPortoInstance(
            PortoExecutor_,
            metaInstanceName);
        MetaIdleInstance_ = GetPortoInstance(
            PortoExecutor_,
            metaIdleInstanceName);

        MetaInstance_->SetIOWeight(ConcreteConfig_->JobsIOWeight);
        MetaIdleInstance_->SetIOWeight(ConcreteConfig_->JobsIOWeight);

        UpdateContainerCpuLimits();
    }

    TFuture<void> InitSlot(int slotIndex) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto slotInitFuture = BIND([this, this_ = MakeStrong(this), slotIndex] {
            for (auto slotType : {ESlotType::Common, ESlotType::Idle}) {
                auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);
                WaitFor(PortoCreateExecutor_->CreateContainer(slotContainer))
                    .ThrowOnError();

                // This forces creation of CPU cgroup for this container.
                WaitFor(PortoExecutor_->SetContainerProperty(
                    slotContainer,
                    "cpu_guarantee",
                    "0.05c"))
                    .ThrowOnError();

                WaitFor(PortoExecutor_->SetContainerProperty(
                    slotContainer,
                    "controllers",
                    "freezer;cpu;cpuacct;cpuset;net_cls"))
                    .ThrowOnError();

                if (slotType == ESlotType::Idle) {
                    WaitFor(PortoExecutor_->SetContainerProperty(
                        slotContainer,
                        "cpu_policy",
                        "idle"))
                        .ThrowOnError();
                }
            }
            CleanProcesses(slotIndex, ESlotType::Common);
            CleanProcesses(slotIndex, ESlotType::Idle);
        })
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run();
        return slotInitFuture;
    }

    std::string GetFullSlotMetaContainerName(int slotIndex, ESlotType slotType)
    {
        auto instanceName = slotType == ESlotType::Common
            ? MetaInstance_->GetName()
            : MetaIdleInstance_->GetName();
        return Format("%v/s_%03d", instanceName, slotIndex);
    }

    std::string GetJobContainerName(TStringBuf prefix, TJobId jobId)
    {
        auto jobIdAsGuid = jobId.Underlying();
        return ConcreteConfig_->UseShortContainerNames
            ? std::string(prefix)
            : std::string(Format("%v-%x-%x", prefix, jobIdAsGuid.Parts32[3], jobIdAsGuid.Parts32[2]));
    }

    std::string GetFullJobContainerName(int slotIndex, ESlotType slotType, TStringBuf prefix, TJobId jobId)
    {
        return GetFullSlotMetaContainerName(slotIndex, slotType) + GetJobContainerName(prefix, jobId);
    }

    IInstanceLauncherPtr CreateJobProxyInstanceLauncher(int slotIndex, ESlotType slotType, TJobId jobId)
    {
        auto launcher = CreatePortoInstanceLauncher(
            GetFullJobContainerName(slotIndex, slotType, JobProxyContainerPrefix, jobId),
            PortoExecutor_);
        if (ConcreteConfig_->JobProxyCpuWeight) {
            launcher->SetCpuWeight(*ConcreteConfig_->JobProxyCpuWeight);
        }
        launcher->SetEnablePorto(EEnablePorto::Full);
        launcher->SetIsolate(false);
        return launcher;
    }

    void UpdateSlotCpuSet(int slotIndex, ESlotType slotType, TStringBuf cpuSet) override
    {
        auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);

        YT_TLOG_DEBUG("Start update slot cpu_set")
            .With("SlotType", slotType)
            .With("SlotIndex", slotIndex)
            .With("CpuSet", cpuSet)
            .With("SlotContainer", slotContainer);

        WaitFor(PortoExecutor_->SetContainerProperty(
            slotContainer,
            "cpu_set",
            std::string{cpuSet}))
        .ThrowOnError();

        YT_TLOG_DEBUG("Finish update slot cpu_set")
            .With("SlotType", slotType)
            .With("SlotIndex", slotIndex)
            .With("CpuSet", cpuSet)
            .With("SlotContainer", slotContainer);
    }

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& /*config*/,
        int slotIndex,
        ESlotType slotType,
        TJobId jobId) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto launcher = CreateJobProxyInstanceLauncher(slotIndex, slotType, jobId);
        return New<TPortoProcess>(JobProxyProgramName, launcher);
    }

    void UpdateContainerCpuLimits()
    {
        if (MetaInstance_) {
            MetaInstance_->SetCpuLimit(CpuLimit_ - IdleCpuLimit_);
        }
        if (MetaIdleInstance_) {
            MetaIdleInstance_->SetCpuLimit(IdleCpuLimit_);
        }
    }

    void UpdateCpuLimit(double cpuLimit) override
    {
        if (std::abs(CpuLimit_ - cpuLimit) < CpuUpdatePrecision) {
            return;
        }

        CpuLimit_ = cpuLimit;
        IdleCpuLimit_ = CalculateIdleCpuLimit();
        UpdateContainerCpuLimits();

    }

    void UpdateIdleCpuFraction(double idleCpuFraction)
    {
        IdleCpuFraction_ = idleCpuFraction;
        IdleCpuLimit_ = CalculateIdleCpuLimit();
        UpdateContainerCpuLimits();
    }

    double CalculateIdleCpuLimit() const
    {
        return SelfInstance_
            ? std::max(0., CpuLimit_ - SelfInstance_->GetCpuGuarantee()) * IdleCpuFraction_
            : 0;
    }

    void ClearSlotCpuSets()
    {
        for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
            UpdateSlotCpuSet(slotIndex, ESlotType::Common, EmptyCpuSet);
            UpdateSlotCpuSet(slotIndex, ESlotType::Idle, EmptyCpuSet);
        }
    }

    double GetCpuLimit(ESlotType slotType) const override
    {
        switch (slotType) {
            case ESlotType::Common: {
                return CpuLimit_ - IdleCpuLimit_;
            }
            case ESlotType::Idle: {
                return IdleCpuLimit_;
            }
        }
    }

    i64 GetMajorPageFaultCount() const override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        if (!SelfInstance_) {
            THROW_ERROR_EXCEPTION("Job environment disabled");
        }

        return SelfInstance_->GetMajorPageFaultCount();
    }

    void EnrichJobEnvironmentConfig(int /*slotIndex*/, TNonNullPtr<NJobProxy::TJobProxyInternalConfig> /*jobProxyConfig*/) const override
    { }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        return CreatePortoJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager,
            Bootstrap_->GetGpuManager());
    }

    IUserSlotPtr CreateUserSlot(TUserSlotContext context) override
    {
        return CreatePortoUserSlot(std::move(context));
    }

    TJobFSSecretaryPtr CreateJobFSSecretary(NLogging::TLogger logger) override
    {
        return New<TPortoJobFSSecretary>(&*Bootstrap_, std::move(logger));
    }

    IVolumeManagerPtr CreateVolumeManager(const std::vector<TSlotLocationConfigPtr>&) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto future = CreatePortoVolumeManager(
            Bootstrap_->GetConfig()->DataNode,
            Bootstrap_->GetDynamicConfigManager(),
            CreateVolumeArtifactCacheAdapter(Bootstrap_->GetArtifactCache()),
            Bootstrap_->GetControlInvoker(),
            Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TmpfsLayers),
            &*Bootstrap_);

        return WaitFor(future)
            .ValueOrThrow(
                EErrorCode::PortoVolumeManagerFailure,
                "Failed to initialize volume manager");
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreatePortoJobEnvironment(NJobProxy::TJobEnvironmentConfig config, TNonNullPtr<IBootstrap> bootstrap)
{
    return New<TPortoJobEnvironment>(std::move(config), bootstrap);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
