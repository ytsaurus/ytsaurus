#include "job_environment.h"

#include "job_workspace_builder.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>

#include <yt/yt/server/node/exec_node/environment/common/job_directory_manager.h>
#include <yt/yt/server/node/exec_node/environment/common/volume_manager.h>
#include <yt/yt/server/node/exec_node/environment/common/job_environment_base.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/gpu_helpers.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/server/tools/public.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/library/containers/helpers.h>

#include <yt/yt/library/containers/cri/cri_executor.h>
#include <yt/yt/library/containers/cri/image_cache.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/net/connection.h>

#include <util/string/join.h>

#include <util/system/user.h>

#ifdef _linux_
#include <grp.h>
#endif

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;

using namespace NJobProxy;
using namespace NContainers;
using namespace NContainers::NCri;

using NNet::TIP6Address;

using NServer::ExecProgramName;
using NServer::JobProxyProgramName;
using NTools::ToolsProgramName;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TCriJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TCriJobEnvironment(
        TJobEnvironmentConfig config,
        TNonNullPtr<IBootstrap> bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , ConcreteConfig_(config.GetConcrete<TCriJobEnvironmentConfig>())
        , Executor_(CreateCriExecutor(ConcreteConfig_->CriExecutor))
        , ImageCache_(CreateCriImageCache(ConcreteConfig_->CriImageCache, Executor_))
    { }

    TJobEnvironmentCapabilities GetCapabilities() const override
    {
        return {
            .SupportsSidecarVolumeMounts = false,
            .UseJobUserForPrivateSandbox = true,
            .PrivateSandboxPermissions = 0711,
        };
    }

    void EnrichJobEnvironmentConfig(int slotIndex, TNonNullPtr<NJobProxy::TJobProxyInternalConfig> jobProxyConfig) const override
    {
        auto criJobEnv = jobProxyConfig->JobEnvironment.GetConcrete<TCriJobEnvironmentConfig>();

        criJobEnv->PodDescriptor = PodDescriptors_[slotIndex];
        criJobEnv->PodSpec = PodSpecs_[slotIndex];
        if (auto gpuConfig = GetContainerGpuConfig(jobProxyConfig)) {
            criJobEnv->GpuConfig = std::move(gpuConfig);
        }
    }

    void DoInit(int slotCount, double cpuLimit, double /*idleCpuFraction*/) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        Executor_->CleanNamespace();

        PodDescriptors_.clear();
        PodSpecs_.clear();
        SlotCpusetCpus_.clear();
        CpuLimit_ = cpuLimit;
        FirstSlotInitialized_.Reset();

        PodDescriptors_.resize(slotCount);
        PodSpecs_.resize(slotCount);
        SlotCpusetCpus_.resize(slotCount);

        WaitFor(ImageCache_->Initialize())
            .ThrowOnError();

        WaitFor(Executor_->PullImage(TCriImageDescriptor{
            .Image = ConcreteConfig_->JobProxyImage,
        }))
            .ThrowOnError();
    }

    TFuture<void> InitSlot(int slotIndex) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        // Wait for first slot initialization to avoid downloading shared resources concurrently.
        if (FirstSlotInitialized_) {
            auto error = WaitForFast(FirstSlotInitialized_);
            if (!error.IsOK()) {
                return MakeFuture(error);
            }
        }

        auto podSpec = New<NCri::TCriPodSpec>();
        podSpec->Name = Format("%v%v", SlotPodPrefix, slotIndex);
        podSpec->Resources.CpuLimit = CpuLimit_;
        PodSpecs_[slotIndex] = podSpec;
        SlotCpusetCpus_[slotIndex] = EmptyCpuSet;

        auto slotInitFuture = Executor_->RunPodSandbox(podSpec);
        slotInitFuture
            .Subscribe(BIND([this, this_ = MakeStrong(this), slotIndex] (const TErrorOr<TCriPodDescriptorPtr>& result){
                YT_ASSERT_THREAD_AFFINITY(JobThread);

                if (result.IsOK()) {
                    // Error is handled outside in slot manager.
                    PodDescriptors_[slotIndex] = result.Value();
                }  else {
                    result.ThrowOnError();
                }
            })
            .Via(Bootstrap_->GetJobInvoker()));

        if (!FirstSlotInitialized_) {
            FirstSlotInitialized_ = slotInitFuture.AsVoid();
        }

        return slotInitFuture.AsVoid();
    }

    void CleanProcesses(int slotIndex, ESlotType slotType) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        try {
            EnsureJobProxyFinished(slotIndex, /*kill */ true);
            Executor_->CleanPodSandbox(PodDescriptors_[slotIndex]);
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

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const std::string& path, int /*locationIndex*/) override
    {
        return CreateHostJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        // Inject default docker image for job workspace.
        if (!context.FSSecretary->GetDockerImage() && !context.FSSecretary->GetRootVolumeParams()) {
            context.FSSecretary->SetDockerImage(ConcreteConfig_->JobProxyImage);
        }

        return CreateCriJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager,
            ImageCache_);
    }

    IVolumeManagerPtr CreateVolumeManager(const std::vector<TSlotLocationConfigPtr>& locations) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto future = CreateHostVolumeManager(
            locations,
            MounterThread_->GetInvoker(),
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);

        return WaitFor(future)
            .ValueOrThrow(
                EErrorCode::SimpleVolumeManagerFailure,
                "Failed to initialize volume manager");
    }

    TFuture<std::vector<TShellCommandResult>> RunCommands(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const TRootFS& rootFS,
        const std::string& /*user*/,
        const std::optional<std::vector<TDevice>>& /*devices*/,
        const std::optional<std::string>& /*hostName*/,
        const std::vector<TIP6Address>& /*ipAddresses*/,
        std::string tag,
        bool /*throwOnFailedCommand*/) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(slotType == ESlotType::Common);

        auto spec = New<NCri::TCriContainerSpec>();

        // Run setup using default docker image for job workspace.
        spec->Image.Image = ConcreteConfig_->JobProxyImage;

        spec->Labels[YTJobIdLabel] = ToString(jobId);

        // NB: If nvidia container runtime is used, empty list of devices
        // should be set explicitly to avoid binding all devices to job container.
        if (Bootstrap_->GetGpuManager()->HasGpuDevices()) {
            spec->Environment["NVIDIA_VISIBLE_DEVICES"] = "";
        }

        // Add bind mounts required for user job.
        for (const auto& bind : rootFS.Binds) {
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = bind.TargetPath,
                .HostPath = bind.SourcePath,
                .ReadOnly = bind.ReadOnly,
            });
        }

        const auto& cpusetCpu = SlotCpusetCpus_[slotIndex];
        if (cpusetCpu != EmptyCpuSet) {
            spec->Resources.CpusetCpus = cpusetCpu;
        }

        auto creationState = GetOrCrash(ProcessCreationStates_, slotIndex);
        std::vector<TFuture<void>> results;
        results.reserve(std::ssize(commands));
        for (int index = 0; index < std::ssize(commands); ++index) {
            spec->Name = Format("command-%v-%v", tag, index);

            const auto& command = commands[index];
            auto process = Executor_->CreateProcess(
                command->Path,
                spec,
                PodDescriptors_[slotIndex],
                PodSpecs_[slotIndex]);
            process->AddArguments(command->Args);
            // TODO(ignat): add envs.
            process->SetWorkingDirectory("/slot/home");

            results.push_back(
                BIND([=] {
                    auto guard = WaitFor(AcquireProcessCreationGuard(creationState))
                        .ValueOrThrow();
                    return process->Spawn();
                })
                .AsyncVia(ActionQueue_->GetInvoker())
                .Run());
        }

        return AllSucceeded(std::move(results))
            .Apply(BIND([this, this_ = MakeStrong(this), slotIndex](const TError& error) {
                Executor_->CleanPodSandbox(PodDescriptors_[slotIndex]);
                error.ThrowOnError();
                return std::vector<TShellCommandResult>{};
            })
            .AsyncVia(ActionQueue_->GetInvoker()));
    }

private:
    static constexpr TStringBuf SlotPodPrefix = "yt_slot_";
    static constexpr TStringBuf LocalBinDir = "/usr/local/bin";

    const TCriJobEnvironmentConfigPtr ConcreteConfig_;
    const ICriExecutorPtr Executor_;
    const ICriImageCachePtr ImageCache_;

    std::vector<TCriPodDescriptorPtr> PodDescriptors_;
    std::vector<TCriPodSpecPtr> PodSpecs_;
    std::vector<std::string> SlotCpusetCpus_;
    double CpuLimit_ = 0;
    TFuture<void> FirstSlotInitialized_;

    const TActionQueuePtr MounterThread_ = New<TActionQueue>("CriMounter");

    TContainerGpuConfigPtr GetContainerGpuConfig(const TNonNullPtr<NJobProxy::TJobProxyInternalConfig>& jobProxyConfig) const
    {
        if (!Bootstrap_->GetGpuManager()->HasGpuDevices()) {
            return nullptr;
        }

        auto config = New<TContainerGpuConfig>();

        if (Bootstrap_->GetGpuManager()->GetGpuFlavor() == EGpuFlavor::Nvidia) {
            config->NvidiaDriverCapabilities = Bootstrap_
                ->GetDynamicConfig()
                ->ExecNode
                ->GpuManager
                ->DefaultNvidiaDriverCapabilities;
            config->NvidiaVisibleDevices = JoinSeq(",", jobProxyConfig->GpuIndexes);
        }

        auto infinibandDevices = ListInfinibandDevices();
        config->InfinibandDevices.reserve(infinibandDevices.size());
        for (auto& infinibandDevice: infinibandDevices) {
            config->InfinibandDevices.push_back(std::move(infinibandDevice));
        }
        return config;
    }

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& config,
        int slotIndex,
        ESlotType slotType,
        TJobId jobId) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(slotType == ESlotType::Common);

        auto spec = New<NCri::TCriContainerSpec>();

        spec->Name = "job-proxy";

        // Run job proxy in docker image specified for the job.
        // For now CRI job-environment does not isolate user jobs from job proxy.
        spec->Image.Image = config->DockerImage.value_or(ConcreteConfig_->JobProxyImage);
        spec->Image.Id = config->DockerImageId.value_or("");

        spec->Labels[YTJobIdLabel] = ToString(jobId);

        if (config->DoNotSetUserId) {
            spec->Credentials.Uid = ::getuid();
        } else {
            spec->Credentials.Uid = GetUserId(slotIndex);
            // FIXME(khlebnikov): Use own group or "nogroup"
        }
        spec->Credentials.Gid = ::getgid();

        if (!spec->Environment.contains("USER")) {
            std::string username;
            if (config->DoNotSetUserId) {
                username = ::GetUsername();
            } else {
                username = Format("%v%v", SlotPodPrefix, slotIndex);
            }
            spec->Environment["USER"] = username;
            spec->Environment["LOGNAME"] = username;
        }

        // NB: If nvidia container runtime is used, empty list of devices
        // should be set explicitly to avoid binding all devices to job container.
        if (auto gpuContainerConfig = GetContainerGpuConfig(config)) {
            spec->Environment["NVIDIA_DRIVER_CAPABILITIES"] = gpuContainerConfig->NvidiaDriverCapabilities;
            spec->Environment["NVIDIA_VISIBLE_DEVICES"] = gpuContainerConfig->NvidiaVisibleDevices;

            for (const auto& devicePath : gpuContainerConfig->InfinibandDevices) {
                spec->BindDevices.push_back(NCri::TCriBindDevice{
                    .ContainerPath = devicePath,
                    .HostPath = devicePath,
                    .Permissions = NCri::ECriBindDevicePermissions::Read | NCri::ECriBindDevicePermissions::Write,
                });
            }

            if (!gpuContainerConfig->InfinibandDevices.empty()) {
                YT_TLOG_DEBUG("Binding InfiniBand devices to job container")
                    .With("Devices", gpuContainerConfig->InfinibandDevices);

                // Code using InfiniBand devices usually requires CAP_IPC_LOCK.
                // See https://catalog.ngc.nvidia.com/orgs/hpc/containers/preflightcheck.
                spec->CapabilitiesToAdd.push_back("IPC_LOCK");
            }
        }

        spec->BindMounts.push_back(NCri::TCriBindMount{
            .ContainerPath = config->SlotPath,
            .HostPath = config->SlotPath,
            .ReadOnly = false,
        });

        spec->BindMounts.push_back(NCri::TCriBindMount{
            .ContainerPath = "/slot",
            .HostPath = config->SlotPath,
            .ReadOnly = false,
        });

        for (const auto& bind : ConcreteConfig_->JobProxyBindMounts) {
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = bind->InternalPath,
                .HostPath = bind->ExternalPath,
                .ReadOnly = bind->ReadOnly,
            });
        }

        // Add bind mounts required for user job.
        for (const auto& bind : config->Binds) {
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = bind->InternalPath,
                .HostPath = bind->ExternalPath,
                .ReadOnly = bind->ReadOnly,
            });
        }

        if (!ConcreteConfig_->UseJobProxyFromImage) {
            auto jobProxyPath = Format("%v/%v", LocalBinDir, JobProxyProgramName);
            auto execProgramPath = Format("%v/%v", LocalBinDir, ExecProgramName);
            auto toolsProgramPath = Format("%v/%v", LocalBinDir, ToolsProgramName);

            spec->Command = {jobProxyPath};
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = jobProxyPath,
                .HostPath = ResolveBinaryPath(JobProxyProgramName).ValueOrThrow(),
                .ReadOnly = true,
            });
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = execProgramPath,
                .HostPath = ResolveBinaryPath(ExecProgramName).ValueOrThrow(),
                .ReadOnly = true,
            });
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = toolsProgramPath,
                .HostPath = ResolveBinaryPath(std::string(ToolsProgramName)).ValueOrThrow(),
                .ReadOnly = true,
            });
        }

        // Required for job_proxy to be able to work with the containerd socket.
        auto processSocketPath = [&spec] (std::string_view socketPath) {
            if (socketPath.size() >= 7 && socketPath.starts_with("unix")) {
                socketPath.remove_prefix(7);
            }
            spec->BindMounts.push_back(NCri::TCriBindMount{
                .ContainerPath = std::string(socketPath),
                .HostPath = std::string(socketPath),
                .ReadOnly = false,
            });
        };
        processSocketPath(ConcreteConfig_->CriExecutor->ImageEndpoint);
        processSocketPath(ConcreteConfig_->CriExecutor->RuntimeEndpoint);

#ifdef _linux_
        const auto& groupName = ConcreteConfig_->ContainerUserGroupName;
        if (!groupName.empty()) {
            const auto* groupRetrieved = getgrnam(groupName.c_str());
            if (groupRetrieved != nullptr) {
                spec->Credentials.Groups = {groupRetrieved->gr_gid};
            } else {
                THROW_ERROR_EXCEPTION("Cannot find user group by the specified name")
                    .With("container_user_group_name", groupName);
            }
        }
#endif

        spec->Resources.CpuLimit = config->ContainerCpuLimit;
        spec->Resources.MemoryLimit = config->SlotContainerMemoryLimit;

        const auto& cpusetCpu = SlotCpusetCpus_[slotIndex];
        if (cpusetCpu != EmptyCpuSet) {
            spec->Resources.CpusetCpus = cpusetCpu;
        }

        // Allow strace in job shell.
        spec->CapabilitiesToAdd.push_back("SYS_PTRACE");

        return Executor_->CreateProcess(JobProxyProgramName, spec, PodDescriptors_[slotIndex], PodSpecs_[slotIndex]);
    }

    void UpdateSlotCpuSet(int slotIndex, ESlotType slotType, TStringBuf cpuSet) override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        if (slotType != ESlotType::Common) {
            return;
        }

        // TODO(gritukan): This is an ugly way to pass cpuset to container spec.
        // Let's live with it during testing and then refactor.
        SlotCpusetCpus_[slotIndex] = cpuSet;

        YT_TLOG_DEBUG("Updated slot cpuset")
            .With("SlotIndex", slotIndex)
            .With("CpuSet", cpuSet);
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateCriJobEnvironment(NJobProxy::TJobEnvironmentConfig config, TNonNullPtr<IBootstrap> bootstrap)
{
    return New<TCriJobEnvironment>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
