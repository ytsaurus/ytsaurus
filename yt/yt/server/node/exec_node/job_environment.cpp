#include "job_environment.h"

#include "bootstrap.h"
#include "job_directory_manager.h"
#include "slot_manager.h"
#include "private.h"
#include "yt/yt/core/concurrency/delayed_executor.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/library/containers/helpers.h>
#include <yt/yt/library/containers/process.h>

#ifdef _linux_
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/instance.h>
#endif

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/library/containers/cri/cri_executor.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/misc/proc.h>

#include <util/generic/guid.h>

#include <util/system/execpath.h>
#include <util/system/user.h>

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NContainers;
using namespace NContainers::NCri;
using namespace NDataNode;
using namespace NYTree;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TProcessJobEnvironmentBase
    : public IJobEnvironment
{
public:
    TProcessJobEnvironmentBase(
        TJobEnvironmentConfigPtr config,
        IBootstrap* bootstrap)
        : BasicConfig_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    void Init(int slotCount, double cpuLimit, double idleCpuFraction) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        Bootstrap_->SubscribePopulateAlerts(
            BIND(&TProcessJobEnvironmentBase::PopulateAlerts, MakeWeak(this)));
        // Shutdown all possible processes.
        try {
            DoInit(slotCount, cpuLimit, idleCpuFraction);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean up processes during initialization")
                << ex;
            Disable(error);
        }
    }

    TFuture<void> RunJobProxy(
        const TJobProxyInternalConfigPtr& config,
        ESlotType slotType,
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateEnabled();

        try {
            const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
            const auto& dynamicConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;
            if (dynamicConfig && dynamicConfig->EnableNumaNodeScheduling) {
                if (numaNodeAffinity) {
                    UpdateSlotCpuSet(slotIndex, slotType, numaNodeAffinity->CpuSet);
                } else {
                    // Without cpu restrictions.
                    UpdateSlotCpuSet(slotIndex, slotType, EmptyCpuSet);
                }
            }
            auto process = CreateJobProxyProcess(config, slotIndex, slotType, jobId);

            process->AddArguments({
                "--config", ProxyConfigFileName,
                "--operation-id", ToString(operationId),
                "--job-id", ToString(jobId)
            });

            const auto& stderrPath = config->StderrPath;
            if (stderrPath) {
                process->AddArguments({
                    "--stderr-path", *stderrPath
                });
            }

            process->SetWorkingDirectory(workingDirectory);

            AddArguments(process, slotIndex);

            YT_LOG_INFO("Spawn job proxy (SlotType: %v, SlotIndex: %v, JobId: %v, OperationId: %v, WorkingDirectory: %v, StderrPath: %v)",
                slotType,
                slotIndex,
                jobId,
                operationId,
                workingDirectory,
                stderrPath);

            TJobProxyProcess jobProxyProcess;
            jobProxyProcess.Process = process;
            jobProxyProcess.Result = BIND([=] () {
                    // Make forks outside controller thread.
                    return process->Spawn();
                })
                .AsyncVia(ActionQueue_->GetInvoker())
                .Run();

            JobProxyProcesses_[slotIndex] = jobProxyProcess;
            return jobProxyProcess.Result;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to spawn job proxy") << ex;
            Disable(error);

            THROW_ERROR error;
        }
    }

    bool IsEnabled() const override
    {
        return Enabled_.load();
    }

    void UpdateCpuLimit(double /*cpuLimit*/) override
    { }

    double GetCpuLimit(ESlotType /*slotType*/) const override
    {
        return 0;
    }

    i64 GetMajorPageFaultCount() const override
    {
        return 0;
    }

    TFuture<void> RunSetupCommands(
        int /*slotIndex*/,
        ESlotType /*slotType*/,
        TJobId /*jobId*/,
        const std::vector<TShellCommandConfigPtr>& /*commands*/,
        const TRootFS& /*rootFS*/,
        const TString& /*user*/,
        const std::optional<std::vector<TDevice>>& /*devices*/,
        int /*startIndex*/) override
    {
        THROW_ERROR_EXCEPTION("Setup scripts are not yet supported by %Qlv environment",
            BasicConfig_->Type);
    }

    void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& /*oldConfig*/,
        const TSlotManagerDynamicConfigPtr& /*newConfig*/) override
    { }

protected:
    struct TJobProxyProcess
    {
        TProcessBasePtr Process;
        TFuture<void> Result;
    };

    const TJobEnvironmentConfigPtr BasicConfig_;
    IBootstrap* const Bootstrap_;

    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("JobEnvironment");

    THashMap<int, TJobProxyProcess> JobProxyProcesses_;

    TFuture<void> JobProxyResult_;

    std::atomic<bool> Enabled_ = true;

    TAtomicObject<TError> Alert_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoInit(int slotCount, double /*cpuLimit*/, double /*idleCpuFraction*/)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            CleanProcesses(slotIndex);
        }
    }

    void ValidateEnabled() const
    {
        if (!Enabled_.load()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::JobEnvironmentDisabled,
                "Job environment %Qlv is disabled",
                BasicConfig_->Type);
        }
    }

    void EnsureJobProxyFinished(int slotIndex, bool kill)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto it = JobProxyProcesses_.find(slotIndex);
        if (it != JobProxyProcesses_.end()) {
            if (kill) {
                try {
                    it->second.Process->Kill(SIGKILL);
                } catch (const std::exception& ex) {
                    // If we failed to kill container we ignore it for now.
                    YT_LOG_WARNING(TError(ex), "Failed to kill container properly (SlotIndex: %v)", slotIndex);
                }
            }

            // Ensure that job proxy process finished.
            auto error = WaitFor(it->second.Result);
            YT_LOG_INFO(error, "Job proxy process finished (SlotIndex: %v)", slotIndex);
            // Drop reference to a process.
            JobProxyProcesses_.erase(it);
        }
    }

    void Disable(const TError& error)
    {
        if (!Enabled_.exchange(false)) {
            return;
        }

        auto alert = TError(EErrorCode::JobEnvironmentDisabled, "Job environment is disabled") << error;
        YT_LOG_ERROR(alert);

        Alert_.Store(alert);

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        const auto& dynamicConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;

        if (dynamicConfig->AbortOnJobsDisabled) {
            YT_LOG_FATAL(alert);
        }

        if (dynamicConfig->EnableJobEnvironmentResurrection) {
            YT_UNUSED_FUTURE(BIND(&TSlotManager::Disable, Bootstrap_->GetSlotManager())
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(error));
        }
    }

    virtual void AddArguments(TProcessBasePtr /*process*/, int /*slotIndex*/)
    { }

private:
    void PopulateAlerts(std::vector<TError>* alerts)
    {
        auto alert = Alert_.Load();
        if (!alert.IsOK()) {
            alerts->push_back(alert);
        }
    }

    virtual TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& /*config*/,
        int /*slotIndex*/,
        ESlotType /*slotType*/,
        TJobId /*jobId*/)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return New<TSimpleProcess>(JobProxyProgramName);
    }

    virtual void UpdateSlotCpuSet(int /*slotIndex*/, ESlotType /*slotType*/, TStringBuf /*cpuSet*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TSimpleJobEnvironment(
        TSimpleJobEnvironmentConfigPtr config,
        IBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    { }

    void CleanProcesses(int slotIndex, ESlotType slotType) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateEnabled();

        YT_LOG_DEBUG("Start clean processes (SlotIndex: %v)", slotIndex);

        try {
            EnsureJobProxyFinished(slotIndex, true);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes")
                << TErrorAttribute("slot_index", slotIndex)
                << TErrorAttribute("slot_type", slotType)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }

        YT_LOG_DEBUG("Finish clean processes (SlotIndex: %v)", slotIndex);
    }

    int GetUserId(int /*slotIndex*/) const override
    {
        return ::getuid();
    }

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int /*locationIndex*/) override
    {
        return CreateSimpleJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return CreateSimpleJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager);
    }

private:
    const TSimpleJobEnvironmentConfigPtr Config_;

    const TActionQueuePtr MounterThread_ = New<TActionQueue>("Mounter");

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& /*config*/,
        int /*slotIndex*/,
        ESlotType /*slotType*/,
        TJobId /*jobId*/) override
    {
        auto process = New<TSimpleProcess>(JobProxyProgramName);
        process->CreateProcessGroup();
        return process;
    }
};

class TTestingJobEnvironment
    : public TSimpleJobEnvironment
{
public:
    TTestingJobEnvironment(
        TTestingJobEnvironmentConfigPtr config,
        IBootstrap* bootstrap)
        : TSimpleJobEnvironment(config, bootstrap)
        , Config_(std::move(config))
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

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

constexpr int MaxStderrSizeInError = 500;

constexpr double CpuUpdatePrecision = 0.01;

class TPortoJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TPortoJobEnvironment(
        TPortoJobEnvironmentConfigPtr config,
        IBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
        , PortoExecutor_(CreatePortoExecutor(
            Config_->PortoExecutor,
            "env_spawn",
            ExecNodeProfiler.WithPrefix("/job_environment/porto")))
        , DestroyPortoExecutor_(CreatePortoExecutor(
            Config_->PortoExecutor,
            "env_destroy",
            ExecNodeProfiler.WithPrefix("/job_environment/porto_destroy")))
        , ContainerDestroyFailureCounter_(ExecNodeProfiler.WithPrefix("/job_environment").Counter("/container_destroy_failures"))
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        const auto& slotManagerConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;
        ShouldCloseDescriptors_.store(slotManagerConfig->ShouldCloseDescriptors);
    }

    void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig) override
    {
        auto environmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(newConfig->JobEnvironment);

        if (environmentConfig->Type == EJobEnvironmentType::Porto) {
            auto portoConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(newConfig->JobEnvironment);

            if (auto executor = PortoExecutor_) {
                executor->OnDynamicConfigChanged(portoConfig->PortoExecutor);
            }

            if (auto executor = DestroyPortoExecutor_) {
                executor->OnDynamicConfigChanged(portoConfig->PortoExecutor);
            }
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
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateEnabled();

        YT_LOG_DEBUG("Start clean processes (SlotIndex: %v)", slotIndex);

        try {
            EnsureJobProxyFinished(slotIndex, true);

            auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);

            YT_LOG_DEBUG(
                "Destroy job subcontainers for slot (SlotContainer: %v, SlotIndex: %v)",
                slotContainer,
                slotIndex);

            DestroyAllSubcontainers(slotContainer);

            YT_LOG_DEBUG(
                "Reset CPU limit and guarantee (SlotContainer: %v, SlotIndex: %v)",
                slotContainer,
                slotIndex);

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

            YT_LOG_DEBUG(
                "Drop reference to a job proxy process (SlotContainer: %v, SlotIndex: %v)",
                slotContainer,
                slotIndex);

            // Drop reference to a process if there were any.
            JobProxyProcesses_.erase(slotIndex);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes")
                << TErrorAttribute("slot_index", slotIndex)
                << TErrorAttribute("slot_type", slotType)
                << ex;

            Disable(error);
            THROW_ERROR error;
        }
    }

    int GetUserId(int slotIndex) const override
    {
        return Config_->StartUid + slotIndex;
    }

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int locationIndex) override
    {
        return CreatePortoJobDirectoryManager(Bootstrap_->GetConfig()->DataNode->VolumeManager, path, locationIndex);
    }

    TFuture<void> RunSetupCommands(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const std::vector<TShellCommandConfigPtr>& commands,
        const TRootFS& rootFS,
        const TString& user,
        const std::optional<std::vector<TDevice>>& devices,
        int startIndex) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return BIND([this_ = MakeStrong(this), slotIndex, slotType, jobId, commands, rootFS, user, devices, startIndex] {
            for (int index = 0; index < std::ssize(commands); ++index) {
                const auto& command = commands[index];
                YT_LOG_DEBUG(
                    "Running setup command (JobId: %v, Path: %v, Args: %v, User: %v, Devices: %v)",
                    jobId,
                    command->Path,
                    command->Args,
                    user,
                    devices);
                auto launcher = this_->CreateSetupInstanceLauncher(slotIndex, slotType, jobId, rootFS, user, startIndex + index);
                if (devices) {
                    launcher->SetDevices(*devices);
                }

                auto instanceOrError = WaitFor(launcher->Launch(command->Path, command->Args, {}));
                YT_LOG_WARNING_IF(!instanceOrError.IsOK(), instanceOrError, "Failed to launch setup command (JobId: %v)",
                    jobId);
                const auto& instance = instanceOrError.ValueOrThrow();

                auto error = WaitFor(instance->Wait());
                if (!error.IsOK()) {
                    auto instanceStderr = instance->GetStderr();
                    YT_LOG_WARNING(error, "Setup command failed (JobId: %v, Stderr: %v)",
                        jobId,
                        instanceStderr);

                    if (instanceStderr.size() > MaxStderrSizeInError) {
                        error.MutableAttributes()->Set("stderr_truncated", true);
                        instanceStderr = instanceStderr.substr(0, MaxStderrSizeInError);
                    }
                    error.MutableAttributes()->Set("stderr", instanceStderr);
                    THROW_ERROR error;
                }

                YT_LOG_DEBUG(
                    "Setup command finished (JobId: %v, Path: %v, Args: %v)",
                    jobId,
                    command->Path,
                    command->Args);
            }
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
    }

private:
    static constexpr TStringBuf JobsMetaContainerName = "jm";
    static constexpr TStringBuf JobsMetaContainerIdleSuffix = "_idle";
    static constexpr TStringBuf JobProxyContainerPrefix = "/jp";
    static constexpr TStringBuf SetupCommandContainerPrefix = "/sc";

    const TPortoJobEnvironmentConfigPtr Config_;

    //! Main Porto connection for container creation and lightweight operations.
    IPortoExecutorPtr PortoExecutor_;

    //! Porto connection used for container destruction, which is
    //! possibly a long operation and requires additional retries.
    IPortoExecutorPtr DestroyPortoExecutor_;
    NProfiling::TCounter ContainerDestroyFailureCounter_;

    IInstancePtr SelfInstance_;
    IInstancePtr MetaInstance_;
    IInstancePtr MetaIdleInstance_;

    std::atomic_bool ShouldCloseDescriptors_;

    double CpuLimit_;
    double IdleCpuLimit_;
    double IdleCpuFraction_;
    int SlotCount_;

    void DestroyAllSubcontainers(const TString& rootContainer)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG(
            "Start destroy subcontainers (RootContainer: %v)",
            rootContainer);

        // Retry destruction until success.
        bool destroyed = false;
        while (!destroyed) {
            YT_LOG_DEBUG("Start container destruction attempt (RootContainer: %v)", rootContainer);

            auto containers = WaitFor(DestroyPortoExecutor_->ListSubcontainers(rootContainer, false))
                .ValueOrThrow();

            std::vector<TFuture<void>> futures;
            for (const auto& container : containers) {
                YT_LOG_DEBUG("Destroy subcontainer (Container: %v)", container);
                futures.push_back(DestroyPortoExecutor_->DestroyContainer(container));
            }

            auto result = WaitFor(AllSet(futures));
            if (result.IsOK()) {
                destroyed = true;
                for (const auto& error : result.Value()) {
                    if (!error.IsOK() &&
                        !error.FindMatching(EPortoErrorCode::ContainerDoesNotExist))
                    {
                        destroyed = false;
                        YT_LOG_WARNING(error, "Failed to destroy subcontainers (Container: %v)",
                            rootContainer);
                    }
                }
            } else {
                YT_LOG_WARNING(result, "Failed to destroy subcontainers (Container: %v)",
                    rootContainer);
            }

            if (!destroyed) {
                ContainerDestroyFailureCounter_.Increment(1);
                TDelayedExecutor::WaitForDuration(Config_->ContainerDestructionBackoff);
            }
        }

        YT_LOG_DEBUG("Finish destroy subcontainers (RootContainer: %v)", rootContainer);
    }

    void DoInit(int slotCount, double cpuLimit, double idleCpuFraction) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto portoFatalErrorHandler = BIND([weakThis_ = MakeWeak(this)](const TError& error) {
            // We use weak ptr to avoid cyclic references between container manager and job environment.
            auto this_ = weakThis_.Lock();
            if (this_) {
                this_->Disable(error);
            }
        });

        PortoExecutor_->SubscribeFailed(portoFatalErrorHandler);
        SelfInstance_ = GetSelfPortoInstance(PortoExecutor_);

        CpuLimit_ = cpuLimit;
        IdleCpuFraction_ = idleCpuFraction;
        IdleCpuLimit_ = CalculateIdleCpuLimit();
        SlotCount_ = slotCount;

        YT_VERIFY(!Config_->UseDaemonSubcontainer || SelfInstance_->GetParentName());
        auto baseContainer = Config_->UseDaemonSubcontainer
            ? *SelfInstance_->GetParentName()
            : SelfInstance_->GetName();

        // If we are in the top-level container of current namespace, use names without prefix.
        auto metaInstanceName = baseContainer.empty()
            ? TString(JobsMetaContainerName)
            : Format("%v/%v", baseContainer, JobsMetaContainerName);

        auto metaIdleInstanceName = metaInstanceName + JobsMetaContainerIdleSuffix;

        auto createContainer = [this] (const TString& name) {
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

        MetaInstance_->SetIOWeight(Config_->JobsIOWeight);
        MetaIdleInstance_->SetIOWeight(Config_->JobsIOWeight);

        UpdateContainerCpuLimits();

        auto createSlots = [this, slotCount] (ESlotType slotType) {
            try {
                for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                    auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);
                    WaitFor(PortoExecutor_->CreateContainer(slotContainer))
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
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to create meta containers for jobs")
                    << ex;
            }
        };

        createSlots(ESlotType::Common);
        createSlots(ESlotType::Idle);

        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            CleanProcesses(slotIndex, ESlotType::Common);
            CleanProcesses(slotIndex, ESlotType::Idle);
        }
    }

    TString GetFullSlotMetaContainerName(int slotIndex, ESlotType slotType)
    {
        auto instanceName = slotType == ESlotType::Common
            ? MetaInstance_->GetName()
            : MetaIdleInstance_->GetName();
        return Format("%v/s_%03d", instanceName, slotIndex);
    }

    TString GetJobContainerName(TStringBuf prefix, TJobId jobId)
    {
        auto jobIdAsGuid = jobId.Underlying();
        return Config_->UseShortContainerNames
            ? TString(prefix)
            : Format("%v-%x-%x", prefix, jobIdAsGuid.Parts32[3], jobIdAsGuid.Parts32[2]);
    }

    TString GetFullJobContainerName(int slotIndex, ESlotType slotType, TStringBuf prefix, TJobId jobId)
    {
        return GetFullSlotMetaContainerName(slotIndex, slotType) + GetJobContainerName(prefix, jobId);
    }

    IInstanceLauncherPtr CreateJobProxyInstanceLauncher(int slotIndex, ESlotType slotType, TJobId jobId)
    {
        auto launcher = CreatePortoInstanceLauncher(
            GetFullJobContainerName(slotIndex, slotType, JobProxyContainerPrefix, jobId),
            PortoExecutor_);
        launcher->SetEnablePorto(EEnablePorto::Full);
        launcher->SetIsolate(false);
        return launcher;
    }

    void UpdateSlotCpuSet(int slotIndex, ESlotType slotType, TStringBuf cpuSet) override
    {
        auto slotContainer = GetFullSlotMetaContainerName(slotIndex, slotType);

        YT_LOG_DEBUG(
            "Start update slot cpu_set (SlotType: %v, SlotIndex: %v, CpuSet: %v, SlotContainer: %v)",
            slotType,
            slotIndex,
            cpuSet,
            slotContainer);

        WaitFor(PortoExecutor_->SetContainerProperty(
            slotContainer,
            "cpu_set",
            TString{cpuSet}))
        .ThrowOnError();

        YT_LOG_DEBUG(
            "Finish update slot cpu_set (SlotType: %v, SlotIndex: %v, CpuSet: %v, SlotContainer: %v)",
            slotType,
            slotIndex,
            cpuSet,
            slotContainer);
    }

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& /*config*/,
        int slotIndex,
        ESlotType slotType,
        TJobId jobId) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto launcher = CreateJobProxyInstanceLauncher(slotIndex, slotType, jobId);
        return New<TPortoProcess>(JobProxyProgramName, launcher);
    }

    void UpdateContainerCpuLimits() {
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
        VERIFY_THREAD_AFFINITY(JobThread);

        if (!SelfInstance_) {
            THROW_ERROR_EXCEPTION("Job environment disabled");
        }

        return SelfInstance_->GetMajorPageFaultCount();
    }

    IInstanceLauncherPtr CreateSetupInstanceLauncher(
        int slotIndex,
        ESlotType slotType,
        TJobId jobId,
        const TRootFS& rootFS,
        const TString& user,
        int index)
    {
        auto launcher = CreatePortoInstanceLauncher(
            GetFullJobContainerName(slotIndex, slotType, SetupCommandContainerPrefix, jobId) + "_" + ToString(index),
            PortoExecutor_);
        launcher->SetRoot(rootFS);
        launcher->SetUser(user);
        return launcher;
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return CreatePortoJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager);
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

class TCriJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TCriJobEnvironment(
        TCriJobEnvironmentConfigPtr config,
        IBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
        , Executor_(CreateCriExecutor(Config_->CriExecutor))
    { }

    void DoInit(int slotCount, double cpuLimit, double /*idleCpuFraction*/) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        Executor_->CleanNamespace();

        PodDescriptors_.clear();
        PodSpecs_.clear();
        SlotCpusetCpus_.clear();

        {
            std::vector<TFuture<TCriPodDescriptor>> podFutures;

            podFutures.reserve(slotCount);
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                auto podSpec = New<NCri::TCriPodSpec>();
                podSpec->Name = Format("%v%v", SlotPodPrefix, slotIndex);
                podSpec->Resources.CpuLimit = cpuLimit;
                PodSpecs_.push_back(podSpec);
                auto podFuture = Executor_->RunPodSandbox(podSpec);
                podFutures.push_back(podFuture);

                // Wait for the first slot, otherwise containerd runs parallel
                // pull for sandbox_image (pause) if it is not cached yet.
                if (slotIndex == 0) {
                    WaitFor(podFuture)
                        .ThrowOnError();
                }

                SlotCpusetCpus_.push_back(EmptyCpuSet);
            }

            auto imageFuture = Executor_->PullImage(TCriImageDescriptor{
                .Image = Config_->JobProxyImage,
            });

            PodDescriptors_ = WaitFor(AllSucceeded(std::move(podFutures)))
                .ValueOrThrow();
            YT_VERIFY(std::ssize(PodDescriptors_) == slotCount);

            WaitFor(imageFuture)
                .ThrowOnError();
        }
    }

    void CleanProcesses(int slotIndex, ESlotType slotType) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, /*kill =*/ true);
            Executor_->CleanPodSandbox(PodDescriptors_[slotIndex]);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes")
                << TErrorAttribute("slot_index", slotIndex)
                << TErrorAttribute("slot_type", slotType)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    int GetUserId(int slotIndex) const override
    {
        return Config_->StartUid + slotIndex;
    }

    IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int /*locationIndex*/) override
    {
        return CreateSimpleJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecNode->SlotManager->DetachedTmpfsUmount);
    }

    TJobWorkspaceBuilderPtr CreateJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        // Inject default docker image for job workspace.
        if (!context.DockerImage) {
            context.DockerImage = Config_->JobProxyImage;
        }

        return CreateCriJobWorkspaceBuilder(
            invoker,
            std::move(context),
            directoryManager,
            Executor_);
    }

private:
    static constexpr TStringBuf SlotPodPrefix = "yt_slot_";
    static constexpr TStringBuf LocalBinDir = "/usr/local/bin";

    const TCriJobEnvironmentConfigPtr Config_;
    const ICriExecutorPtr Executor_;

    std::vector<TCriPodDescriptor> PodDescriptors_;
    std::vector<TCriPodSpecPtr> PodSpecs_;
    std::vector<TString> SlotCpusetCpus_;

    const TActionQueuePtr MounterThread_ = New<TActionQueue>("CriMounter");

    TProcessBasePtr CreateJobProxyProcess(
        const TJobProxyInternalConfigPtr& config,
        int slotIndex,
        ESlotType slotType,
        TJobId jobId) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(slotType == ESlotType::Common);

        auto spec = New<NCri::TCriContainerSpec>();

        spec->Name = "job-proxy";

        // Run job proxy in docker image specified for the job.
        // For now CRI job-environment does not isolate user jobs from job proxy.
        spec->Image.Image = config->DockerImage.value_or(Config_->JobProxyImage);

        spec->Labels[YTJobIdLabel] = ToString(jobId);

        if (config->DoNotSetUserId) {
            spec->Credentials.Uid = ::getuid();
        } else {
            spec->Credentials.Uid = GetUserId(slotIndex);
            // FIXME(khlebnikov): Use own group or "nogroup"
        }
        spec->Credentials.Gid = ::getgid();

        if (!spec->Environment.contains("USER")) {
            TString username;
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
        if (Bootstrap_->GetGpuManager()->HasGpuDevices()) {
            auto nvidiaDriverCapabilities = Bootstrap_
                ->GetDynamicConfig()
                ->ExecNode
                ->GpuManager
                ->DefaultNvidiaDriverCapabilities;
            spec->Environment["NVIDIA_DRIVER_CAPABILITIES"] = nvidiaDriverCapabilities;
            spec->Environment["NVIDIA_VISIBLE_DEVICES"] = JoinSeq(",", config->GpuIndexes);
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

        for (const auto& bind : Config_->JobProxyBindMounts) {
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

        if (!Config_->UseJobProxyFromImage) {
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
                .HostPath = ResolveBinaryPath(TString(ToolsProgramName)).ValueOrThrow(),
                .ReadOnly = true,
            });
        }

        spec->Resources.CpuLimit = config->ContainerCpuLimit;
        spec->Resources.MemoryLimit = config->SlotContainerMemoryLimit;

        const auto& cpusetCpu = SlotCpusetCpus_[slotIndex];
        if (cpusetCpu != EmptyCpuSet) {
            spec->Resources.CpusetCpus = cpusetCpu;
        }

        return Executor_->CreateProcess(JobProxyProgramName, spec, PodDescriptors_[slotIndex], PodSpecs_[slotIndex]);
    }

    void UpdateSlotCpuSet(int slotIndex, ESlotType slotType, TStringBuf cpuSet) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (slotType != ESlotType::Common) {
            return;
        }

        // TODO(gritukan): This is an ugly way to pass cpuset to container spec.
        // Let's live with it during testing and then refactor.
        SlotCpusetCpus_[slotIndex] = cpuSet;

        YT_LOG_DEBUG("Updated slot cpuset (SlotIndex: %v, CpuSet: %v)",
            slotIndex,
            cpuSet);
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(INodePtr configNode, IBootstrap* bootstrap)
{
    auto config = ConvertTo<TJobEnvironmentConfigPtr>(configNode);
    switch (config->Type) {
        case EJobEnvironmentType::Simple: {
            auto simpleConfig = ConvertTo<TSimpleJobEnvironmentConfigPtr>(configNode);
            return New<TSimpleJobEnvironment>(
                simpleConfig,
                bootstrap);
        }

        case EJobEnvironmentType::Porto: {
#ifdef _linux_
            auto portoConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(configNode);
            return New<TPortoJobEnvironment>(
                portoConfig,
                bootstrap);
#else
            THROW_ERROR_EXCEPTION("Porto is not supported for this platform");
#endif
        }

        case EJobEnvironmentType::Testing: {
            auto simpleConfig = ConvertTo<TTestingJobEnvironmentConfigPtr>(configNode);
            return New<TTestingJobEnvironment>(
                simpleConfig,
                bootstrap);
        }

        case EJobEnvironmentType::Cri: {
            auto criConfig = ConvertTo<TCriJobEnvironmentConfigPtr>(configNode);
            return New<TCriJobEnvironment>(
                criConfig,
                bootstrap);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
