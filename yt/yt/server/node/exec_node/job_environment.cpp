#include "job_environment.h"

#include "bootstrap.h"
#include "job_directory_manager.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/lib/misc/public.h>

#ifdef _linux_
#include <yt/yt/server/lib/containers/porto_executor.h>
#include <yt/yt/server/lib/containers/instance.h>

#include <yt/yt/server/lib/misc/process.h>
#endif

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/ytlib/tools/tools.h>
#include <yt/yt/ytlib/tools/proc.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/misc/proc.h>

#include <util/generic/guid.h>

#include <util/system/execpath.h>

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NContainers;
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

    void Init(int slotCount, double cpuLimit) override
    {
        Bootstrap_->SubscribePopulateAlerts(
            BIND(&TProcessJobEnvironmentBase::PopulateAlerts, MakeWeak(this)));
        // Shutdown all possible processes.
        try {
            DoInit(slotCount, cpuLimit);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean up processes during initialization")
                << ex;
            Disable(error);
        }
    }

    TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId,
        const std::optional<TString>& stderrPath) override
    {
        ValidateEnabled();

        try {
            auto process = CreateJobProxyProcess(slotIndex, jobId);

            process->AddArguments({
                "--config", ProxyConfigFileName,
                "--operation-id", ToString(operationId),
                "--job-id", ToString(jobId)
            });

            if (stderrPath) {
                process->AddArguments({
                    "--stderr-path", *stderrPath
                });
            }

            process->SetWorkingDirectory(workingDirectory);

            AddArguments(process, slotIndex);

            YT_LOG_INFO("Spawning job proxy (SlotIndex: %v, JobId: %v, OperationId: %v, WorkingDirectory: %v, StderrPath: %v)",
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
        return Enabled_;
    }

    void UpdateCpuLimit(double /*cpuLimit*/) override
    { }

    TFuture<void> RunSetupCommands(
        int /*slotIndex*/,
        TJobId /*jobId*/,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& /*commands*/,
        const TRootFS& /*rootFS*/,
        const TString& /*user*/,
        const std::optional<std::vector<TDevice>>& /*devices*/,
        int /*startIndex*/) override
    {
        THROW_ERROR_EXCEPTION("Setup scripts are not yet supported by %Qlv environment",
            BasicConfig_->Type);
    }

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

    bool Enabled_ = true;

    TAtomicObject<TError> Alert_;

    virtual void DoInit(int slotCount, double /*cpuLimit*/)
    {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            CleanProcesses(slotIndex);
        }
    }

    void ValidateEnabled() const
    {
        if (!Enabled_) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::JobEnvironmentDisabled,
                "Job environment %Qlv is disabled",
                BasicConfig_->Type);
        }
    }

    void EnsureJobProxyFinished(int slotIndex, bool kill)
    {
        auto it = JobProxyProcesses_.find(slotIndex);
        if (it != JobProxyProcesses_.end()) {
            if (kill) {
                try {
                    it->second.Process->Kill(SIGKILL);
                } catch (const TErrorException& ex) {
                    // If we failed to kill container we ignore it for now.
                    YT_LOG_WARNING(ex, "Failed to kill container properly (SlotIndex: %v)", slotIndex);
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
        if (!Enabled_)
            return;

        Enabled_ = false;

        auto alert = TError("Job environment is disabled") << error;

        YT_LOG_ERROR(alert);
        YT_VERIFY(!Logger.GetAbortOnAlert());

        Alert_.Store(alert);
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

    virtual TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/, TJobId /* jobId */)
    {
        return New<TSimpleProcess>(JobProxyProgramName);
    }
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

    void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, true);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes (SlotIndex: %v)",
                slotIndex) << ex;
            Disable(error);
            THROW_ERROR error;
        }
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

private:
    const TSimpleJobEnvironmentConfigPtr Config_;

    const TActionQueuePtr MounterThread_ = New<TActionQueue>("Mounter");

    TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/, TJobId /* jobId */) override
    {
        auto process = New<TSimpleProcess>(JobProxyProgramName);
        process->CreateProcessGroup();
        return process;
    }
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
            "environ",
            ExecNodeProfiler.WithPrefix("/job_environement/porto")))
    {  }

    void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, true);

            auto slotContainer = GetFullSlotMetaContainerName(MetaInstance_->GetName(), slotIndex);

            DestroyAllSubcontainers(slotContainer);

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

            // Drop reference to a process if there were any.
            JobProxyProcesses_.erase(slotIndex);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes (SlotIndex: %v)",
                slotIndex) << ex;
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
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const TRootFS& rootFS,
        const TString& user,
        const std::optional<std::vector<TDevice>>& devices,
        int startIndex) override
    {
        return BIND([this_ = MakeStrong(this), slotIndex, jobId, commands, rootFS, user, devices, startIndex] {
            for (int index = 0; index < std::ssize(commands); ++index) {
                const auto& command = commands[index];
                YT_LOG_DEBUG("Running setup command (JobId: %v, Path: %v, Args: %v)",
                    jobId,
                    command->Path,
                    command->Args);
                auto launcher = this_->CreateSetupInstanceLauncher(slotIndex, jobId, rootFS, user, startIndex + index);
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
            }
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    IPortoExecutorPtr PortoExecutor_;

    IInstancePtr MetaInstance_;

    double CpuLimit_;

    void DestroyAllSubcontainers(const TString& rootContainer)
    {
        YT_LOG_DEBUG("Started destroying subcontainers (RootContainer: %v)",
            rootContainer);

        auto containers = WaitFor(PortoExecutor_->ListSubcontainers(rootContainer, false))
            .ValueOrThrow();

        std::vector<TFuture<void>> futures;
        for (const auto& container : containers) {
            YT_LOG_DEBUG("Destroying subcontainer (Container: %v)", container);
            futures.push_back(PortoExecutor_->DestroyContainer(container));
        }

        auto throwOnError = [&] (const TError& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to destroy all subcontainers of %v",
                rootContainer);
        };

        auto result = WaitFor(AllSet(futures));
        throwOnError(result);
        for (const auto& error : result.Value()) {
            if (error.IsOK() ||
                error.FindMatching(EPortoErrorCode::ContainerDoesNotExist))
            {
                continue;
            }
            throwOnError(error);
        }

        YT_LOG_DEBUG("Finished destroying subcontainers (RootContainer: %v)",
            rootContainer);
    }

    void DoInit(int slotCount, double cpuLimit) override
    {
        auto portoFatalErrorHandler = BIND([weakThis_ = MakeWeak(this)](const TError& error) {
            // We use weak ptr to avoid cyclic references between container manager and job environment.
            auto this_ = weakThis_.Lock();
            if (this_) {
                this_->Disable(error);
            }
        });

        CpuLimit_ = cpuLimit;

        PortoExecutor_->SubscribeFailed(portoFatalErrorHandler);
        auto selfInstance = GetSelfPortoInstance(PortoExecutor_);

        YT_VERIFY(!Config_->UseDaemonSubcontainer || selfInstance->GetParentName());
        auto baseContainer = Config_->UseDaemonSubcontainer
            ? *selfInstance->GetParentName()
            : selfInstance->GetName();

        // If we are in the top-level container of current namespace, use names without prefix.
        auto metaInstanceName = baseContainer.empty()
            ? GetDefaultJobsMetaContainerName()
            : Format("%v/%v", baseContainer, GetDefaultJobsMetaContainerName());

        try {
            // Cleanup leftovers during restart.
            WaitFor(PortoExecutor_->DestroyContainer(metaInstanceName))
                .ThrowOnError();
        } catch (const TErrorException& ex) {
            // If container doesn't exist it's ok.
            if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                throw;
            }
        }

        WaitFor(PortoExecutor_->CreateContainer(metaInstanceName))
            .ThrowOnError();
        
        MetaInstance_ = GetPortoInstance(
            PortoExecutor_,
            metaInstanceName);
        MetaInstance_->SetIOWeight(Config_->JobsIOWeight);
        MetaInstance_->SetCpuLimit(CpuLimit_);
       
        try {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                auto slotContainer = GetFullSlotMetaContainerName(MetaInstance_->GetName(), slotIndex);
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
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create meta containers for jobs")
                << ex;
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, CpuLimit_);
    }

    IInstanceLauncherPtr CreateJobProxyInstanceLauncher(int slotIndex, TJobId jobId)
    {
        TString jobProxyContainerName = Config_->UseShortContainerNames
            ? "/jp"
            : Format("/jp-%v-%v", IntToString<16>(jobId.Parts32[3]), IntToString<16>(jobId.Parts32[2]));

        auto launcher = CreatePortoInstanceLauncher(
            GetFullSlotMetaContainerName(MetaInstance_->GetName(), slotIndex) + jobProxyContainerName,
            PortoExecutor_);
        launcher->SetEnablePorto(EEnablePorto::Full);
        launcher->SetIsolate(false);
        return launcher;
    }

    TProcessBasePtr CreateJobProxyProcess(int slotIndex, TJobId jobId) override
    {
        auto launcher = CreateJobProxyInstanceLauncher(slotIndex, jobId);
        return New<TPortoProcess>(JobProxyProgramName, launcher);
    }

    void UpdateCpuLimit(double cpuLimit) override
    {
        if (std::abs(CpuLimit_ - cpuLimit) < CpuUpdatePrecision) {
            return;
        }

        if (MetaInstance_) {
            MetaInstance_->SetCpuLimit(cpuLimit);
        }

        CpuLimit_ = cpuLimit;
    }

    IInstanceLauncherPtr CreateSetupInstanceLauncher(int slotIndex, TJobId jobId, const TRootFS& rootFS, const TString& user, int index)
    {
        TString setupCommandContainerJobPart = Config_->UseShortContainerNames
            ? "/sc"
            : Format("/jp-%v-%v", IntToString<16>(jobId.Parts32[3]), IntToString<16>(jobId.Parts32[2]));
        auto setupCommandContainerName = setupCommandContainerJobPart + "_" + ToString(index);

        auto launcher = CreatePortoInstanceLauncher(
            GetFullSlotMetaContainerName(MetaInstance_->GetName(), slotIndex) + setupCommandContainerName,
            PortoExecutor_);
        launcher->SetRoot(rootFS);
        launcher->SetUser(user);
        return launcher;
    }
};

#endif

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

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
