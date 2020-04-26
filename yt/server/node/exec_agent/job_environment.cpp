#include "job_environment.h"
#include "job_directory_manager.h"
#include "private.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/node/data_node/config.h>
#include <yt/server/node/data_node/master_connector.h>

#include <yt/server/lib/misc/public.h>

#ifdef _linux_
#include <yt/server/lib/containers/porto_executor.h>
#include <yt/server/lib/containers/instance.h>

#include <yt/server/lib/misc/process.h>
#endif

#include <yt/ytlib/job_proxy/private.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/tools/tools.h>
#include <yt/ytlib/tools/proc.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/library/process/process.h>
#include <yt/core/misc/proc.h>

#include <util/generic/guid.h>

#include <util/system/execpath.h>

namespace NYT::NExecAgent {

using namespace NCGroup;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NContainers;
using namespace NDataNode;
using namespace NYTree;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetSlotProcessGroup(int slotIndex)
{
    return Format("slots/%v", slotIndex);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TProcessJobEnvironmentBase
    : public IJobEnvironment
{
public:
    TProcessJobEnvironmentBase(TJobEnvironmentConfigPtr config, TBootstrap* bootstrap)
        : BasicConfig_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    virtual void Init(int slotCount, double cpuLimit) override
    {
        // Shutdown all possible processes.
        try {
            DoInit(slotCount, cpuLimit);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean up processes during initialization")
                << ex;
            Disable(error);
        }
    }

    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        TJobId jobId,
        TOperationId operationId) override
    {
        ValidateEnabled();

        try {
            auto process = CreateJobProxyProcess(slotIndex, jobId);

            process->AddArguments({
                "--config", ProxyConfigFileName,
                "--operation-id", ToString(operationId),
                "--job-id", ToString(jobId)
            });

            process->SetWorkingDirectory(workingDirectory);

            AddArguments(process, slotIndex);

            YT_LOG_INFO("Spawning a job proxy (SlotIndex: %v, JobId: %v, OperationId: %v, WorkingDirectory: %v)",
                slotIndex,
                jobId,
                operationId,
                workingDirectory);

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

    virtual bool IsEnabled() const override
    {
        return Enabled_;
    }

    virtual void UpdateCpuLimit(double /*cpuLimit*/) override
    { }

    virtual TFuture<void> RunSetupCommands(
        int /*slotIndex*/,
        TJobId /*jobId*/,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& /*commands*/,
        const TRootFS& /*rootFS*/,
        const TString& /*user*/) override
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
    TBootstrap* const Bootstrap_;

    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("JobEnvironment");

    THashMap<int, TJobProxyProcess> JobProxyProcesses_;

    TFuture<void> JobProxyResult_;

    bool Enabled_ = true;

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

        auto masterConnector = Bootstrap_->GetMasterConnector();
        masterConnector->RegisterAlert(alert);
    }

    virtual void AddArguments(TProcessBasePtr process, int slotIndex)
    { }

private:
    virtual TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/, TJobId /* jobId */)
    {
        return New<TSimpleProcess>(JobProxyProgramName);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCGroupJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TCGroupJobEnvironment(TCGroupJobEnvironmentConfigPtr config, TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    {
        // Freezer is always implicitly supported.
        TNonOwningCGroup freezer("freezer", "slots");
        CGroups_.push_back(freezer.GetFullPath());

        for (const auto& type : Config_->SupportedCGroups) {
            TNonOwningCGroup group(type, "slots");
            CGroups_.push_back(group.GetFullPath());
        }
    }

    virtual void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        // Kill all processes via freezer.
        auto error = WaitFor(BIND([=] () {
                TNonOwningCGroup freezer("freezer", GetSlotProcessGroup(slotIndex));
                freezer.EnsureExistance();
                RunKiller(freezer.GetFullPath());
                freezer.Unlock();
            })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run());

        if (!error.IsOK()) {
            auto wrapperError = TError("Failed to kill processes in freezer process group (SlotIndex: %v)",
                slotIndex) << error;

            Disable(wrapperError);
            THROW_ERROR wrapperError;
        }

        // No need to kill, job proxy was already killed inside cgroup.
        EnsureJobProxyFinished(slotIndex, false);

        // Remove all supported cgroups.
        error = WaitFor(BIND([=] () {
                for (const auto& path : GetCGroupPaths(slotIndex)) {
                    TNonOwningCGroup group(path);
                    group.RemoveRecursive();
                }
            })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run());

        if (!error.IsOK()) {
            auto wrapperError = TError("Failed to clean up cgroups (SlotIndex: %v)",
                slotIndex) << error;
            Disable(wrapperError);
            THROW_ERROR wrapperError;
        }
    }

    virtual int GetUserId(int slotIndex) const override
    {
        return Config_->StartUid + slotIndex;
    }

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int /*locationIndex*/)
    {
        return CreateSimpleJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecAgent->SlotManager->DetachedTmpfsUmount);
    }

private:
    const TCGroupJobEnvironmentConfigPtr Config_;
    const TActionQueuePtr MounterThread_ = New<TActionQueue>("Mounter");

    std::vector<TString> CGroups_;

    virtual void DoInit(int slotCount, double cpuLimit) override
    {
        if (!HasRootPermissions()) {
            THROW_ERROR_EXCEPTION("Failed to initialize \"cgroup\" job environment: root permissions required");
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, cpuLimit);
    }

    virtual void AddArguments(TProcessBasePtr process, int slotIndex) override
    {
        for (const auto& path : GetCGroupPaths(slotIndex)) {
            process->AddArguments({"--cgroup", path});
        }
    }

    std::vector<TString> GetCGroupPaths(int slotIndex) const
    {
        std::vector<TString> result;
        for (const auto& cgroup : CGroups_) {
            result.push_back(Format("%v/%v", cgroup, slotIndex));
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TSimpleJobEnvironment(TSimpleJobEnvironmentConfigPtr config, TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    { }

    virtual void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, true);

            if (HasRootPermissions_) {
                RunTool<TKillAllByUidTool>(GetUserId(slotIndex));
            }
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes (SlotIndex: %v)",
                slotIndex) << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    virtual int GetUserId(int slotIndex) const override
    {
        return HasRootPermissions_
            ? Config_->StartUid + slotIndex
            : ::getuid();
    }

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int /*locationIndex*/) override
    {
        return CreateSimpleJobDirectoryManager(
            MounterThread_->GetInvoker(),
            path,
            Bootstrap_->GetConfig()->ExecAgent->SlotManager->DetachedTmpfsUmount);
    }

private:
    const TSimpleJobEnvironmentConfigPtr Config_;

    const bool HasRootPermissions_ = HasRootPermissions();
    const TActionQueuePtr MounterThread_ = New<TActionQueue>("Mounter");

    virtual void DoInit(int slotCount, double cpuLimit) override
    {
        if (!HasRootPermissions_ && Config_->EnforceJobControl) {
            THROW_ERROR_EXCEPTION("Failed to initialize \"simple\" job environment: "
                "\"enforce_job_control\" option set, but no root permissions provided");
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, cpuLimit);
    }

    virtual TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/, TJobId /* jobId */)
    {
        auto process = New<TSimpleProcess>(JobProxyProgramName);
        if (!HasRootPermissions_) {
            process->CreateProcessGroup();
        }
        return process;
    }
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

constexpr double CpuUpdatePrecision = 0.01;

class TPortoJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TPortoJobEnvironment(TPortoJobEnvironmentConfigPtr config, TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
        , PortoExecutor_(CreatePortoExecutor(
            Config_->PortoExecutor,
            "environ",
            ExecAgentProfiler.AppendPath("/job_environement/porto")))
    {  }

    virtual void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, true);

            DestroyAllSubcontainers(GetFullSlotMetaContainerName(
                MetaInstance_->GetAbsoluteName(),
                slotIndex));

            // Reset CPU guarantee.
            WaitFor(PortoExecutor_->SetContainerProperty(
                GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex),
                "cpu_guarantee",
                "0.05c"))
                .ThrowOnError();

            // Drop reference to a process if there were any.
            JobProxyProcesses_.erase(slotIndex);

            JobProxyInstances_.erase(slotIndex);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean processes (SlotIndex: %v)",
                slotIndex) << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    virtual int GetUserId(int slotIndex) const override
    {
        return Config_->StartUid + slotIndex;
    }

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path, int locationIndex)
    {
        return CreatePortoJobDirectoryManager(Bootstrap_->GetConfig()->DataNode->VolumeManager, path, locationIndex);
    }

    virtual TFuture<void> RunSetupCommands(
        int slotIndex,
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const TRootFS& rootFS,
        const TString& user) override
    {
        return BIND([this_ = MakeStrong(this), slotIndex, jobId, commands, rootFS, user] {
            for (const auto& command : commands) {
                YT_LOG_DEBUG("Running setup command (JobId: %v, Path: %v, Args: %v)",
                    jobId,
                    command->Path,
                    command->Args);
                auto instance = this_->CreateSetupInstance(slotIndex, jobId, rootFS, user);
                try {
                    auto process = CreateSetupProcess(instance, command);
                    WaitFor(process->Spawn())
                        .ThrowOnError();
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Setup command failed (JobId: %v, Stderr: %v)",
                        jobId,
                        instance->GetStderr());
                    throw;
                }
            }
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    IPortoExecutorPtr PortoExecutor_;

    IInstancePtr SelfInstance_;
    IInstancePtr MetaInstance_;
    THashMap<int, IInstancePtr> JobProxyInstances_;

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

        auto result = WaitFor(CombineAll(futures));
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

    virtual void DoInit(int slotCount, double cpuLimit) override
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
        SelfInstance_ = GetSelfPortoInstance(PortoExecutor_);

        auto getMetaContainer = [&] () -> IInstancePtr {
            auto metaInstanceName = Format("%v/%v", SelfInstance_->GetAbsoluteName(), GetDefaultJobsMetaContainerName());

            try {
                WaitFor(PortoExecutor_->DestroyContainer(metaInstanceName))
                    .ThrowOnError();
            } catch (const TErrorException& ex) {
                // If container doesn't exist it's ok.
                if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                    throw;
                }
            }

            auto instance = CreatePortoInstance(
                metaInstanceName,
                PortoExecutor_);
            instance->SetIOWeight(Config_->JobsIOWeight);
            instance->SetCpuLimit(CpuLimit_);
            return instance;
        };

        MetaInstance_ = getMetaContainer();
        DestroyAllSubcontainers(MetaInstance_->GetAbsoluteName());

        try {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                WaitFor(PortoExecutor_->CreateContainer(GetFullSlotMetaContainerName(
                    MetaInstance_->GetAbsoluteName(),
                    slotIndex)))
                    .ThrowOnError();

                // This forces creation of CPU cgroup for this container.
                WaitFor(PortoExecutor_->SetContainerProperty(
                    GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex),
                    "cpu_guarantee",
                    "0.05c"))
                    .ThrowOnError();

                WaitFor(PortoExecutor_->SetContainerProperty(
                    GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex),
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

    void InitJobProxyInstance(int slotIndex, TJobId jobId)
    {
        if (!JobProxyInstances_[slotIndex]) {
            JobProxyInstances_[slotIndex] = CreatePortoInstance(
                GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex) + "/jp_" + ToString(jobId),
                PortoExecutor_);
        }
    }

    virtual TProcessBasePtr CreateJobProxyProcess(int slotIndex, TJobId jobId) override
    {
        InitJobProxyInstance(slotIndex, jobId);
        return New<TPortoProcess>(JobProxyProgramName, JobProxyInstances_.at(slotIndex));
    }

    virtual void UpdateCpuLimit(double cpuLimit)
    {
        if (std::abs(CpuLimit_ - cpuLimit) < CpuUpdatePrecision) {
            return;
        }

        MetaInstance_->SetCpuLimit(cpuLimit);
        CpuLimit_ = cpuLimit;
    }

    IInstancePtr CreateSetupInstance(int slotIndex, TJobId jobId, const TRootFS& rootFS, const TString& user)
    {
        auto instance = CreatePortoInstance(
            GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex) + "/sc_" + ToString(jobId),
            PortoExecutor_);
        instance->SetRoot(rootFS);
        instance->SetUser(user);
        return instance;
    }

    static TProcessBasePtr CreateSetupProcess(const IInstancePtr& instance, const NJobAgent::TShellCommandConfigPtr& command)
    {
        auto process = New<TPortoProcess>(command->Path, instance);
        process->AddArguments(command->Args);
        return process;
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(INodePtr configNode, TBootstrap* bootstrap)
{
    auto config = ConvertTo<TJobEnvironmentConfigPtr>(configNode);
    switch (config->Type) {
        case EJobEnvironmentType::Simple: {
            auto simpleConfig = ConvertTo<TSimpleJobEnvironmentConfigPtr>(configNode);
            return New<TSimpleJobEnvironment>(
                simpleConfig,
                bootstrap);
        }

        case EJobEnvironmentType::Cgroups: {
            auto cgroupConfig = ConvertTo<TCGroupJobEnvironmentConfigPtr>(configNode);
            return New<TCGroupJobEnvironment>(
                cgroupConfig,
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

} // namespace NYT::NExecAgent
