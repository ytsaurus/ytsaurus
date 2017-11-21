#include "job_environment.h"
#include "config.h"
#include "mounter.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/program/names.h>

#include <yt/server/containers/container_manager.h>
#include <yt/server/containers/instance.h>

#include <yt/server/misc/process.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/tools/tools.h>

#include <yt/core/misc/process.h>
#include <yt/core/misc/proc.h>

#include <util/generic/guid.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NExecAgent {

using namespace NCGroup;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NContainers;
using namespace NYTree;
using namespace NTools;

static const auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TString GetSlotProcessGroup(int slotIndex)
{
    return "slots/" + ToString(slotIndex);
}

////////////////////////////////////////////////////////////////////////////////

class TProcessJobEnvironmentBase
    : public IJobEnvironment
{
public:
    TProcessJobEnvironmentBase(TJobEnvironmentConfigPtr config, const TBootstrap* bootstrap)
        : BasicConfig_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        const TJobId& jobId,
        const TOperationId& operationId) override
    {
        ValidateEnabled();

        try {
            auto process = CreateJobProxyProcess(slotIndex);

            process->AddArguments({
                "--config", ProxyConfigFileName,
                "--operation-id", ToString(operationId),
                "--job-id", ToString(jobId)
            });

            process->SetWorkingDirectory(workingDirectory);

            AddArguments(process, slotIndex);

            LOG_INFO("Spawning a job proxy (SlotIndex: %v, JobId: %v, OperationId: %v, WorkingDirectory: %v)",
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

    virtual IMounterPtr CreateMounter(int /*slotIndex*/) override
    {
        //Same mounter for all slots.
        if (!Mounter_) {
            Mounter_ = CreateSimpleMounter(ActionQueue_->GetInvoker());
        }
        return Mounter_;
    }

protected:
    struct TJobProxyProcess
    {
        TProcessBasePtr Process;
        TFuture<void> Result;
    };

    const TJobEnvironmentConfigPtr BasicConfig_;
    THashMap<int, TJobProxyProcess> JobProxyProcesses_;
    const TBootstrap* const Bootstrap_;
    TActionQueuePtr ActionQueue_ = New<TActionQueue>("JobEnvironment");

    TFuture<void> JobProxyResult_;

    bool Enabled_ = true;

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
                it->second.Process->Kill(SIGKILL);
            }

            // Ensure that job proxy process finised.
            auto error = WaitFor(it->second.Result);
            LOG_INFO(error, "Job proxy process finished (SlotIndex: %v)", slotIndex);
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

        LOG_ERROR(alert);

        auto masterConnector = Bootstrap_->GetMasterConnector();
        masterConnector->RegisterAlert(alert);
    }

    virtual void AddArguments(TProcessBasePtr process, int slotIndex)
    { }

private:
    IMounterPtr Mounter_;

    virtual TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/)
    {
        return New<TSimpleProcess>(JobProxyProgramName);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCGroupJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TCGroupJobEnvironment(TCGroupJobEnvironmentConfigPtr config, const TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    {
        if (!HasRootPermissions()) {
            auto error = TError("Failed to initialize \"cgroup\" job environment: root permissions required");
            Disable(error);
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

private:
    const TCGroupJobEnvironmentConfigPtr Config_;

    virtual void AddArguments(TProcessBasePtr process, int slotIndex) override
    {
        for (const auto& path : GetCGroupPaths(slotIndex)) {
            process->AddArguments({"--cgroup", path});
        }
    }

    std::vector<TString> GetCGroupPaths(int slotIndex) const
    {
        std::vector<TString> result;
        auto subgroupName = GetSlotProcessGroup(slotIndex);

        // Freezer is always implicitly supported.
        TNonOwningCGroup freezer("freezer", subgroupName);
        result.push_back(freezer.GetFullPath());

        for (const auto& type : Config_->SupportedCGroups) {
            TNonOwningCGroup group(type, subgroupName);
            result.push_back(group.GetFullPath());
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TSimpleJobEnvironment(TSimpleJobEnvironmentConfigPtr config, const TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    {
        if (!HasRootPermissions_ && Config_->EnforceJobControl) {
            auto error = TError("Failed to initialize \"simple\" job environment: "
                "\"enforce_job_control\" option set, but no root permissions provided");
            Disable(error);
        }
    }

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

private:
    const TSimpleJobEnvironmentConfigPtr Config_;
    const bool HasRootPermissions_ = HasRootPermissions();
};

////////////////////////////////////////////////////////////////////////////////

class TPortoJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TPortoJobEnvironment(TPortoJobEnvironmentConfigPtr config, const TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
    {
        auto portoFatalErrorHandler = BIND([weakThis_ = MakeWeak(this)] (const TError& error) {
            // We use weak ptr to avoid cyclic references between container manager and job environment.
            auto this_ = weakThis_.Lock();
            if (this_) {
                this_->Disable(error);
            }
        });

        try {
            ContainerManager_ = CreatePortoManager(
                "yt_job-proxy_",
                portoFatalErrorHandler,
                { ECleanMode::All, Config_->PortoWaitTime, Config_->PortoPollPeriod });
        } catch (const std::exception& ex) {
            auto error = TError("Failed to initialize \"porto\" job environment")
                << ex;
            Disable(error);
        }
    }

    virtual void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            auto jobProxyProcess = JobProxyProcesses_[slotIndex].Process;
            if (jobProxyProcess) {
                jobProxyProcess->Kill(SIGKILL);

                // No need to kill - we're killing container with all subcontainers.
                EnsureJobProxyFinished(slotIndex, false);

                // Drop reference to a process if there were any.
                JobProxyProcesses_.erase(slotIndex);
            }
            PortoInstances_.erase(slotIndex);
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

    virtual IMounterPtr CreateMounter(int slotIndex) override
    {
        auto instanceProvider = BIND([=, this_ = MakeStrong(this)]() {
            InitPortoInstance(slotIndex);
            return PortoInstances_.at(slotIndex);
        });

        return CreatePortoMounter(instanceProvider);
    }

private:
    void InitPortoInstance(int slotIndex)
    {
        if (!PortoInstances_[slotIndex]) {
            PortoInstances_[slotIndex] = ContainerManager_->CreateInstance();
        }
    }

    virtual TProcessBasePtr CreateJobProxyProcess(int slotIndex) override
    {
        InitPortoInstance(slotIndex);
        return New<TPortoProcess>(JobProxyProgramName, PortoInstances_.at(slotIndex));
    }

    const TPortoJobEnvironmentConfigPtr Config_;

    IContainerManagerPtr ContainerManager_;
    THashMap<int, IInstancePtr> PortoInstances_;
};

////////////////////////////////////////////////////////////////////////////////

IJobEnvironmentPtr CreateJobEnvironment(INodePtr configNode, const TBootstrap* bootstrap)
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
            auto portoConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(configNode);
            return New<TPortoJobEnvironment>(
                portoConfig,
                bootstrap);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
