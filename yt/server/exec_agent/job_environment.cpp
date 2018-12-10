#include "job_environment.h"
#include "config.h"
#include "job_directory_manager.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/controller_agent/job_memory.h>

#include <yt/server/data_node/config.h>
#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/volume_manager.h>

#include <yt/server/misc/public.h>

#ifdef _linux_
#include <yt/server/containers/porto_executor.h>
#include <yt/server/containers/instance.h>

#include <yt/server/misc/process.h>
#endif

#include <yt/ytlib/job_proxy/private.h>

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
using namespace NJobProxy;
#ifdef _linux_
using namespace NContainers;
#endif
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

    virtual void Init(int slotCount, double jobsCpuLimit) override
    {
        // Shutdown all possible processes.
        try {
            DoInit(slotCount, jobsCpuLimit);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean up processes during initialization")
                << ex;
            Disable(error);
        }
    }

    virtual TFuture<void> RunJobProxy(
        int slotIndex,
        const TString& workingDirectory,
        const TJobId& jobId,
        const TOperationId& operationId) override
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

    virtual TFuture<IVolumePtr> PrepareRootVolume(const std::vector<TArtifactKey>& /*layers*/) override
    {
        THROW_ERROR_EXCEPTION("Custom rootfs is not supported by %Qlv environment",
            BasicConfig_->Type);
    }

    virtual std::optional<i64> GetMemoryLimit() const override
    {
        return std::nullopt;
    }

    virtual std::optional<double> GetCpuLimit() const override
    {
        return std::nullopt;
    }

    virtual bool ExternalJobMemory() const override
    {
        return false;
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

    virtual void DoInit(int slotCount, double /* jobsCpuLimit */)
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
                it->second.Process->Kill(SIGKILL);
            }

            // Ensure that job proxy process finished.
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
    virtual TProcessBasePtr CreateJobProxyProcess(int /*slotIndex*/, const TJobId& /* jobId */)
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

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path)
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

    virtual void DoInit(int slotCount, double jobsCpuLimit) override
    {
        if (!HasRootPermissions()) {
            THROW_ERROR_EXCEPTION("Failed to initialize \"cgroup\" job environment: root permissions required");
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, jobsCpuLimit);
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

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path)
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

    virtual void DoInit(int slotCount, double jobsCpuLimit) override
    {
        if (!HasRootPermissions_ && Config_->EnforceJobControl) {
            THROW_ERROR_EXCEPTION("Failed to initialize \"simple\" job environment: "
                "\"enforce_job_control\" option set, but no root permissions provided");
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, jobsCpuLimit);
    }
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoJobEnvironment
    : public TProcessJobEnvironmentBase
{
public:
    TPortoJobEnvironment(TPortoJobEnvironmentConfigPtr config, TBootstrap* bootstrap)
        : TProcessJobEnvironmentBase(config, bootstrap)
        , Config_(std::move(config))
        , PortoExecutor_(CreatePortoExecutor(Config_->PortoWaitTime, Config_->PortoPollPeriod))
    {  }

    virtual void CleanProcesses(int slotIndex) override
    {
        ValidateEnabled();

        try {
            EnsureJobProxyFinished(slotIndex, true);

            CleanAllSubcontainers(GetFullSlotMetaContainerName(
                MetaInstance_->GetAbsoluteName(),
                slotIndex));

            // Reset cpu guarantee.
            WaitFor(PortoExecutor_->SetProperty(
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

    virtual IJobDirectoryManagerPtr CreateJobDirectoryManager(const TString& path)
    {
#ifdef __linux__
        return CreatePortoJobDirectoryManager(Bootstrap_->GetConfig()->DataNode->VolumeManager, path);
#else
        return nullptr;
#endif
    }

    virtual TFuture<IVolumePtr> PrepareRootVolume(const std::vector<TArtifactKey>& layers) override
    {
#ifdef __linux__
        return RootVolumeManager_->PrepareVolume(layers);
#else
        return VoidFuture;
#endif
    }

    virtual std::optional<i64> GetMemoryLimit() const override
    {
        auto guard = Guard(LimitsLock_);
        return MemoryLimit_;
    }

    virtual std::optional<double> GetCpuLimit() const override
    {
        auto guard = Guard(LimitsLock_);
        return CpuLimit_;
    }

    virtual bool ExternalJobMemory() const override
    {
        return Config_->ExternalJobContainer.operator bool();
    }

private:
    const TPortoJobEnvironmentConfigPtr Config_;
    IPortoExecutorPtr PortoExecutor_;

    IInstancePtr MetaInstance_;
    THashMap<int, IInstancePtr> JobProxyInstances_;

    TSpinLock LimitsLock_;
    std::optional<double> CpuLimit_;
    std::optional<i64> MemoryLimit_;

    TPeriodicExecutorPtr LimitsUpdateExecutor_;
    IVolumeManagerPtr RootVolumeManager_;

    TString GetAbsoluteName(const TString& name)
    {
        auto properties = WaitFor(PortoExecutor_->GetProperties(
            name,
            std::vector<TString>{"absolute_name"}))
            .ValueOrThrow();

        return properties.at("absolute_name")
             .ValueOrThrow();
    }

    void CleanAllSubcontainers(const TString& metaName)
    {
        const auto containers = WaitFor(PortoExecutor_->ListContainers())
            .ValueOrThrow();

        LOG_DEBUG("Destroying all subcontainers (MetaName: %v)", metaName);

        std::vector<TFuture<void>> actions;
        for (const auto& name : containers) {
            if (name == "/") {
                continue;
            }

            try {
                auto absoluteName = GetAbsoluteName(name);
                if (!absoluteName.StartsWith(metaName + "/")) {
                    continue;
                }

                LOG_DEBUG("Cleaning (Container: %v)", absoluteName);
                actions.push_back(PortoExecutor_->DestroyContainer(name));
            } catch (const TErrorException& ex) {
                // If container disappeared, we don't care.
                if (ex.Error().FindMatching(EContainerErrorCode::ContainerDoesNotExist)) {
                    LOG_DEBUG(ex, "Failed to clean container; it vanished");
                } else {
                    throw;
                }
            }
        }

        auto errors = WaitFor(CombineAll(actions));
        THROW_ERROR_EXCEPTION_IF_FAILED(errors, "Failed to clean containers");

        for (const auto& error : errors.Value()) {
            if (error.IsOK() ||
                error.FindMatching(EContainerErrorCode::ContainerDoesNotExist))
            {
                continue;
            }

            THROW_ERROR_EXCEPTION("Failed to clean containers")
                    << error;
        }
    }

    virtual void DoInit(int slotCount, double jobsCpuLimit) override
    {
        auto portoFatalErrorHandler = BIND([weakThis_ = MakeWeak(this)](const TError& error) {
            // We use weak ptr to avoid cyclic references between container manager and job environment.
            auto this_ = weakThis_.Lock();
            if (this_) {
                this_->Disable(error);
            }
        });

        PortoExecutor_->SubscribeFailed(portoFatalErrorHandler);

        auto getMetaContainer = [&] () -> IInstancePtr {
            if (Config_->ExternalJobContainer) {
                return GetPortoInstance(PortoExecutor_, *Config_->ExternalJobContainer);
            }   else {
                auto self = GetSelfPortoInstance(PortoExecutor_);
                auto metaInstanceName = Format("%v/%v", self->GetAbsoluteName(), GetDefaultJobsMetaContainerName());

                try {
                    WaitFor(PortoExecutor_->DestroyContainer(metaInstanceName))
                        .ThrowOnError();
                } catch (const TErrorException& ex) {
                    // If container doesn't exist it's ok.
                    if (!ex.Error().FindMatching(EContainerErrorCode::ContainerDoesNotExist)) {
                        throw;
                    }
                }

                auto instance = CreatePortoInstance(
                    metaInstanceName,
                    PortoExecutor_);
                instance->SetIOWeight(Config_->JobsIOWeight);
                instance->SetCpuLimit(jobsCpuLimit);
                return instance;
            }
        };

        MetaInstance_ = getMetaContainer();
        CleanAllSubcontainers(MetaInstance_->GetAbsoluteName());

        try {
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                WaitFor(PortoExecutor_->CreateContainer(GetFullSlotMetaContainerName(
                    MetaInstance_->GetAbsoluteName(),
                    slotIndex)))
                    .ThrowOnError();

                // This forces creation of cpu cgroup for this container.
                WaitFor(PortoExecutor_->SetProperty(
                    GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex),
                    "cpu_guarantee",
                    "0.05c"))
                    .ThrowOnError();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create meta containers for jobs")
                << ex;
        }

        TProcessJobEnvironmentBase::DoInit(slotCount, jobsCpuLimit);

#ifdef __linux__
        // To these moment all old processed must have been killed, so we can safely clean up old volumes
        // during root volume manager initialization.
        RootVolumeManager_ = CreatePortoVolumeManager(
            Bootstrap_->GetConfig()->DataNode->VolumeManager,
            Bootstrap_);

        if (Config_->ResourceLimitsUpdatePeriod) {
            LimitsUpdateExecutor_ = New<TPeriodicExecutor>(
                ActionQueue_->GetInvoker(),
                BIND(&TPortoJobEnvironment::UpdateLimits, MakeWeak(this)),
                *Config_->ResourceLimitsUpdatePeriod);
            LimitsUpdateExecutor_->Start();
        }
#endif
    }

    void InitJobProxyInstance(int slotIndex, const TJobId& jobId)
    {
        if (!JobProxyInstances_[slotIndex]) {
            JobProxyInstances_[slotIndex] = CreatePortoInstance(
                GetFullSlotMetaContainerName(MetaInstance_->GetAbsoluteName(), slotIndex) + "/job_proxy_" + ToString(jobId),
                PortoExecutor_);

            if (Config_->ExternalJobRootVolume) {
                TRootFS rootFS;
                rootFS.RootPath = *Config_->ExternalJobRootVolume;
                rootFS.IsRootReadOnly = false;

                for (const auto& pair : Config_->ExternalBinds) {
                    rootFS.Binds.push_back(TBind{pair.first, pair.second, false});
                }

                JobProxyInstances_[slotIndex]->SetRoot(rootFS);
            }
        }
    }

    virtual TProcessBasePtr CreateJobProxyProcess(int slotIndex, const TJobId& jobId) override
    {
        InitJobProxyInstance(slotIndex, jobId);
        return New<TPortoProcess>(JobProxyProgramName, JobProxyInstances_.at(slotIndex));
    }

    void UpdateLimits()
    {
        try {
            auto container = Config_->ExternalJobContainer
                ? MetaInstance_
                : GetSelfPortoInstance(PortoExecutor_);

            auto limits = container->GetResourceLimitsRecursive();

            auto guard = Guard(LimitsLock_);
            auto newCpuLimit = std::max<double>(limits.Cpu - Config_->NodeDedicatedCpu, 0);
            if (!CpuLimit_ || *CpuLimit_ != limits.Cpu) {
                LOG_INFO("Update porto cpu limit (OldCpuLimit: %v, NewCpuLimit: %v)", CpuLimit_, newCpuLimit);
                CpuLimit_ = newCpuLimit;
            }

            if (!MemoryLimit_ || *MemoryLimit_ != limits.Memory) {
                LOG_INFO("Update porto memory limit (OldMemoryLimit: %v, NewMemoryLimit: %v)", MemoryLimit_, limits.Memory);
                MemoryLimit_ = limits.Memory;
            }

        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Failed to update resource limits from porto");
        }
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
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
