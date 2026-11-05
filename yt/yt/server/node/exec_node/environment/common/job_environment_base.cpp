#include "job_environment_base.h"

#include "host_slot.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

using namespace NJobProxy;
using namespace NContainers;

using NNet::TIP6Address;

using NServer::JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TProcessJobEnvironmentBase::TProcessJobEnvironmentBase(
    TJobEnvironmentConfig config,
    TNonNullPtr<IBootstrap> bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
{ }

IUserSlotPtr TProcessJobEnvironmentBase::CreateUserSlot(TUserSlotContext context)
{
    return CreateHostUserSlot(std::move(context));
}

TJobFSSecretaryPtr TProcessJobEnvironmentBase::CreateJobFSSecretary(NLogging::TLogger logger)
{
    return New<TJobFSSecretary>(&*Bootstrap_, std::move(logger));
}

TError TProcessJobEnvironmentBase::Init(int slotCount, double cpuLimit, double idleCpuFraction)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Bootstrap_->SubscribePopulateAlerts(
        BIND(&TProcessJobEnvironmentBase::PopulateAlerts, MakeWeak(this)));

    for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
        auto state = New<TProcessCreationState>();
        state->CreationDisabled = true;
        EmplaceOrCrash(ProcessCreationStates_, slotIndex, std::move(state));
    }

    // Shutdown all possible processes.
    try {
        DoInit(slotCount, cpuLimit, idleCpuFraction);
    } catch (const std::exception& ex) {
        auto error = TError("Failed to clean up processes during initialization")
            .With(ex);
        Disable(error);
        return error;
    }

    return TError();
}

TFuture<void> TProcessJobEnvironmentBase::InitSlot(int slotIndex)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return BIND(&IJobEnvironment::CleanProcesses, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run(slotIndex, ESlotType::Common);
}

void TProcessJobEnvironmentBase::PrepareSlotForJob(int slotIndex)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto& state = GetOrCrash(ProcessCreationStates_, slotIndex);
    YT_VERIFY(state->CreationDisabled);
    state = New<TProcessCreationState>();
}

TFuture<void> TProcessJobEnvironmentBase::RunJobProxy(
    const TJobProxyInternalConfigPtr& config,
    ESlotType slotType,
    int slotIndex,
    const std::string& workingDirectory,
    TJobId jobId,
    TOperationId operationId,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    ValidateEnabled();

    std::vector<std::string> environmentVariables;
    environmentVariables.reserve(config->EnvironmentVariables.size());
    for (const auto& variable: config->EnvironmentVariables) {
        try {
            environmentVariables.push_back(Format("%v=%v", variable->Name, variable->LoadValue()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot load environment variable %Qv", variable->Name)
                .With(ex);
        }
    }

    try {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;
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
                "--stderr-path", *stderrPath,
            });
        }

        process->SetWorkingDirectory(workingDirectory);

        AddArguments(process, slotIndex);

        for (const auto& variable: environmentVariables) {
            process->AddEnvVar(variable);
        }

        YT_TLOG_INFO("Spawn job proxy")
            .With("SlotType", slotType)
            .With("SlotIndex", slotIndex)
            .With("JobId", jobId)
            .With("OperationId", operationId)
            .With("WorkingDirectory", workingDirectory)
            .With("StderrPath", stderrPath);

        TJobProxyProcess jobProxyProcess;
        jobProxyProcess.Process = process;
        auto creationState = GetOrCrash(ProcessCreationStates_, slotIndex);
        jobProxyProcess.Result = BIND([=] {
                auto guard = WaitFor(AcquireProcessCreationGuard(creationState))
                    .ValueOrThrow();

                // Make forks outside controller thread.
                return process->Spawn();
            })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();

        JobProxyProcesses_[slotIndex] = jobProxyProcess;
        return jobProxyProcess.Result;
    } catch (const std::exception& ex) {
        auto error = TError("Failed to spawn job proxy").With(ex);
        Disable(error);

        THROW_ERROR error;
    }
}

bool TProcessJobEnvironmentBase::IsEnabled() const
{
    return Enabled_.load();
}

void TProcessJobEnvironmentBase::UpdateCpuLimit(double /*cpuLimit*/)
{ }

double TProcessJobEnvironmentBase::GetCpuLimit(ESlotType /*slotType*/) const
{
    return 0;
}

i64 TProcessJobEnvironmentBase::GetMajorPageFaultCount() const
{
    return 0;
}

void TProcessJobEnvironmentBase::EnrichJobEnvironmentConfig(int /*slotIndex*/, TNonNullPtr<NJobProxy::TJobProxyInternalConfig> /*jobProxyConfig*/) const
{ }

TFuture<std::vector<TShellCommandResult>> TProcessJobEnvironmentBase::RunCommands(
    int /*slotIndex*/,
    ESlotType /*slotType*/,
    TJobId /*jobId*/,
    const std::vector<TShellCommandConfigPtr>& /*commands*/,
    const TRootFS& /*rootFS*/,
    const std::string& /*user*/,
    const std::optional<std::vector<TDevice>>& /*devices*/,
    const std::optional<std::string>& /*hostName*/,
    const std::vector<TIP6Address>& /*ipAddresses*/,
    std::string /*tag*/,
    bool /*throwOnFailedCommand*/)
{
    THROW_ERROR_EXCEPTION(
        "Running custom commands is not yet supported by %Qlv environment",
        Config_.GetType());
}

void TProcessJobEnvironmentBase::OnDynamicConfigChanged(
    const TSlotManagerDynamicConfigPtr& /*oldConfig*/,
    const TSlotManagerDynamicConfigPtr& /*newConfig*/)
{ }

void TProcessJobEnvironmentBase::Disable(TError error)
{
    if (!Enabled_.exchange(false)) {
        return;
    }

    static constexpr auto Message = "Job environment is disabled"_sb;
    YT_TLOG_ERROR(Message)
        .With(error);

    auto alert = TError(NExecNode::EErrorCode::JobEnvironmentDisabled, Message)
        .With(error);

    Alert_.Store(alert);

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    auto dynamicConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;

    YT_TLOG_FATAL_IF(dynamicConfig->AbortOnJobsDisabled, Message)
        .With(error);

    if (dynamicConfig->EnableJobEnvironmentResurrection) {
        YT_UNUSED_FUTURE(BIND(&TSlotManager::OnJobEnvironmentDisabled, Bootstrap_->GetSlotManager())
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run(std::move(alert)));
    }
}

void TProcessJobEnvironmentBase::DoInit(int /*slotCount*/, double /*cpuLimit*/, double /*idleCpuFraction*/)
{ }

void TProcessJobEnvironmentBase::ValidateEnabled() const
{
    if (!Enabled_.load()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::JobEnvironmentDisabled,
            "Job environment %Qlv is disabled",
            Config_.GetType());
    }
}

void TProcessJobEnvironmentBase::EnsureJobProxyFinished(int slotIndex, bool kill)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    {
        auto state = GetOrCrash(ProcessCreationStates_, slotIndex);
        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&state->Lock))
            .ValueOrThrow();
        state->CreationDisabled = true;
    }

    auto it = JobProxyProcesses_.find(slotIndex);
    if (it != JobProxyProcesses_.end()) {
        if (kill) {
            try {
                it->second.Process->Kill(SIGKILL);
            } catch (const std::exception& ex) {
                // If we failed to kill container we ignore it for now.
                YT_TLOG_WARNING("Failed to kill container properly")
                    .With("SlotIndex", slotIndex)
                    .With(ex);
            }
        }

        // Ensure that job proxy process finished.
        auto error = WaitFor(it->second.Result);
        YT_TLOG_INFO("Job proxy process finished")
            .With("SlotIndex", slotIndex)
            .With(error);
        // Drop reference to a process.
        JobProxyProcesses_.erase(it);
    }
}

TFuture<TIntrusivePtr<TAsyncLockReaderGuard>> TProcessJobEnvironmentBase::AcquireProcessCreationGuard(
    const TProcessCreationStatePtr& state)
{
    return TAsyncLockReaderGuard::Acquire(&state->Lock)
        .Apply(BIND([state] (const TIntrusivePtr<TAsyncLockReaderGuard>& guard) {
            THROW_ERROR_EXCEPTION_IF(state->CreationDisabled, "Job preparation canceled");
            return guard;
        }));
}

void TProcessJobEnvironmentBase::AddArguments(TProcessBasePtr /*process*/, int /*slotIndex*/)
{ }

void TProcessJobEnvironmentBase::PopulateAlerts(TNonNullPtr<std::vector<TError>> alerts)
{
    auto alert = Alert_.Load();
    if (!alert.IsOK()) {
        alerts->push_back(std::move(alert));
    }
}

TProcessBasePtr TProcessJobEnvironmentBase::CreateJobProxyProcess(
    const TJobProxyInternalConfigPtr& /*config*/,
    int /*slotIndex*/,
    ESlotType /*slotType*/,
    TJobId /*jobId*/)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return New<TSimpleProcess>(JobProxyProgramName);
}

void TProcessJobEnvironmentBase::UpdateSlotCpuSet(int /*slotIndex*/, ESlotType /*slotType*/, TStringBuf /*cpuSet*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
