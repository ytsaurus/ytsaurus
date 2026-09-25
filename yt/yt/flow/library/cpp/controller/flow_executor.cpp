#include "flow_executor.h"
#include "flow_executor_runtime.h"

#include "private.h"

#include "controller.h"
#include "persisted_state_manager.h"
#include "state_access.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/controller/describe/describe_computation.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_computations.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_partition.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_worker.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_workers.h>

#include <yt/yt/flow/library/cpp/misc/deploy_url_provider.h>

#include <util/random/random.h>
#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/key_states.h>
#include <yt/yt/flow/library/cpp/tables/partition_states.h>
#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/library/orchid/orchid_ypath_service.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/misc/codicil.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/common/admin_service_proxy.h>

#include <yt/yt/client/tablet_client/public.h>

#include <algorithm>
#include <vector>

namespace NYT::NFlow::NController {

using namespace NConcurrency;
using namespace NLogging;
using namespace NOrchid;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IFlowExecutor);
DEFINE_REFCOUNTED_TYPE(IFlowExecutorRuntime);

////////////////////////////////////////////////////////////////////////////////


namespace {

////////////////////////////////////////////////////////////////////////////////

struct TGetCommandRequiredPermissionArg
    : public TYsonStructLite
{
    std::string Command;

    REGISTER_YSON_STRUCT_LITE(TGetCommandRequiredPermissionArg);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("command", &TThis::Command);
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

struct TEmptyArg
    : public TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TEmptyArg);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

struct TDescribePipelineArg
    : public TYsonStructLite
{
    bool StatusOnly = false;

    REGISTER_YSON_STRUCT_LITE(TDescribePipelineArg);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
        registrar.Parameter("status_only", &TThis::StatusOnly)
            .Default(NDescribe::TDescribePipelineArguments().StatusOnly);
    }
};

struct TDescribeComputationArg
    : public TYsonStructLite
{
    TComputationId ComputationId;

    REGISTER_YSON_STRUCT_LITE(TDescribeComputationArg);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
        registrar.Parameter("computation_id", &TThis::ComputationId);
    }
};

struct TDescribePartitionArg
    : public TYsonStructLite
{
    TPartitionId PartitionId;
    TDuration OrchidTimeout;

    REGISTER_YSON_STRUCT_LITE(TDescribePartitionArg);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
        registrar.Parameter("partition_id", &TThis::PartitionId);
        registrar.Parameter("orchid_timeout", &TThis::OrchidTimeout)
            .Default(TDuration::Seconds(5));
    }
};

struct TDescribeWorkerArg
    : public TYsonStructLite
{
    std::string Worker;

    REGISTER_YSON_STRUCT_LITE(TDescribeWorkerArg);

    static void Register(TRegistrar registrar)
    {
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
        registrar.Parameter("worker", &TThis::Worker)
            .Default();
    }
};

class TFlowExecutor
    : public IFlowExecutor
{
public:
    explicit TFlowExecutor(IFlowExecutorRuntimePtr runtime);

    TYsonString Execute(
        const std::string& command,
        const TYsonString& argument,
        const std::string& user) override;

    void AuthorizeCommand(
        const std::string& command,
        const std::string& user) override;

    TGetFlowViewResult GetFlowView(const TGetFlowViewArg& argument) override;
    TGetPipelineDynamicSpecResult GetPipelineDynamicSpec(const TGetPipelineDynamicSpecArg& argument) override;
    TSetPipelineDynamicSpecResult SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument) override;
    TGetPipelineSpecResult GetPipelineSpec(const TGetPipelineSpecArg& argument) override;
    TSetPipelineSpecResult SetPipelineSpec(const TSetPipelineSpecArg& argument) override;
    TSetPipelineSpecsResult SetPipelineSpecs(const TSetPipelineSpecsArg& argument) override;
    TGetPipelineStateResult GetPipelineState(const TGetPipelineStateArg& argument) override;
    TSetTargetPipelineStateResult SetTargetPipelineState(const TSetTargetPipelineStateArg& argument) override;
    TGetFlowCoreTargetResult GetFlowCoreTarget(const TGetFlowCoreTargetArg& argument) override;
    TSetFlowCoreTargetResult SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument) override;

private:
    using TExecuteHandler = std::function<TYsonString(const TYsonString& argument)>;

    struct TCommandDescriptor
    {
        TExecuteHandler Handler;
        EPermission RequiredPermission = EPermission::Write;
    };

    const IFlowExecutorRuntimePtr Runtime_;
    THashMap<std::string, TCommandDescriptor> CommandDescriptors_;
    std::vector<std::string> Commands_;

    void RegisterCommand(
        std::string command,
        EPermission requiredPermission,
        TExecuteHandler handler);

    EPermission GetCommandRequiredPermission(const std::string& command) const;
    [[noreturn]] void ThrowCommandNotFound(const std::string& command) const;
};

TFlowExecutor::TFlowExecutor(IFlowExecutorRuntimePtr runtime)
    : Runtime_(std::move(runtime))
{
    YT_VERIFY(Runtime_);

    RegisterCommand("list", EPermission::Read, [this] (const auto&) {
        return ConvertToYsonString(Commands_);
    });
    RegisterCommand("get-command-required-permission", EPermission::Read, [this] (const auto& argument) {
        auto parsedArgument = ConvertTo<TGetCommandRequiredPermissionArg>(argument);
        return BuildYsonStringFluently()
            .BeginMap()
            .Item("permission")
            .Value(GetCommandRequiredPermission(parsedArgument.Command))
            .EndMap();
    });
    RegisterCommand("get-flow-view", EPermission::Read, [this] (const auto& argument) {
        return GetFlowView(ConvertTo<TGetFlowViewArg>(argument));
    });
    RegisterCommand("get-flow-view-v2", EPermission::Read, [this] (const auto& argument) {
        return ConvertToYsonString(Runtime_->GetFlowViewV2(ConvertTo<TGetFlowViewV2Arg>(argument)));
    });
    RegisterCommand("get-pipeline-spec", EPermission::Read, [this] (const auto& argument) {
        return ConvertToYsonString(GetPipelineSpec(ConvertTo<TGetPipelineSpecArg>(argument)));
    });
    RegisterCommand("set-pipeline-spec", EPermission::Write, [this] (const auto& argument) {
        return ConvertToYsonString(SetPipelineSpec(ConvertTo<TSetPipelineSpecArg>(argument)));
    });
    RegisterCommand("get-pipeline-dynamic-spec", EPermission::Read, [this] (const auto& argument) {
        return ConvertToYsonString(GetPipelineDynamicSpec(ConvertTo<TGetPipelineDynamicSpecArg>(argument)));
    });
    RegisterCommand("set-pipeline-dynamic-spec", EPermission::Write, [this] (const auto& argument) {
        return ConvertToYsonString(SetPipelineDynamicSpec(ConvertTo<TSetPipelineDynamicSpecArg>(argument)));
    });
    RegisterCommand("set-pipeline-specs", EPermission::Write, [this] (const auto& argument) {
        return ConvertToYsonString(SetPipelineSpecs(ConvertTo<TSetPipelineSpecsArg>(argument)));
    });
    RegisterCommand("get-pipeline-state", EPermission::Read, [this] (const auto& argument) {
        return ConvertToYsonString(GetPipelineState(ConvertTo<TGetPipelineStateArg>(argument)));
    });
    RegisterCommand("set-target-pipeline-state", EPermission::Write, [this] (const auto& argument) {
        return ConvertToYsonString(SetTargetPipelineState(ConvertTo<TSetTargetPipelineStateArg>(argument)));
    });
    RegisterCommand("describe-pipeline", EPermission::Read, [this] (const auto& argument) {
        auto parsedArgument = ConvertTo<TDescribePipelineArg>(argument);
        return ConvertToYsonString(NDescribe::DescribePipeline(
            Runtime_->MakeDescribePipelineArguments(parsedArgument.StatusOnly)));
    });
    RegisterCommand("describe-computation", EPermission::Read, [this] (const auto& argument) {
        auto parsedArgument = ConvertTo<TDescribeComputationArg>(argument);
        return ConvertToYsonString(NDescribe::DescribeComputation(
            Runtime_->GetDescribeFlowView(),
            parsedArgument.ComputationId,
            Runtime_->GetDescribeControllerErrors()));
    });
    RegisterCommand("describe-computations", EPermission::Read, [this] (const auto& argument) {
        ConvertTo<TEmptyArg>(argument);
        return ConvertToYsonString(NDescribe::DescribeComputations(
            Runtime_->GetDescribeFlowView(),
            Runtime_->GetDescribeControllerErrors()));
    });
    RegisterCommand("describe-partition", EPermission::Read, [this] (const auto& argument) {
        auto parsedArgument = ConvertTo<TDescribePartitionArg>(argument);
        auto jobOrchid = Runtime_->GetDescribeJobOrchid(
            parsedArgument.PartitionId,
            parsedArgument.OrchidTimeout);
        return ConvertToYsonString(NDescribe::DescribePartition(
            Runtime_->GetDescribeFlowView(),
            parsedArgument.PartitionId,
            jobOrchid));
    });
    RegisterCommand("describe-worker", EPermission::Read, [this] (const auto& argument) {
        auto parsedArgument = ConvertTo<TDescribeWorkerArg>(argument);
        return ConvertToYsonString(NDescribe::DescribeWorker(
            Runtime_->GetDescribeFlowView(),
            parsedArgument.Worker,
            Runtime_->GetDescribeDeployStageUrl()));
    });
    RegisterCommand("describe-workers", EPermission::Read, [this] (const auto& argument) {
        ConvertTo<TEmptyArg>(argument);
        return ConvertToYsonString(NDescribe::DescribeWorkers(Runtime_->GetDescribeFlowView()));
    });
    RegisterCommand("get-worker-orchid", EPermission::Read, [this] (const auto& argument) {
        return Runtime_->GetWorkerOrchid(argument);
    });
    RegisterCommand("get-controller-orchid", EPermission::Read, [this] (const auto& argument) {
        return Runtime_->GetControllerOrchid(argument);
    });
    RegisterCommand("kill-worker", EPermission::Write, [this] (const auto& argument) {
        return Runtime_->KillWorker(argument);
    });
    RegisterCommand("update-worker", EPermission::Write, [this] (const auto& argument) {
        return Runtime_->UpdateWorker(argument);
    });
    RegisterCommand("get-worker-backtraces", EPermission::Read, [this] (const auto& argument) {
        return Runtime_->GetWorkerBacktraces(argument);
    });
    RegisterCommand("read-states", EPermission::Read, [this] (const auto& argument) {
        return Runtime_->ReadStates(argument);
    });
    RegisterCommand("delete-states", EPermission::Write, [this] (const auto& argument) {
        return Runtime_->DeleteStates(argument);
    });
    RegisterCommand("get-flow-core-target", EPermission::Read, [this] (const auto& argument) {
        return ConvertToYsonString(GetFlowCoreTarget(ConvertTo<TGetFlowCoreTargetArg>(argument)));
    });
    RegisterCommand("set-flow-core-target", EPermission::Write, [this] (const auto& argument) {
        return ConvertToYsonString(SetFlowCoreTarget(ConvertTo<TSetFlowCoreTargetArg>(argument)));
    });
}

TYsonString TFlowExecutor::Execute(
    const std::string& command,
    const TYsonString& argument,
    const std::string& user)
{
    return Runtime_->RunCommand(command, argument, user, [&] {
        if (auto it = CommandDescriptors_.find(command); it != CommandDescriptors_.end()) {
            return it->second.Handler(argument);
        }
        ThrowCommandNotFound(command);
    });
}

void TFlowExecutor::AuthorizeCommand(
    const std::string& command,
    const std::string& user)
{
    auto descriptor = GetOrDefault(CommandDescriptors_, command);
    if (!descriptor.Handler) {
        ThrowCommandNotFound(command);
    }
    Runtime_->AuthorizeCommand(descriptor.RequiredPermission, user);
}

TGetFlowViewResult TFlowExecutor::GetFlowView(const TGetFlowViewArg& argument)
{
    return Runtime_->GetFlowView(argument);
}

TGetPipelineDynamicSpecResult TFlowExecutor::GetPipelineDynamicSpec(
    const TGetPipelineDynamicSpecArg& argument)
{
    auto versionedSpec = Runtime_->GetPipelineDynamicSpec();
    auto specNode = ConvertToNode(versionedSpec->GetValue());
    TGetPipelineDynamicSpecResult result;
    result.Spec = GetNodeByYPath(specNode, TYPath(argument.Path));
    result.Version = versionedSpec->GetVersion();
    return result;
}

TSetPipelineDynamicSpecResult TFlowExecutor::SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument)
{
    return Runtime_->SetPipelineDynamicSpec(argument);
}

TGetPipelineSpecResult TFlowExecutor::GetPipelineSpec(const TGetPipelineSpecArg& argument)
{
    auto versionedSpec = Runtime_->GetPipelineSpec();
    auto specNode = ConvertToNode(versionedSpec->GetValue());
    TGetPipelineSpecResult result;
    result.Spec = GetNodeByYPath(specNode, TYPath(argument.Path));
    result.Version = versionedSpec->GetVersion();
    return result;
}

TSetPipelineSpecResult TFlowExecutor::SetPipelineSpec(const TSetPipelineSpecArg& argument)
{
    return Runtime_->SetPipelineSpec(argument);
}

TSetPipelineSpecsResult TFlowExecutor::SetPipelineSpecs(const TSetPipelineSpecsArg& argument)
{
    return Runtime_->SetPipelineSpecs(argument);
}

TGetPipelineStateResult TFlowExecutor::GetPipelineState(const TGetPipelineStateArg& /*argument*/)
{
    auto flowView = Runtime_->GetDescribeFlowView();
    TGetPipelineStateResult result;
    result.PipelineState = flowView->IsSynced()
        ? flowView->State->ExecutionSpec->PipelineState->GetValue()
        : EPipelineState::Unknown;
    return result;
}

TSetTargetPipelineStateResult TFlowExecutor::SetTargetPipelineState(const TSetTargetPipelineStateArg& argument)
{
    return Runtime_->SetTargetPipelineState(argument);
}

TGetFlowCoreTargetResult TFlowExecutor::GetFlowCoreTarget(const TGetFlowCoreTargetArg& /*argument*/)
{
    auto versionedTarget = Runtime_->GetFlowCoreTarget();
    TGetFlowCoreTargetResult result;
    result.FlowCoreTarget = versionedTarget->GetValue();
    result.Version = versionedTarget->GetVersion();
    return result;
}

TSetFlowCoreTargetResult TFlowExecutor::SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument)
{
    return Runtime_->SetFlowCoreTarget(argument);
}

void TFlowExecutor::RegisterCommand(
    std::string command,
    EPermission requiredPermission,
    TExecuteHandler handler)
{
    auto inserted = CommandDescriptors_.emplace(
        command,
        TCommandDescriptor{
            .Handler = std::move(handler),
            .RequiredPermission = requiredPermission,
        })
        .second;
    YT_VERIFY(inserted);
    Commands_.push_back(std::move(command));
}

EPermission TFlowExecutor::GetCommandRequiredPermission(const std::string& command) const
{
    if (auto it = CommandDescriptors_.find(command); it != CommandDescriptors_.end()) {
        return it->second.RequiredPermission;
    }
    THROW_ERROR_EXCEPTION(
        "No such command: %v. Cannot get required permission for unknown command. Possible commands: %v",
        command,
        ConvertToYsonString(Commands_, EYsonFormat::Text));
}

void TFlowExecutor::ThrowCommandNotFound(const std::string& command) const
{
    THROW_ERROR_EXCEPTION(
        "No such command: %v, possible commands: %v",
        command,
        ConvertToYsonString(Commands_, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

struct TLiveFlowExecutorRuntime
    : public IFlowExecutorRuntime
{
public:
    TLiveFlowExecutorRuntime(
        IControllerPtr controller,
        IPersistedStateManagerPtr persistedStateManager,
        IYTConnectorPtr ytConnector,
        TControllerServiceConfigPtr config,
        NYTree::IMapNodePtr orchidRoot,
        IStatusProfilerPtr rootStatusProfiler,
        IInvokerPtr poolInvoker,
        NHttp::IClientPtr httpClient,
        NRpc::IChannelFactoryPtr channelFactory,
        IPipelineAuthenticatorPtr authenticator);

    TYsonString RunCommand(
        const std::string& command,
        const TYsonString& argument,
        const std::string& user,
        const TCommandHandler& handler) override;

protected:
    TLogger Logger;

private:
    const IControllerPtr Controller_;
    const IPersistedStateManagerPtr PersistedStateManager_;
    const IYTConnectorPtr YTConnector_;
    const TControllerServiceConfigPtr Config_;
    const NYTree::IMapNodePtr OrchidRoot_;
    const IStatusProfilerPtr RootStatusProfiler_;
    const NHttp::IClientPtr HttpClient_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NRpc::IChannelFactoryPtr WorkerChannelFactory_;
    const IPipelineAuthenticatorPtr Authenticator_;
    const TLoadThroughputThrottlerPtr TablesThrottler_;
    const NTables::IKeyStatesPtr KeyStates_;
    const NTables::IPartitionStatesPtr PartitionStates_;
    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const IInvokerPtr PoolInvoker_;

private:
    TGetFlowViewResult GetFlowView(const TGetFlowViewArg& argument) override;

    //! Like get-flow-view but returns a compressed payload (TCompressedFlowViewResult); the codec comes
    //! from the dynamic spec. The full cached view is served from the keeper's pre-compressed blob.
    TGetFlowViewV2Result GetFlowViewV2(const TGetFlowViewV2Arg& argument) override;

    TVersionedPipelineSpecPtr GetPipelineSpec() override;
    TVersionedDynamicPipelineSpecPtr GetPipelineDynamicSpec() override;
    TVersionedFlowCoreTargetPtr GetFlowCoreTarget() override;

    TSetPipelineDynamicSpecResult SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument) override;
    //! One attempt of #SetPipelineDynamicSpec, without the conflict retries.
    TSetPipelineDynamicSpecResult DoSetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument);

    TSetPipelineSpecResult SetPipelineSpec(const TSetPipelineSpecArg& argument) override;

    TSetPipelineSpecsResult SetPipelineSpecs(const TSetPipelineSpecsArg& argument) override;

    TSetTargetPipelineStateResult SetTargetPipelineState(const TSetTargetPipelineStateArg& argument) override;

    TSetFlowCoreTargetResult SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument) override;
    TFlowViewPtr GetDescribeFlowView() override;
    THashMap<std::string, TError> GetDescribeControllerErrors() override;
    NDescribe::TDescribePipelineArguments MakeDescribePipelineArguments(bool statusOnly) override;
    TErrorOr<TYsonString> GetDescribeJobOrchid(
        const TPartitionId& partitionId,
        TDuration timeout) override;
    std::string GetDescribeDeployStageUrl() override;
    TFuture<TYsonString> RequestWorkerOrchid(const std::string& worker, const std::string& path);
    TYsonString GetWorkerOrchid(const TYsonString& argument) override;
    TYsonString GetControllerOrchid(const TYsonString& argument) override;
    TYsonString KillWorker(const TYsonString& argument) override;
    TYsonString UpdateWorker(const TYsonString& argument) override;
    TYsonString GetWorkerBacktraces(const TYsonString& argument) override;

    TYsonString ReadStates(const TYsonString& argument) override;

    TYsonString DeleteStates(const TYsonString& argument) override;


    void AuthorizeCommand(EPermission permission, const std::string& user) override;
};

////////////////////////////////////////////////////////////////////////////////

TGetFlowViewResult TLiveFlowExecutorRuntime::GetFlowView(const TGetFlowViewArg& argument)
{
    if (argument.Path.empty()) {
        return Controller_->GetFlowViewKeeper()->GetYsonString(argument.Cache);
    }
    return Controller_->GetFlowViewKeeper()->GetYsonStringByPath(TYPath(argument.Path), argument.Cache);
}

TGetFlowViewV2Result TLiveFlowExecutorRuntime::GetFlowViewV2(const TGetFlowViewV2Arg& argument)
{
    auto keeper = Controller_->GetFlowViewKeeper();
    // The common full-view case is served straight from the keeper's pre-compressed cache; a sub-path or a
    // fresh (cache=false) view is serialized via GetFlowView and compressed with the current codec.
    auto compressed = (argument.Path.empty() && argument.Cache)
        ? keeper->GetCompressedYsonString()
        : keeper->CompressYson(GetFlowView(argument));
    TGetFlowViewV2Result result;
    result.Codec = compressed.Codec;
    result.Data = std::string(compressed.Data.Begin(), compressed.Data.Size());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

//! Spec updates race the controller's own committing cycle: both write the pipeline's important
//! versions, and with the dyntable backend both also touch the leader lease row, so a row lock
//! conflict is an expected outcome rather than a failure. The pause is randomized because a fixed
//! one lets an update collide with the same periodic writer over and over.
bool IsRetriableSpecUpdateError(const TError& error)
{
    return static_cast<bool>(error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict));
}

TDuration GetSpecUpdateRetryDelay(TDuration period, int attempt)
{
    auto backoff = period * (1 << std::min(attempt, 3));
    return backoff / 2 + TDuration::MicroSeconds(RandomNumber<ui64>(backoff.MicroSeconds() + 1));
}

TVersionedDynamicPipelineSpecPtr TLiveFlowExecutorRuntime::GetPipelineDynamicSpec()
{
    return PersistedStateManager_->RecoverDynamicSpec();
}

////////////////////////////////////////////////////////////////////////////////

TSetPipelineDynamicSpecResult TLiveFlowExecutorRuntime::SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument)
{
    THROW_ERROR_EXCEPTION_UNLESS(argument.Spec, "Spec must be specified");

    // Every attempt rereads the current spec, so a retry applies the requested change to whatever
    // the winner of the conflict has left behind, not to a stale snapshot. #SetPipelineSpecs
    // retries the very same way; this path used to be the one without it.
    for (int attempt = 0;; ++attempt) {
        try {
            return DoSetPipelineDynamicSpec(argument);
        } catch (const TErrorException& exception) {
            if (attempt + 1 >= Config_->SetSpecRetryCount || !IsRetriableSpecUpdateError(exception.Error())) {
                throw;
            }
            auto delay = GetSpecUpdateRetryDelay(Config_->SetSpecRetryPeriod, attempt);
            YT_TLOG_INFO("Dynamic spec update conflicted, retrying")
                .With("Attempt", attempt + 1)
                .With("Delay", delay)
                .With(exception.Error());
            TDelayedExecutor::WaitForDuration(delay);
        }
    }
}

TSetPipelineDynamicSpecResult TLiveFlowExecutorRuntime::DoSetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument)
{
    auto versionedSpec = PersistedStateManager_->RecoverDynamicSpec();
    const auto originalVersion = versionedSpec->GetVersion();
    const auto expectedVersion = argument.ExpectedVersion.has_value() ? argument.ExpectedVersion.value() : originalVersion;
    auto specNode = argument.Spec;
    if (argument.Path != "") {
        auto subNode = std::move(specNode);
        specNode = ConvertToNode(versionedSpec->GetValue());
        NYTree::SetNodeByYPath(specNode, TYPath(argument.Path), subNode);
    }

    auto newDynamicSpec = NYTree::ConvertTo<TDynamicPipelineSpecPtr>(specNode);
    ValidateDynamicPipelineSpec(newDynamicSpec);
    versionedSpec->TrySetValue(newDynamicSpec, Controller_->GetVersionProvider());

    if (versionedSpec->GetVersion() != originalVersion) { // Persist only if there are changes.
        PersistedStateManager_->PersistSpecs(std::nullopt, std::nullopt, versionedSpec, expectedVersion);
        Controller_->GetFlowViewKeeper()->SetSpecs(std::nullopt, versionedSpec);
    }

    TSetPipelineDynamicSpecResult result;
    result.Version = versionedSpec->GetVersion();
    return result;
}

TVersionedPipelineSpecPtr TLiveFlowExecutorRuntime::GetPipelineSpec()
{
    return PersistedStateManager_->RecoverSpec();
}

////////////////////////////////////////////////////////////////////////////////

// Gate for spec updates: allows Stopped/Completed, Paused with force=true,
// and Unknown (controller has not yet observed the pipeline, e.g. first-time spec set).
// TODO: tighten Unknown handling once the pipeline-creation flow is clarified.
void CheckPipelineSafeForSpecUpdate(
    const TFlowViewKeeperPtr& keeper,
    bool force)
{
    auto flowView = keeper->GetFlowView();
    flowView->EnsureIsSynced();

    const auto state = flowView->State->ExecutionSpec->PipelineState->GetValue();
    if (state == EPipelineState::Stopped || state == EPipelineState::Unknown) {
        return;
    }
    if (state == EPipelineState::Paused && force) {
        return;
    }
    if (state == EPipelineState::Completed) {
        THROW_ERROR_EXCEPTION(
            "Cannot modify pipeline spec; pipeline is in Completed state. "
            "Recreate the pipeline to recover");
    }
    THROW_ERROR_EXCEPTION(
        "Cannot modify pipeline spec while pipeline is %Qlv; stop the pipeline first%v",
        state,
        state == EPipelineState::Paused
            ? " (or pass force=true to apply on a paused pipeline)"
            : "");
}

// Gate for state-accessor mutations: allows Stopped/Completed, or Paused with force=true.
// Unknown is rejected: we must observe the pipeline before touching its state.
void CheckPipelineSafeForStateMutation(
    const TFlowViewKeeperPtr& keeper,
    bool force,
    TStringBuf actionName)
{
    auto flowView = keeper->GetFlowView();
    THROW_ERROR_EXCEPTION_UNLESS(flowView->IsSynced(),
        "Cannot %v: controller has not observed the pipeline yet, retry later",
        actionName);

    const auto state = flowView->State->ExecutionSpec->PipelineState->GetValue();
    if (state == EPipelineState::Stopped || state == EPipelineState::Completed) {
        return;
    }
    if (state == EPipelineState::Paused && force) {
        return;
    }
    THROW_ERROR_EXCEPTION(
        "Cannot %v while pipeline is %Qlv; stop the pipeline first%v",
        actionName,
        state,
        state == EPipelineState::Paused
            ? " (or pass force=true to apply on a paused pipeline)"
            : "");
}

TSetPipelineSpecResult TLiveFlowExecutorRuntime::SetPipelineSpec(const TSetPipelineSpecArg& argument)
{
    // Delegate to SetPipelineSpecs.
    TSetPipelineSpecsArg specsArg;
    specsArg.Spec = argument.Spec;
    specsArg.DynamicSpec = std::nullopt;
    specsArg.ExpectedSpecVersion = argument.ExpectedVersion;
    specsArg.ExpectedDynamicSpecVersion = std::nullopt;
    specsArg.AllowSpecUpdateOnPause = argument.Force;
    specsArg.ValidateStrict = false;

    auto specsResult = SetPipelineSpecs(specsArg);

    TSetPipelineSpecResult result;
    result.Version = specsResult.SpecVersion;
    return result;
}

TSetPipelineSpecsResult TLiveFlowExecutorRuntime::SetPipelineSpecs(const TSetPipelineSpecsArg& argument)
{
    // At least one spec must be specified.
    THROW_ERROR_EXCEPTION_UNLESS(argument.Spec || argument.DynamicSpec,
        "At least one of Spec or DynamicSpec must be specified");

    auto keeper = Controller_->GetFlowViewKeeper();
    TSetPipelineSpecsResult result;

    for (int attempt = 0; attempt < Config_->SetSpecRetryCount; ++attempt) {
        try {
            auto spec = PersistedStateManager_->RecoverSpec();
            auto dynamicSpec = PersistedStateManager_->RecoverDynamicSpec();
            auto flowCoreTarget = PersistedStateManager_->RecoverFlowCoreTarget();

            const auto originalSpecVersion = spec->GetVersion();
            const auto originalDynamicSpecVersion = dynamicSpec->GetVersion();

            auto flowView = keeper->GetFlowView();

            const auto& targetValue = flowCoreTarget->GetValue();
            const auto& actualFlowCoreVersion = Controller_->GetNodeInfo()->FlowCoreVersion;
            if (!CheckFlowCoreTarget(targetValue, actualFlowCoreVersion)) {
                THROW_ERROR_EXCEPTION(NFlow::EErrorCode::FlowCoreTargetMismatch,
                    "Flow core target mismatches with actual running Controller version, cannot modify Spec")
                    .With("flow_core_target", targetValue)
                    .With("actual_flow_core_version", actualFlowCoreVersion);
            }

            auto expectedVersions = MakePipelineImportantVersions(flowView->State, spec);
            if (argument.ExpectedSpecVersion) {
                expectedVersions->PipelineSpecVersion = *argument.ExpectedSpecVersion;
            }
            const auto expectedDynamicSpecVersion = argument.ExpectedDynamicSpecVersion.value_or(originalDynamicSpecVersion);

            // Update specs if provided.
            if (argument.Spec) {
                spec->TrySetValue(NYTree::ConvertTo<TPipelineSpecPtr>(*argument.Spec), Controller_->GetVersionProvider());
                ValidatePipelineSpec(spec->GetValue());
            }
            if (argument.DynamicSpec) {
                dynamicSpec->TrySetValue(NYTree::ConvertTo<TDynamicPipelineSpecPtr>(*argument.DynamicSpec), Controller_->GetVersionProvider());
                ValidateDynamicPipelineSpec(dynamicSpec->GetValue());
            }

            result.SpecVersion = spec->GetVersion();
            result.DynamicSpecVersion = dynamicSpec->GetVersion();

            bool specChanged = (spec->GetVersion() != originalSpecVersion);
            bool dynamicSpecChanged = (dynamicSpec->GetVersion() != originalDynamicSpecVersion);

            if (!specChanged && !dynamicSpecChanged) {
                // No version increment - no change.
                break;
            }

            CheckPipelineSafeForSpecUpdate(
                keeper,
                argument.AllowSpecUpdateOnPause);

            if (argument.ValidateStrict) {
                std::vector<TError> errors;
                if (argument.Spec) {
                    auto specNode = argument.Spec.value()->AsMap();
                    auto specErrors = NFlow::TRegistry::Get()->ValidatePipelineSpecParseability(specNode);
                    if (!specErrors.empty()) {
                        errors.push_back(TError("Strict validation failed for Spec").With(specErrors));
                    }
                }

                // If dynamic spec is not changed we still validate it with new static spec.
                auto dynamicSpecNode = argument.DynamicSpec ? argument.DynamicSpec.value()->AsMap() : ConvertTo<IMapNodePtr>(dynamicSpec->GetValue());
                auto dynamicSpecErrors = NFlow::TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec->GetValue(), dynamicSpecNode);
                if (!dynamicSpecErrors.empty()) {
                    errors.push_back(TError("Strict validation failed for DynamicSpec").With(dynamicSpecErrors));
                }

                if (!errors.empty()) {
                    THROW_ERROR_EXCEPTION("Strict validation failed for specs").With(errors);
                }
            }

            // Persist specs atomically.
            PersistedStateManager_->PersistSpecs(
                specChanged ? std::make_optional(spec) : std::nullopt,
                specChanged ? std::make_optional(expectedVersions) : std::nullopt,
                dynamicSpecChanged ? std::make_optional(dynamicSpec) : std::nullopt,
                dynamicSpecChanged ? std::make_optional(expectedDynamicSpecVersion) : std::nullopt);

            // Update keeper atomically after successful persist.
            keeper->SetSpecs(
                specChanged ? std::make_optional(spec) : std::nullopt,
                dynamicSpecChanged ? std::make_optional(dynamicSpec) : std::nullopt);

            break;
        } catch (const TErrorException& exception) {
            if (attempt + 1 == Config_->SetSpecRetryCount) {
                throw;
            }
            if (exception.Error().FindMatching(NFlow::EErrorCode::PipelineStateVersionMismatch)) {
                continue;
            }
            if (exception.Error().FindMatching(NFlow::EErrorCode::FlowCoreTargetVersionMismatch)) {
                // The persisted FlowCoreTarget version can be ahead of the
                // controller's in-memory flow view.
                TDelayedExecutor::WaitForDuration(Config_->SetSpecRetryPeriod);
                continue;
            }
            // Conflict occurs when PersistSpecs transaction overlaps with a concurrent PersistFlowState transaction.
            // Both transactions are updating ImportantVersionsKey.
            if (exception.Error().FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
                TDelayedExecutor::WaitForDuration(Config_->SetSpecRetryPeriod);
                continue;
            }
            throw;
        }
    }

    return result;
}

TSetTargetPipelineStateResult TLiveFlowExecutorRuntime::SetTargetPipelineState(const TSetTargetPipelineStateArg& argument)
{
    // TODO(pechatnov): Move it closer to EPipelineState definition.
    constexpr auto isTargetPipelineState = [] (EPipelineState state) {
        return state == EPipelineState::Completed || state == EPipelineState::Stopped || state == EPipelineState::Paused;
    };

    THROW_ERROR_EXCEPTION_UNLESS(isTargetPipelineState(argument.TargetPipelineState), "State %Qv cannot be target pipeline state", argument.TargetPipelineState);

    auto keeper = Controller_->GetFlowViewKeeper();

    auto flowView = keeper->GetFlowView();
    if (flowView->IsSynced()) {
        const auto currentPipelineState = flowView->State->ExecutionSpec->PipelineState->GetValue();
        if (currentPipelineState == EPipelineState::Completed) {
            THROW_ERROR_EXCEPTION(
                "Pipeline is in %Qlv state, cannot change target state to %Qlv: "
                "Completed is a final state with no expected transitions. "
                "Recreate the pipeline to recover.",
                currentPipelineState,
                argument.TargetPipelineState);
        }
    }

    auto dynamicSpec = PersistedStateManager_->RecoverDynamicSpec();
    if (dynamicSpec->GetValue()->TargetState == argument.TargetPipelineState) {
        return {};
    }

    // Refuse start-pipeline when the controller does not match the FlowCoreTarget. In that case new TargetState
    // would be silently ignored and the pipeline would stay in its current state with no visible progress.
    if (argument.TargetPipelineState == EPipelineState::Completed) {
        auto target = PersistedStateManager_->RecoverFlowCoreTarget();
        const auto& targetValue = target->GetValue();
        const auto& controllerFlowCoreVersion = Controller_->GetNodeInfo()->FlowCoreVersion;
        if (!CheckFlowCoreTarget(targetValue, controllerFlowCoreVersion)) {
            THROW_ERROR_EXCEPTION(
                NFlow::EErrorCode::FlowCoreTargetMismatch,
                "Cannot start pipeline: controller FlowCoreVersion does not match FlowCoreTarget. "
                "Update the controller binary or reset the target.")
                .With("flow_core_target", targetValue)
                .With("controller_flow_core_version", controllerFlowCoreVersion);
        }
    }

    const auto expectedVersion = dynamicSpec->GetVersion();

    dynamicSpec->GetValue()->TargetState = argument.TargetPipelineState;
    dynamicSpec->Bump(Controller_->GetVersionProvider());

    PersistedStateManager_->PersistSpecs(std::nullopt, std::nullopt, dynamicSpec, expectedVersion);
    keeper->SetSpecs(std::nullopt, dynamicSpec);

    return {};
}

TVersionedFlowCoreTargetPtr TLiveFlowExecutorRuntime::GetFlowCoreTarget()
{
    return PersistedStateManager_->RecoverFlowCoreTarget();
}

////////////////////////////////////////////////////////////////////////////////

TSetFlowCoreTargetResult TLiveFlowExecutorRuntime::SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument)
{
    auto flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
    flowView->EnsureIsSynced();

    auto pipelineState = flowView->State->ExecutionSpec->PipelineState->GetValue();

    switch (pipelineState) {
        case EPipelineState::Stopped:
        case EPipelineState::Unknown:
            break;

        case EPipelineState::Paused:
            if (!argument.AllowUpdateOnPause) {
                THROW_ERROR_EXCEPTION(
                    "Cannot update FlowCoreTarget in state %Qv: stop the pipeline first, "
                    "or pass 'allow_update_on_pause=true' if you intend to update the pipeline in Paused state "
                    "(allowed states: Stopped, Paused with 'allow_update_on_pause=true')",
                    pipelineState);
            }
            break;

        default:
            THROW_ERROR_EXCEPTION(
                "Cannot update FlowCoreTarget in state %Qv: stop the pipeline first "
                "(allowed states: Stopped, Paused with 'allow_update_on_pause=true')",
                pipelineState);
    }

    // Read the current persisted target to get the up-to-date version.
    // The keeper's FlowView may be stale (e.g. when the pipeline is stopped and not reloading target).
    auto currentTarget = PersistedStateManager_->RecoverFlowCoreTarget();

    TSetFlowCoreTargetResult result;

    // Target value already matches. Additionally, if the caller specified an expected version,
    // verify it matches the current version — otherwise we must proceed and persist a new version
    // so the caller can detect the update.
    bool targetMatches = currentTarget->GetValue() == argument.FlowCoreTarget;
    bool targetVersionMatches = !argument.ExpectedVersion || *argument.ExpectedVersion == currentTarget->GetVersion();
    if (targetMatches && targetVersionMatches) {
        result.Version = currentTarget->GetVersion();
        return result;
    }

    auto expectedVersions = MakePipelineImportantVersions(flowView->State, flowView->CurrentSpec);
    expectedVersions->FlowCoreTargetVersion = argument.ExpectedVersion.value_or(currentTarget->GetVersion());

    auto newTarget = CloneYsonStruct(currentTarget);
    newTarget->TrySetValue(argument.FlowCoreTarget, Controller_->GetVersionProvider());

    PersistedStateManager_->PersistFlowCoreTarget(newTarget, expectedVersions);

    result.Version = newTarget->GetVersion();
    return result;
}

NDescribe::TDescribePipelineArguments TLiveFlowExecutorRuntime::MakeDescribePipelineArguments(bool statusOnly)
{
    auto controllerErrors = RootStatusProfiler_->GetStatus().Errors;

    TFlowViewPtr flowView;
    try {
        flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
    } catch (const TErrorException& ex) {
        if (!statusOnly) {
            throw;
        }
        controllerErrors["flow_view_keeper"] = TError(ex);
    }

    // The internal flow tables bundle is resolved via the connector, which caches the result
    // and is warmed at controller startup; it is ready by the time describe runs.
    auto flowTablesBundle = WaitFor(YTConnector_->GetFlowTablesBundle()).ValueOrThrow();

    return {
        .FlowView = flowView,
        .ControllerErrors = controllerErrors,
        .Logger = ControllerLogger(),
        .Authenticator = Authenticator_,
        .StatusOnly = statusOnly,
        .ControllerFlowCoreVersion = Controller_->GetNodeInfo()->FlowCoreVersion,
        .ControllerBuildInfo = GetFlowCoreBuildInfo(),
        .ControllerBuildType = Controller_->GetNodeInfo()->BuildType,
        .FlowTablesBundle = std::move(flowTablesBundle),
        .DeployStageUrl = GetDeployStageUrl(),
    };
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TYsonString> TLiveFlowExecutorRuntime::GetDescribeJobOrchid(
    const TPartitionId& partitionId,
    TDuration timeout)
{
    TErrorOr<TYsonString> jobOrchid;
    try {
        auto partitionCodicilGuard = TErrorCodicils::MakeGuard("partition_id", [&] {
            return ToString(partitionId);
        });

        auto flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        auto partitionIt = layout->Partitions.find(partitionId);
        THROW_ERROR_EXCEPTION_IF(partitionIt == layout->Partitions.end(), "Partition %Qv not found", partitionId);
        auto jobId = partitionIt->second->CurrentJobId;
        THROW_ERROR_EXCEPTION_UNLESS(jobId.has_value(), "Partition %Qv has no job", partitionId);
        auto jobIt = layout->Jobs.find(*jobId);
        THROW_ERROR_EXCEPTION_IF(jobIt == layout->Jobs.end(), "Job %Qv not found", *jobId);
        auto workerAddress = jobIt->second->WorkerAddress;

        auto req = RequestWorkerOrchid(workerAddress, Format("/job_tracker/jobs/%v", *jobId));
        jobOrchid = WaitFor(req.WithTimeout(timeout)).ValueOrThrow();
    } catch (const std::exception& e) {
        jobOrchid = TError(e);
    }

    return jobOrchid;
}

TFlowViewPtr TLiveFlowExecutorRuntime::GetDescribeFlowView()
{
    return Controller_->GetFlowViewKeeper()->GetFlowView();
}

THashMap<std::string, TError> TLiveFlowExecutorRuntime::GetDescribeControllerErrors()
{
    return RootStatusProfiler_->GetStatus().Errors;
}

std::string TLiveFlowExecutorRuntime::GetDescribeDeployStageUrl()
{
    return GetDeployStageUrl();
}

////////////////////////////////////////////////////////////////////////////////

struct TGetWorkerOrchidArg
    : public TYsonStructLite
{
    std::string Worker;
    std::string Path;

    REGISTER_YSON_STRUCT_LITE(TGetWorkerOrchidArg);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("worker", &TThis::Worker);
        registrar.Parameter("path", &TThis::Path)
            .Default("");
        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

TFuture<TYsonString> TLiveFlowExecutorRuntime::RequestWorkerOrchid(const std::string& worker, const std::string& path)
{
    auto workerCodicilGuard = TErrorCodicils::MakeGuard("worker", [&] {
        return worker;
    });
    auto pathCodicilGuard = TErrorCodicils::MakeGuard("request_path", [&] {
        return path;
    });
    auto state = Controller_->GetFlowViewKeeper()->GetFlowView()->State;
    auto it = state->Workers.find(worker);
    THROW_ERROR_EXCEPTION_IF(it == state->Workers.end(), "Worker is not known");
    THROW_ERROR_EXCEPTION_IF(!path.empty() && !path.starts_with("/"), "Path must be empty or starting with '/'");

    auto ypathService = CreateOrchidYPathService(TOrchidOptions{.Channel = WorkerChannelFactory_->CreateChannel(worker)});
    auto ypathReq = TYPathProxy::Get(TYPath(path));
    return ExecuteVerb(ypathService, ypathReq)
        .Apply(BIND([] (const TYPathProxy::TRspGetPtr& ypathRsp) {
            return TYsonString(ypathRsp->value());
        }));
}

TYsonString TLiveFlowExecutorRuntime::GetWorkerOrchid(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TGetWorkerOrchidArg>(serializedArgument);
    // clang-format off
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("value").Value(WaitFor(RequestWorkerOrchid(argument.Worker, argument.Path)).ValueOrThrow())
        .EndMap();
    // clang-format on
}

////////////////////////////////////////////////////////////////////////////////

TYsonString TLiveFlowExecutorRuntime::GetControllerOrchid(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TGetControllerOrchidArg>(serializedArgument);
    // Virtual node can not be reached with trivial call of GetNodeByYPath. So use node as IYPathService.
    auto ypathReq = TYPathProxy::Get(TYPath(argument.Path));
    auto ypathRsp = WaitFor(ExecuteVerb(OrchidRoot_, ypathReq)).ValueOrThrow("Orchid local request failed");

    TGetControllerOrchidResult result;
    result.Value = TYsonString(ypathRsp->value());
    return ConvertToYsonString(result);
}

////////////////////////////////////////////////////////////////////////////////

struct TKillWorkerArg
    : public TYsonStructLite
{
    std::string Worker;
    int ExitCode{};
    TDuration Timeout;

    REGISTER_YSON_STRUCT_LITE(TKillWorkerArg);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("worker", &TThis::Worker);
        registrar.Parameter("exit_code", &TThis::ExitCode)
            .Default(13);
        registrar.Parameter("timeout", &TThis::Timeout)
            .Default(TDuration::Minutes(1));

        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

TYsonString TLiveFlowExecutorRuntime::KillWorker(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TKillWorkerArg>(serializedArgument);

    auto workerCodicilGuard = TErrorCodicils::MakeGuard("worker", [&] {
        return argument.Worker;
    });

    auto state = Controller_->GetFlowViewKeeper()->GetFlowView()->State;
    auto it = state->Workers.find(argument.Worker);
    THROW_ERROR_EXCEPTION_IF(it == state->Workers.end(), "Worker is not known");

    TAdminServiceProxy proxy(WorkerChannelFactory_->CreateChannel(argument.Worker));
    auto req = proxy.Die();
    req->SetTimeout(argument.Timeout);
    req->set_exit_code(argument.ExitCode);

    auto error = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF(error.IsOK(), "Killing worker request should not complete successfully");

    // clang-format off
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("kill_request_response_error").Value(error)
        .EndMap();
    // clang-format on
}

////////////////////////////////////////////////////////////////////////////////


struct TUpdateWorkerArg
    : public TYsonStructLite
{
    std::string Worker;
    std::optional<bool> Banned;
    std::optional<IMapNodePtr> Coefficients;

    REGISTER_YSON_STRUCT_LITE(TUpdateWorkerArg);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("worker", &TThis::Worker);
        registrar.Parameter("banned", &TThis::Banned)
            .Default();
        registrar.Parameter("coefficients", &TThis::Coefficients)
            .Default();

        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

TYsonString TLiveFlowExecutorRuntime::UpdateWorker(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TUpdateWorkerArg>(serializedArgument);

    THROW_ERROR_EXCEPTION_IF(argument.Banned.has_value(), "Unimplemented");
    THROW_ERROR_EXCEPTION_IF(argument.Coefficients.has_value(), "Unimplemented");

    // clang-format off
    return BuildYsonStringFluently()
        .BeginMap()
        .EndMap();
    // clang-format on
}

////////////////////////////////////////////////////////////////////////////////

struct TGetWorkerBacktracesArg
    : public TYsonStructLite
{
    std::string Worker;

    REGISTER_YSON_STRUCT_LITE(TGetWorkerBacktracesArg);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("worker", &TThis::Worker);

        registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
    }
};

TYsonString TLiveFlowExecutorRuntime::GetWorkerBacktraces(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TGetWorkerBacktracesArg>(serializedArgument);

    auto workerCodicilGuard = TErrorCodicils::MakeGuard("worker", [&] {
        return argument.Worker;
    });

    auto threads = ConvertTo<std::string>(WaitFor(RequestWorkerOrchid(argument.Worker, "/backtraces/threads")).ValueOrThrow());
    auto fibers = ConvertTo<std::string>(WaitFor(RequestWorkerOrchid(argument.Worker, "/backtraces/fibers")).ValueOrThrow());

    // clang-format off
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("text").Value(Format("Threads:\n%v\n\nFibers:\n%v\n", threads, fibers))
        .EndMap();
    // clang-format on
}

////////////////////////////////////////////////////////////////////////////////

// Pick whichever YSON representation of the state is non-empty (raw or delta-coded).
TYsonString FlattenState(const NYsonSerializer::TStatePtr& state)
{
    if (!state) {
        return {};
    }
    const auto internalState = state->GetValueAs<NTables::TInternalState>();
    if (internalState->State && *internalState->State) {
        return *internalState->State;
    }
    if (internalState->Compressed && *internalState->Compressed) {
        return *internalState->Compressed;
    }
    return {};
}

const TPartitionPtr& GetPartitionOrThrow(const TFlowViewPtr& flowView, TPartitionId partitionId)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto it = layout->Partitions.find(partitionId);
    THROW_ERROR_EXCEPTION_IF(it == layout->Partitions.end(),
        "Unknown partition %Qv",
        partitionId);
    return it->second;
}

struct TStateAccessPlan
{
    std::vector<NTables::IKeyStates::TTableKeyFilter> KeyFilters;
    std::vector<NTables::IPartitionStates::TTableKeyFilter> PartitionFilters;
};

// Paginates each filter and feeds chunks to |onChunk|. Stops once |limit| rows total have been
// yielded (if set). Returns the number of rows yielded.
template <class TTable, class TFilter, class TOnChunk>
i64 ForEachMatchingKey(
    const TTable& table,
    const std::vector<TFilter>& filters,
    i64 batchSize,
    std::optional<i64> limit,
    TOnChunk onChunk)
{
    using TTableKey = typename std::remove_reference_t<decltype(*table)>::TTableKey;
    i64 total = 0;
    for (const auto& filter : filters) {
        std::optional<TTableKey> continuation;
        while (true) {
            const i64 pageSize = limit ? std::min<i64>(batchSize, *limit - total) : batchSize;
            if (pageSize <= 0) {
                return total;
            }
            auto listResult = WaitFor(table->List(filter, pageSize, continuation))
                .ValueOrThrow();
            if (listResult.Keys.empty()) {
                break;
            }
            total += std::ssize(listResult.Keys);
            onChunk(std::move(listResult.Keys));
            if (!listResult.ContinuationOffsetExclusive) {
                break;
            }
            continuation = std::move(listResult.ContinuationOffsetExclusive);
        }
    }
    return total;
}

TStateAccessPlan BuildStateAccessPlan(
    const TStateAccessArgs& argument,
    const TFlowViewPtr& flowView,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const NLogging::TLogger& logger)
{
    const auto& spec = flowView->CurrentSpec->GetValue();
    if (argument.ComputationId) {
        THROW_ERROR_EXCEPTION_IF(!spec->Computations.contains(*argument.ComputationId),
            "Unknown computation %Qv",
            *argument.ComputationId);
    }

    const bool wantKey = argument.Target == EFlowStateTarget::All || argument.Target == EFlowStateTarget::KeyState;
    const bool wantPartition = argument.Target == EFlowStateTarget::All || argument.Target == EFlowStateTarget::PartitionState;

    TStateAccessPlan plan;

    if (argument.ComputationId) {
        if (wantKey) {
            NTables::IKeyStates::TTableKeyFilter filter;
            filter.ComputationId = argument.ComputationId;
            if (argument.KeyAsYson) {
                const auto& computationSpec = GetOrCrash(spec->Computations, *argument.ComputationId);
                filter.ExactKey = BuildKeyFromYson(*argument.KeyAsYson, computationSpec->GroupBySchema, evaluatorCache, logger);
            }
            filter.Name = argument.Name;
            plan.KeyFilters.push_back(std::move(filter));
        }
        // partition_states has no computation_id column; emit one filter per partition.
        if (wantPartition && !argument.KeyAsYson) {
            const auto& layout = flowView->State->ExecutionSpec->Layout;
            for (const auto& [partitionId, partition] : layout->Partitions) {
                if (partition->ComputationId != *argument.ComputationId) {
                    continue;
                }
                NTables::IPartitionStates::TTableKeyFilter filter;
                filter.PartitionId = partitionId;
                filter.Name = argument.Name;
                plan.PartitionFilters.push_back(std::move(filter));
            }
        }
    } else {
        YT_VERIFY(argument.PartitionId);
        const auto& partition = GetPartitionOrThrow(flowView, *argument.PartitionId);
        if (wantKey && partition->SourceKey) {
            NTables::IKeyStates::TTableKeyFilter filter;
            filter.ComputationId = partition->ComputationId;
            filter.ExactKey = *partition->SourceKey;
            filter.Name = argument.Name;
            plan.KeyFilters.push_back(std::move(filter));
        }
        if (wantPartition) {
            NTables::IPartitionStates::TTableKeyFilter filter;
            filter.PartitionId = *argument.PartitionId;
            filter.Name = argument.Name;
            plan.PartitionFilters.push_back(std::move(filter));
        }
    }

    return plan;
}

TYsonString TLiveFlowExecutorRuntime::ReadStates(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TReadStatesArg>(serializedArgument);

    const auto flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
    auto plan = BuildStateAccessPlan(argument, flowView, ColumnEvaluatorCache_, Logger);

    // Each section honours ``limit`` independently; see read-states design.
    std::vector<NTables::IKeyStates::TTableKey> keyStateKeys;
    std::vector<NTables::IPartitionStates::TTableKey> partitionStateKeys;
    auto collect = [] (auto& dst) {
        return [&dst] (auto chunk) {
            for (auto& key : chunk) {
                dst.push_back(std::move(key));
            }
        };
    };

    ForEachMatchingKey(KeyStates_, plan.KeyFilters, argument.Limit, argument.Limit, collect(keyStateKeys));
    ForEachMatchingKey(PartitionStates_, plan.PartitionFilters, argument.Limit, argument.Limit, collect(partitionStateKeys));

    auto keyValues = WaitFor(KeyStates_->Lookup(
        THashSet<NTables::IKeyStates::TTableKey>(keyStateKeys.begin(), keyStateKeys.end())))
        .ValueOrThrow();
    auto partitionValues = WaitFor(PartitionStates_->Lookup(
        THashSet<NTables::IPartitionStates::TTableKey>(partitionStateKeys.begin(), partitionStateKeys.end())))
        .ValueOrThrow();

    std::map<std::pair<TComputationId, TKey>, THashMap<std::string, TYsonString>> keyGrouped;
    for (const auto& tableKey : keyStateKeys) {
        if (auto value = FlattenState(GetOrDefault(keyValues, tableKey, {}))) {
            keyGrouped[{tableKey.ComputationId, tableKey.Key}].emplace(tableKey.Name, std::move(value));
        }
    }
    // partition_states has no computation_id column; recover it from the layout so the
    // output is uniformly typed with key_states / external_key_states.
    std::map<TPartitionId, std::pair<std::optional<TComputationId>, THashMap<std::string, TYsonString>>> partitionGrouped;
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    for (const auto& tableKey : partitionStateKeys) {
        if (auto value = FlattenState(GetOrDefault(partitionValues, tableKey, {}))) {
            auto& entry = partitionGrouped[tableKey.PartitionId];
            if (!entry.first) {
                if (auto it = layout->Partitions.find(tableKey.PartitionId); it != layout->Partitions.end()) {
                    entry.first = it->second->ComputationId;
                }
            }
            entry.second.emplace(tableKey.Name, std::move(value));
        }
    }

    TReadStatesResponse response;
    response.KeyStates.reserve(keyGrouped.size());
    for (auto& [identity, states] : keyGrouped) {
        TKeyStateRow row;
        row.ComputationId = identity.first;
        row.Key = identity.second;
        row.States = std::move(states);
        response.KeyStates.push_back(std::move(row));
    }
    response.PartitionStates.reserve(partitionGrouped.size());
    for (auto& [partitionId, entry] : partitionGrouped) {
        TPartitionStateRow row;
        row.ComputationId = entry.first;
        row.PartitionId = partitionId;
        row.States = std::move(entry.second);
        response.PartitionStates.push_back(std::move(row));
    }

    const bool wantExternal = argument.Target == EFlowStateTarget::All || argument.Target == EFlowStateTarget::ExternalKeyState;
    if (wantExternal) {
        TComputationId resolvedComputationId = argument.ComputationId
            ? *argument.ComputationId
            : GetPartitionOrThrow(flowView, *argument.PartitionId)->ComputationId;
        const auto& computationSpec = GetOrCrash(
            flowView->CurrentSpec->GetValue()->Computations,
            resolvedComputationId);
        // Skip the (heavy) bundle setup when the computation has no externals at all.
        if (!computationSpec->ExternalStateManagers.empty() || !computationSpec->ExternalStateJoiners.empty()) {
            try {
                auto converterCache = CreatePayloadConverterCache(ColumnEvaluatorCache_);
                auto bundle = BuildControllerExternalStateBundle(
                    flowView,
                    resolvedComputationId,
                    YTConnector_->GetClientsCache(),
                    YTConnector_->GetPipelinePath(),
                    Authenticator_,
                    RootStatusProfiler_,
                    ControllerProfiler().WithPrefix("/flow_executor"),
                    converterCache,
                    Logger);
                auto externalFilters = BuildExternalStateFilters(
                    argument,
                    bundle,
                    flowView,
                    ColumnEvaluatorCache_,
                    converterCache,
                    Logger);
                auto externalResult = ReadExternalStates(
                    resolvedComputationId,
                    bundle,
                    externalFilters,
                    argument.Limit,
                    Logger);
                response.ExternalKeyStates = std::move(externalResult.ManagerRows);
                response.JoinedExternalKeyStates = std::move(externalResult.JoinerRows);
                for (auto& err : externalResult.Errors) {
                    response.Errors.push_back(std::move(err));
                }
            } catch (const std::exception& ex) {
                response.Errors.push_back(Format("External state read failed: %v", TError(ex)));
            }
        }
    }
    return ConvertToYsonString(response, EYsonFormat::Pretty);
}

TYsonString TLiveFlowExecutorRuntime::DeleteStates(const TYsonString& serializedArgument)
{
    auto argument = ConvertTo<TDeleteStatesArg>(serializedArgument);

    auto keeper = Controller_->GetFlowViewKeeper();
    CheckPipelineSafeForStateMutation(keeper, argument.Force, "delete states");

    const auto flowView = keeper->GetFlowView();
    auto plan = BuildStateAccessPlan(argument, flowView, ColumnEvaluatorCache_, Logger);

    auto client = YTConnector_->GetClient();
    auto erase = [&] (const auto& table, auto& chunk) {
        if (argument.Commit) {
            auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
                .ValueOrThrow();
            table->Erase(transaction, chunk);
            WaitFor(transaction->Commit()).ThrowOnError();
        }
    };

    TDeleteStatesResponse response;
    response.Committed = argument.Commit;

    ForEachMatchingKey(KeyStates_, plan.KeyFilters, StateAccessBatchSize, std::nullopt, [&] (auto chunk) {
        for (const auto& tableKey : chunk) {
            response.MatchedStates.KeyStates.Total += 1;
            response.MatchedStates.KeyStates.Details[tableKey.ComputationId][tableKey.Name] += 1;
        }
        erase(KeyStates_, chunk);
    });

    // partition_states has no computation_id column; resolve via layout.
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    ForEachMatchingKey(PartitionStates_, plan.PartitionFilters, StateAccessBatchSize, std::nullopt, [&] (auto chunk) {
        for (const auto& tableKey : chunk) {
            TComputationId computationId;
            if (auto it = layout->Partitions.find(tableKey.PartitionId); it != layout->Partitions.end()) {
                computationId = it->second->ComputationId;
            }
            response.MatchedStates.PartitionStates.Total += 1;
            response.MatchedStates.PartitionStates.Details[computationId][tableKey.Name] += 1;
        }
        erase(PartitionStates_, chunk);
    });

    const bool wantExternal = argument.Target == EFlowStateTarget::All || argument.Target == EFlowStateTarget::ExternalKeyState;
    if (wantExternal) {
        TComputationId resolvedComputationId = argument.ComputationId
            ? *argument.ComputationId
            : GetPartitionOrThrow(flowView, *argument.PartitionId)->ComputationId;
        const auto& computationSpec = GetOrCrash(
            flowView->CurrentSpec->GetValue()->Computations,
            resolvedComputationId);
        // Delete touches only managers; if there are none, skip bundle setup.
        if (!computationSpec->ExternalStateManagers.empty()) {
            try {
                auto converterCache = CreatePayloadConverterCache(ColumnEvaluatorCache_);
                auto bundle = BuildControllerExternalStateBundle(
                    flowView,
                    resolvedComputationId,
                    YTConnector_->GetClientsCache(),
                    YTConnector_->GetPipelinePath(),
                    Authenticator_,
                    RootStatusProfiler_,
                    ControllerProfiler().WithPrefix("/flow_executor"),
                    converterCache,
                    Logger);
                auto externalFilters = BuildExternalStateFilters(
                    argument,
                    bundle,
                    flowView,
                    ColumnEvaluatorCache_,
                    converterCache,
                    Logger);
                auto externalResult = DeleteExternalStates(
                    resolvedComputationId,
                    bundle,
                    externalFilters,
                    client,
                    argument.Commit,
                    Logger);
                response.MatchedStates.ExternalKeyStates = std::move(externalResult.Matched);
                for (auto& err : externalResult.Errors) {
                    response.Errors.push_back(std::move(err));
                }
            } catch (const std::exception& ex) {
                response.Errors.push_back(Format("External state delete failed: %v", TError(ex)));
            }
        }
    }

    return ConvertToYsonString(response, EYsonFormat::Pretty);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TLoadThroughputThrottlerPtr CreateTablesThrottler(
    const TLoadThroughputThrottlerSpecPtr& spec,
    const TLogger& logger)
{
    auto throttler = New<TLoadThroughputThrottler>(
        "flow_executor/tables",
        logger,
        ControllerProfiler().WithPrefix("/flow_executor/tables_throttler"));
    throttler->Reconfigure(spec);
    return throttler;
}

NTables::TContextPtr CreateTablesContext(
    const IYTConnectorPtr& ytConnector,
    const TLoadThroughputThrottlerPtr& throttler,
    const TLogger& logger)
{
    auto context = New<NTables::TContext>();
    context->Client = ytConnector->GetClient();
    context->PipelinePath = ytConnector->GetPipelinePath();
    context->LoadThroughputThrottler = throttler;
    context->Logger = logger;
    context->Profiler = ControllerProfiler().WithPrefix("/flow_executor/tables");
    return context;
}

} // namespace

TLiveFlowExecutorRuntime::TLiveFlowExecutorRuntime(
    IControllerPtr controller,
    IPersistedStateManagerPtr persistedStateManager,
    IYTConnectorPtr ytConnector,
    TControllerServiceConfigPtr config,
    NYTree::IMapNodePtr orchidRoot,
    IStatusProfilerPtr rootStatusProfiler,
    IInvokerPtr poolInvoker,
    NHttp::IClientPtr httpClient,
    NRpc::IChannelFactoryPtr channelFactory,
    IPipelineAuthenticatorPtr authenticator)
    : Logger(ControllerLogger())
    , Controller_(std::move(controller))
    , PersistedStateManager_(std::move(persistedStateManager))
    , YTConnector_(std::move(ytConnector))
    , Config_(std::move(config))
    , OrchidRoot_(std::move(orchidRoot))
    , RootStatusProfiler_(std::move(rootStatusProfiler))
    , HttpClient_(std::move(httpClient))
    , ChannelFactory_(std::move(channelFactory))
    , WorkerChannelFactory_(authenticator ? authenticator->CreateSelfCredentialsInjectingChannelFactory(ChannelFactory_) : ChannelFactory_)
    , Authenticator_(authenticator)
    , TablesThrottler_(CreateTablesThrottler(Config_->TablesThrottler, Logger))
    , KeyStates_(New<NTables::TKeyStates>(
        CreateTablesContext(YTConnector_, TablesThrottler_, Logger),
        New<TDynamicTableRequestSpec>()))
    , PartitionStates_(New<NTables::TPartitionStates>(
        CreateTablesContext(YTConnector_, TablesThrottler_, Logger),
        New<TDynamicTableRequestSpec>()))
    , ColumnEvaluatorCache_(NQueryClient::CreateColumnEvaluatorCache(
        New<NQueryClient::TColumnEvaluatorCacheConfig>()))
    , PoolInvoker_(std::move(poolInvoker))
{ }

TYsonString TLiveFlowExecutorRuntime::RunCommand(
    const std::string& command,
    const TYsonString& argument,
    const std::string& user,
    const TCommandHandler& handler)
{
    TCodicilGuard codicilGuard([&] (TCodicilFormatter* formatter) {
        formatter->AppendString("User: ");
        formatter->AppendString(user);
        formatter->AppendString(", FlowExecuteCommand: ");
        formatter->AppendString(command);
        // May be truncated because codicil length limitation.
        formatter->AppendString(", FlowExecuteArgument: ");
        formatter->AppendString(ConvertToYsonString(ConvertTo<INodePtr>(argument), EYsonFormat::Text).ToString());
    });

    auto commandErrorCodicilGuard = TErrorCodicils::MakeGuard("command", [&] {
        return command;
    });

    YT_TLOG_DEBUG("Got flow-execute request")
        .With("Command", command)
        .With("User", user);

    Controller_->EnsureIsLeader();

    // Hold an extra ref so ~TFlowView never runs on the RPC reply fiber.
    auto flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
    auto destroyGuard = Finally([this, &flowView] {
        if (flowView) {
            PoolInvoker_->Invoke(BIND([fv = std::move(flowView)] () mutable {
                fv.Reset();
            }));
        }
    });

    return handler();
}

void TLiveFlowExecutorRuntime::AuthorizeCommand(EPermission permission, const std::string& user)
{
    auto pipelinePath = YTConnector_->GetPipelinePath().GetPath();

    auto response = WaitFor(YTConnector_->GetClient()->CheckPermission(user, pipelinePath, permission))
        .ValueOrThrow();
    response.ToError(user, permission)
        .ThrowOnError("No %Qlv permission for pipeline %v", permission, pipelinePath);
}

////////////////////////////////////////////////////////////////////////////////

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

IFlowExecutorPtr CreateFlowExecutor(IFlowExecutorRuntimePtr runtime)
{
    return New<TFlowExecutor>(std::move(runtime));
}

////////////////////////////////////////////////////////////////////////////////

IFlowExecutorPtr CreateFlowExecutor(
    IControllerPtr controller,
    IPersistedStateManagerPtr persistedStateManager,
    IYTConnectorPtr ytConnector,
    TControllerServiceConfigPtr config,
    NYTree::IMapNodePtr orchidRoot,
    IStatusProfilerPtr rootStatusProfiler,
    IInvokerPtr poolInvoker,
    NHttp::IClientPtr httpClient,
    NRpc::IChannelFactoryPtr channelFactory,
    IPipelineAuthenticatorPtr authenticator)
{
    return CreateFlowExecutor(New<TLiveFlowExecutorRuntime>(
        std::move(controller),
        std::move(persistedStateManager),
        std::move(ytConnector),
        std::move(config),
        std::move(orchidRoot),
        std::move(rootStatusProfiler),
        std::move(poolInvoker),
        std::move(httpClient),
        std::move(channelFactory),
        std::move(authenticator)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
