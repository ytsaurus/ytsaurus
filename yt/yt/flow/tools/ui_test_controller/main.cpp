#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/controller/controller_service.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor_runtime.h>

#include <yt/yt/flow/library/cpp/runner/init.h>

#include <yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib/computation.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/program.h>

#include <library/cpp/yt/error/origin_attributes.h>

#include <util/stream/file.h>
#include <util/stream/output.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NController;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotFlowExecutorRuntime
    : public IFlowExecutorRuntime
{
public:
    TSnapshotFlowExecutorRuntime(
        TFlowViewPtr flowView,
        TInstant now,
        THashMap<TPartitionId, TErrorOr<TYsonString>> jobOrchids,
        std::string controllerFlowCoreVersion)
        : FlowView_(std::move(flowView))
        , Now_(now)
        , JobOrchids_(std::move(jobOrchids))
        , ControllerFlowCoreVersion_(std::move(controllerFlowCoreVersion))
    { }

private:
    const TFlowViewPtr FlowView_;
    const TInstant Now_;
    const THashMap<TPartitionId, TErrorOr<TYsonString>> JobOrchids_;
    const std::string ControllerFlowCoreVersion_;

    TYsonString RunCommand(
        const std::string& /*command*/,
        const TYsonString& /*argument*/,
        const std::string& /*user*/,
        const TCommandHandler& handler) override
    {
        return handler();
    }

    void AuthorizeCommand(EPermission /*permission*/, const std::string& /*user*/) override
    { }

    TGetFlowViewResult GetFlowView(const TGetFlowViewArg& argument) override
    {
        if (argument.Path.empty()) {
            return FlowView_->SerializeAsYsonString();
        }
        return SerializeFlowViewByPath(FlowView_, NYPath::TYPath(argument.Path));
    }

    TGetFlowViewV2Result GetFlowViewV2(const TGetFlowViewV2Arg& argument) override
    {
        auto codec = FlowView_->CurrentDynamicSpec->GetValue()->FlowViewCacheCodec;
        auto compressed = CompressFlowViewYson(GetFlowView(argument), codec);
        TGetFlowViewV2Result result;
        result.Codec = compressed.Codec;
        result.Data = std::string(compressed.Data.Begin(), compressed.Data.Size());
        return result;
    }

    TVersionedPipelineSpecPtr GetPipelineSpec() override
    {
        return FlowView_->CurrentSpec;
    }

    TVersionedDynamicPipelineSpecPtr GetPipelineDynamicSpec() override
    {
        return FlowView_->CurrentDynamicSpec;
    }

    TVersionedFlowCoreTargetPtr GetFlowCoreTarget() override
    {
        return FlowView_->State->ExecutionSpec->FlowCoreTarget;
    }

    TFlowViewPtr GetDescribeFlowView() override
    {
        return FlowView_;
    }

    THashMap<std::string, TError> GetDescribeControllerErrors() override
    {
        return {};
    }

    NDescribe::TDescribePipelineArguments MakeDescribePipelineArguments(bool statusOnly) override
    {
        return {
            .FlowView = FlowView_,
            .Now = Now_,
            .Logger = NLogging::TLogger("UITestController"),
            .StatusOnly = statusOnly,
            .ControllerFlowCoreVersion = ControllerFlowCoreVersion_,
            .ControllerBuildInfo = New<TFlowCoreBuildInfo>(),
        };
    }

    TErrorOr<TYsonString> GetDescribeJobOrchid(
        const TPartitionId& partitionId,
        TDuration /*timeout*/) override
    {
        if (auto* orchid = JobOrchids_.FindPtr(partitionId)) {
            return *orchid;
        }
        TErrorSanitizerGuard errorSanitizerGuard(Now_, TSharedRef::FromString(std::string("localhost")));
        return TError("Job orchid for partition %v is unavailable in UI test controller", partitionId);
    }

    std::string GetDescribeDeployStageUrl() override
    {
        return {};
    }

    TSetPipelineDynamicSpecResult SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& /*argument*/) override
    {
        ThrowCommandNotSupported("set-pipeline-dynamic-spec");
    }

    TSetPipelineSpecResult SetPipelineSpec(const TSetPipelineSpecArg& /*argument*/) override
    {
        ThrowCommandNotSupported("set-pipeline-spec");
    }

    TSetPipelineSpecsResult SetPipelineSpecs(const TSetPipelineSpecsArg& /*argument*/) override
    {
        ThrowCommandNotSupported("set-pipeline-specs");
    }

    TSetTargetPipelineStateResult SetTargetPipelineState(const TSetTargetPipelineStateArg& /*argument*/) override
    {
        ThrowCommandNotSupported("set-target-pipeline-state");
    }

    TSetFlowCoreTargetResult SetFlowCoreTarget(const TSetFlowCoreTargetArg& /*argument*/) override
    {
        ThrowCommandNotSupported("set-flow-core-target");
    }

    TYsonString GetWorkerOrchid(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("get-worker-orchid");
    }

    TYsonString GetControllerOrchid(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("get-controller-orchid");
    }

    TYsonString KillWorker(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("kill-worker");
    }

    TYsonString UpdateWorker(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("update-worker");
    }

    TYsonString GetWorkerBacktraces(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("get-worker-backtraces");
    }

    TYsonString ReadStates(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("read-states");
    }

    TYsonString DeleteStates(const TYsonString& /*argument*/) override
    {
        ThrowCommandNotSupported("delete-states");
    }

    [[noreturn]] static void ThrowCommandNotSupported(TStringBuf command)
    {
        THROW_ERROR_EXCEPTION("Command %Qv is not supported by UI test controller", command);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUITestControllerProgram
    : public virtual TProgram
{
public:
    TUITestControllerProgram()
    {
        Opts_.AddLongOption("flow-view", "Path to a serialized FlowView fixture")
            .StoreResult(&FlowViewPath_)
            .Required();
        Opts_.AddLongOption("job-orchids", "Path to captured job orchids keyed by partition ID")
            .StoreResult(&JobOrchidsPath_)
            .Required();
        Opts_.AddLongOption("pipeline-path", "Pipeline path exposed by the replay controller")
            .StoreResult(&PipelinePath_)
            .Required();
        Opts_.AddLongOption("now-seconds", "Fixed Unix timestamp used by describe")
            .StoreResult(&NowSeconds_)
            .Required();
        Opts_.AddLongOption("port", "RPC listen port")
            .StoreResult(&Port_)
            .DefaultValue(19010);
    }

protected:
    void DoRun() override
    {
        auto serializedFlowView = TFileInput(FlowViewPath_).ReadAll();
        auto node = ConvertToNode(TYsonStringBuf(serializedFlowView));
        auto control = New<TPersistedStateControl<std::string>>(
            New<TNullStorageHandler<std::string>>());
        auto flowView = TFlowView::LoadFromNode(node, std::move(control));
        flowView->EphemeralState->PipelinePath = NYPath::TRichYPath::Parse(PipelinePath_);
        auto flowCoreVersion = flowView->State->ExecutionSpec->FlowCoreTarget->GetValue().Underlying();

        THashMap<TPartitionId, TErrorOr<TYsonString>> jobOrchids;
        auto serializedJobOrchids = TFileInput(JobOrchidsPath_).ReadAll();
        for (const auto& [partitionId, orchid] : ConvertToNode(TYsonStringBuf(serializedJobOrchids))->AsMap()->GetChildren()) {
            jobOrchids.emplace(
                TPartitionId(TGuid::FromString(partitionId)),
                ConvertToYsonString(orchid));
        }

        auto runtime = New<TSnapshotFlowExecutorRuntime>(
            std::move(flowView),
            TInstant::Seconds(NowSeconds_),
            std::move(jobOrchids),
            std::move(flowCoreVersion));
        auto executor = CreateFlowExecutor(std::move(runtime));

        auto workerPool = CreateThreadPool(2, "FlowUiTestRpc");
        auto busServer = NBus::NTcp::CreateBusServer(
            NBus::NTcp::TBusServerConfig::CreateTcp(Port_));
        auto rpcServer = NRpc::NBus::CreateBusServer(busServer);
        rpcServer->RegisterService(CreateControllerService(
            std::move(executor),
            NRpc::CreateNoopAuthenticator(),
            workerPool->GetInvoker()));
        rpcServer->Start();

        Cout << Format("READY localhost:%v\n", Port_);
        Cout.Flush();
        Sleep(TDuration::Max());
    }

private:
    std::string FlowViewPath_;
    std::string JobOrchidsPath_;
    std::string PipelinePath_;
    i64 NowSeconds_ = 0;
    int Port_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

int main(int argc, const char** argv)
{
    NYT::NFlow::NUIScreenshotPipeline::LinkUIScreenshotPipeline();
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TUITestControllerProgram().Run(argc, argv);
}
