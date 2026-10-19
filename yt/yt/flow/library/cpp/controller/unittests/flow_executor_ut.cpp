#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/controller.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor.h>

#include <yt/yt/flow/library/cpp/controller/unittests/mock/persisted_state_manager.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/yt_connector.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/process_function_graph.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TSpecUpdateDiscardFunction
    : public IProcessFunction
{ };

YT_FLOW_DEFINE_PROCESS_FUNCTION(TSpecUpdateDiscardFunction);

class TSpecUpdateControllerMock
    : public IController
{
public:
    MOCK_METHOD(void, Initialize, (), (override));
    MOCK_METHOD(void, EnsureIsLeader, (), (const, override));
    MOCK_METHOD(TFlowViewKeeperPtr, GetFlowViewKeeper, (), (const, override));
    MOCK_METHOD(TNodeInfoPtr, GetNodeInfo, (), (const, override));
    MOCK_METHOD(IVersionProviderPtr, GetVersionProvider, (), (const, override));
    MOCK_METHOD(void, RegisterJobStatus, (const TJobId&, TJobStatusPtr), (override));
    MOCK_METHOD(void, RegisterWorkerStatus, (TStringBuf, TWorkerStatusPtr), (override));
};

////////////////////////////////////////////////////////////////////////////////

TPipelineSpecPtr MakeGraphPipelineSpec(bool includeRemovedNode)
{
    auto spec = New<TPipelineSpec>();
    auto stream = New<TStreamSpec>();
    stream->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonStringBuf("[{name=key;type=uint64;}]"));
    spec->Streams["input"] = stream;

    auto producer = New<TComputationSpec>();
    producer->ComputationClassName = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
    producer->OutputStreamIds = {"input"};
    producer->SourceStreams["source"] = New<TSourceSpec>();
    producer->SourceStreams["source"]->SourceClassName = "NYT::NFlow::TRandomSource";
    producer->Postprocess();
    spec->Computations["producer"] = producer;

    auto computation = New<TComputationSpec>();
    computation->ComputationClassName = "NYT::NFlow::TProcessFunctionComputation";
    computation->InputStreamIds = {"input"};
    computation->GroupBySchema = ConvertTo<NTableClient::TTableSchemaPtr>(
        TYsonStringBuf("[{name=key;type=uint64;sort_order=ascending;}]"));
    computation->ProcessFunctionGraph = New<TProcessFunctionGraphSpec>();
    auto addNode = [&] (const std::string& nodeId) {
        auto node = New<TProcessFunctionGraphNodeSpec>();
        node->ProcessingFunction = TypeName<TSpecUpdateDiscardFunction>();
        node->InputStreamIds = {"input"};
        computation->ProcessFunctionGraph->Nodes[nodeId] = node;
    };
    addNode("retained");
    if (includeRemovedNode) {
        addNode("removed");
    }
    spec->Computations["graph"] = computation;
    ResolveProcessFunctionGraphs(spec);
    ValidatePipelineSpec(spec);
    return spec;
}

TDynamicPipelineSpecPtr MakeGraphDynamicSpec(TStringBuf nodeId)
{
    auto spec = New<TDynamicPipelineSpec>();
    auto computation = New<TDynamicComputationSpec>();
    auto parameters = New<TProcessFunctionGraphDynamicParameters>();
    parameters->Nodes[std::string(nodeId)] = ConvertTo<IMapNodePtr>(TYsonStringBuf("{}"));
    computation->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(parameters);
    spec->Computations["graph"] = computation;
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

class TFlowExecutorSpecUpdateTest
    : public ::testing::TestWithParam<bool>
{
protected:
    TActionQueuePtr Queue_;
    const IVersionProviderPtr VersionProvider_ = New<TFakeVersionProvider>();
    const TFlowViewKeeperPtr Keeper_ = New<TFlowViewKeeper>();
    const TFlowStatePtr State_ = New<TFlowState>();
    TVersionedPipelineSpecPtr Spec_ = New<TVersionedPipelineSpec>();
    TVersionedDynamicPipelineSpecPtr DynamicSpec_ = New<TVersionedDynamicPipelineSpec>();
    IFlowExecutorPtr Executor_;
    TPromise<void> PersistStarted_ = NewPromise<void>();
    TPromise<void> ResumePersist_ = NewPromise<void>();
    bool BlockNextPersist_ = false;
    int PersistCount_ = 0;

    void SetUp() override
    {
        Queue_ = New<TActionQueue>("SpecUpdateTest");
        Spec_->TrySetValue(MakeGraphPipelineSpec(/*includeRemovedNode*/ true), VersionProvider_);
        State_->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Stopped, VersionProvider_);
        SyncFlowView();

        auto controller = New<StrictMock<TSpecUpdateControllerMock>>();
        EXPECT_CALL(*controller, GetFlowViewKeeper()).WillRepeatedly(Return(Keeper_));
        EXPECT_CALL(*controller, GetNodeInfo()).WillRepeatedly(Return(New<TNodeInfo>()));
        EXPECT_CALL(*controller, GetVersionProvider()).WillRepeatedly(Return(VersionProvider_));

        auto connector = New<StrictMock<TMockYTConnector>>();
        EXPECT_CALL(*connector, GetClient()).WillRepeatedly(Return(NApi::IClientPtr{}));
        EXPECT_CALL(*connector, GetPipelinePath()).WillRepeatedly(Return(NYPath::TRichYPath("//pipeline")));

        auto stateManager = New<StrictMock<TMockPersistedStateManager>>();
        EXPECT_CALL(*stateManager, RecoverSpec()).WillRepeatedly([this] {
            return CloneYsonStruct(Spec_);
        });
        EXPECT_CALL(*stateManager, RecoverDynamicSpec()).WillRepeatedly([this] {
            return CloneYsonStruct(DynamicSpec_);
        });
        EXPECT_CALL(*stateManager, RecoverFlowCoreTarget())
            .WillRepeatedly(Return(New<TVersionedFlowCoreTarget>()));
        EXPECT_CALL(*stateManager, PersistSpecs(_, _, _, _)).WillRepeatedly([this] (const std::optional<TVersionedPipelineSpecPtr>& spec, const std::optional<TPipelineImportantVersionsPtr>& expectedVersions, const std::optional<TVersionedDynamicPipelineSpecPtr>& dynamicSpec, const std::optional<TVersion>& expectedDynamicSpecVersion) {
            if (BlockNextPersist_) {
                BlockNextPersist_ = false;
                PersistStarted_.Set();
                WaitFor(ResumePersist_.ToFuture())
                    .ThrowOnError();
            }
            if (spec) {
                expectedVersions.value()->EnsureEqual(*MakePipelineImportantVersions(State_, Spec_));
            }
            if (dynamicSpec) {
                THROW_ERROR_EXCEPTION_UNLESS(
                    expectedDynamicSpecVersion == DynamicSpec_->GetVersion(),
                    NFlow::EErrorCode::SpecVersionMismatch,
                    "Dynamic spec version mismatch");
            }
            if (spec) {
                Spec_ = CloneYsonStruct(*spec);
            }
            if (dynamicSpec) {
                DynamicSpec_ = CloneYsonStruct(*dynamicSpec);
            }
            ++PersistCount_;
        });

        auto config = New<TControllerServiceConfig>();
        config->SetSpecRetryCount = 1;
        Executor_ = CreateFlowExecutor(
            controller,
            stateManager,
            connector,
            config,
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            Queue_->GetInvoker());
    }

    void TearDown() override
    {
        Queue_->Shutdown();
    }

    void SyncFlowView()
    {
        State_->ExecutionSpec->PipelineSpec = Spec_;
        State_->ExecutionSpec->DynamicPipelineSpec = DynamicSpec_;
        Keeper_->Init(State_, New<TFlowEphemeralState>(), Spec_, DynamicSpec_);
    }

    void UpdateDynamicSpec(const TDynamicPipelineSpecPtr& spec)
    {
        if (GetParam()) {
            TSetPipelineSpecsArg argument;
            argument.DynamicSpec = ConvertToNode(spec);
            argument.ValidateStrict = false;
            Executor_->SetPipelineSpecs(argument);
        } else {
            TSetPipelineDynamicSpecArg argument;
            argument.Spec = ConvertToNode(spec);
            Executor_->SetPipelineDynamicSpec(argument);
        }
    }

    void RunConcurrentUpdates(bool staticFirst)
    {
        WaitFor(BIND([this, staticFirst] {
            auto staticUpdate = BIND([this] {
                TSetPipelineSpecArg argument;
                argument.Spec = ConvertToNode(MakeGraphPipelineSpec(/*includeRemovedNode*/ false));
                Executor_->SetPipelineSpec(argument);
            });
            auto dynamicUpdate = BIND([this] {
                UpdateDynamicSpec(MakeGraphDynamicSpec("removed"));
            });

            BlockNextPersist_ = true;
            auto first = (staticFirst ? staticUpdate : dynamicUpdate)
                .AsyncVia(Queue_->GetInvoker())
                .Run();
            WaitFor(PersistStarted_.ToFuture().WithTimeout(TDuration::Seconds(5)))
                .ThrowOnError();

            auto second = (staticFirst ? dynamicUpdate : staticUpdate)
                .AsyncVia(Queue_->GetInvoker())
                .Run();
            // A barrier on the same queue lets the second request run until it yields.
            WaitFor(BIND([] {
            }).AsyncVia(Queue_->GetInvoker())
                    .Run())
                .ThrowOnError();
            EXPECT_FALSE(second.IsSet());

            ResumePersist_.Set();
            WaitFor(first.WithTimeout(TDuration::Seconds(5)))
                .ThrowOnError();
            auto error = WaitFor(second.WithTimeout(TDuration::Seconds(5)));
            EXPECT_FALSE(error.IsOK());
            EXPECT_THAT(ToString(error), HasSubstr("Unknown process function graph node \"removed\""));
            EXPECT_EQ(PersistCount_, 1);
            EXPECT_TRUE(AreNodesEqual(ConvertToNode(Spec_->GetValue()), ConvertToNode(MakeGraphPipelineSpec(!staticFirst))));
            EXPECT_TRUE(AreNodesEqual(
                ConvertToNode(DynamicSpec_->GetValue()),
                ConvertToNode(staticFirst ? New<TDynamicPipelineSpec>() : MakeGraphDynamicSpec("removed"))));
            EXPECT_NO_THROW(ValidateProcessFunctionGraphDynamicSpec(Spec_->GetValue(), DynamicSpec_->GetValue()));

            // Simulate the controller observing the committed specs before the next update.
            SyncFlowView();
            UpdateDynamicSpec(MakeGraphDynamicSpec("retained"));
            EXPECT_EQ(PersistCount_, 2);
            EXPECT_NO_THROW(ValidateProcessFunctionGraphDynamicSpec(Spec_->GetValue(), DynamicSpec_->GetValue()));
        }).AsyncVia(Queue_->GetInvoker())
                .Run()
                .WithTimeout(TDuration::Seconds(20)))
            .ThrowOnError();
    }
};

TEST_P(TFlowExecutorSpecUpdateTest, DynamicUpdateRevalidatesAfterStaticUpdate)
{
    RunConcurrentUpdates(/*staticFirst*/ true);
}

TEST_P(TFlowExecutorSpecUpdateTest, StaticUpdateRevalidatesAfterDynamicUpdate)
{
    RunConcurrentUpdates(/*staticFirst*/ false);
}

INSTANTIATE_TEST_SUITE_P(
    DynamicSpecApi,
    TFlowExecutorSpecUpdateTest,
    ::testing::Bool(),
    [] (const ::testing::TestParamInfo<bool>& info) {
        return info.param ? "Combined" : "Separate";
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
