#include <yt/yt/flow/library/cpp/pipeline_helpers/pipeline.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StartsWith;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitPipelineTest, FailsOnceTheVanillaOperationIsTerminal)
{
    auto client = New<StrictMock<TMockClient>>();
    auto operationId = NScheduler::TOperationId(TGuid::FromString("1-2-3-4"));

    // The controller log tail opens on the log table's row count.
    EXPECT_CALL(*client, GetTabletInfos(_, _, _))
        .WillOnce(Return(MakeFuture(std::vector<TTabletInfo>{TTabletInfo{}})));
    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillRepeatedly(Return(MakeFuture<TPipelineState>(TError("Cannot connect to pipeline controller leader"))));
    EXPECT_CALL(*client, GetOperation(NScheduler::TOperationIdOrAlias{operationId}, _))
        .WillOnce([] (const NScheduler::TOperationIdOrAlias&, const TGetOperationOptions&) {
            TOperation operation;
            operation.State = NScheduler::EOperationState::Aborted;
            return MakeFuture(operation);
        });

    EXPECT_THROW_WITH_SUBSTRING(
        WaitPipeline(
            client,
            NYPath::TRichYPath("//tmp/pipeline"),
            TDuration::Hours(1),
            TVanillaOperationHandle{.Client = client, .OperationId = operationId}),
        "Vanilla operation 1-2-3-4 is aborted");
}

TEST(TRunPipelineTest, FailsOnceTheVanillaOperationIsTerminal)
{
    auto client = New<StrictMock<TMockClient>>();
    auto operationId = NScheduler::TOperationId(TGuid::FromString("1-2-3-4"));

    EXPECT_CALL(*client, NodeExists("//tmp/pipeline", _))
        .WillRepeatedly(Return(MakeFuture(true)));
    EXPECT_CALL(*client, GetTabletInfos(_, _, _))
        .WillOnce(Return(MakeFuture(std::vector<TTabletInfo>{TTabletInfo{}})));
    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillRepeatedly(Return(MakeFuture<TPipelineState>(TError("Cannot connect to pipeline controller leader"))));
    EXPECT_CALL(*client, GetOperation(NScheduler::TOperationIdOrAlias{operationId}, _))
        .WillOnce([] (const NScheduler::TOperationIdOrAlias&, const TGetOperationOptions&) {
            TOperation operation;
            operation.State = NScheduler::EOperationState::Failed;
            return MakeFuture(operation);
        });

    EXPECT_THROW_WITH_SUBSTRING(
        RunPipeline(
            client,
            "//tmp/pipeline",
            New<TPipelineSpec>(),
            New<TDynamicPipelineSpec>(),
            /*setFlowCoreTarget*/ false,
            /*graceful*/ true,
            TDuration::Hours(1),
            /*enablePipelineCreation*/ false,
            /*enablePipelineStopOrPause*/ true,
            TVanillaOperationHandle{.Client = client, .OperationId = operationId}),
        "Vanilla operation 1-2-3-4 is failed");
}

TEST(TWaitPipelineTest, KeepsWaitingWhenTheVanillaOperationLookupFails)
{
    auto client = New<StrictMock<TMockClient>>();
    auto operationId = NScheduler::TOperationId(TGuid::FromString("1-2-3-4"));

    EXPECT_CALL(*client, GetTabletInfos(_, _, _))
        .WillOnce(Return(MakeFuture(std::vector<TTabletInfo>{TTabletInfo{}})));

    InSequence sequence;
    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce(Return(MakeFuture<TPipelineState>(TError("Cannot connect to pipeline controller leader"))));
    EXPECT_CALL(*client, GetOperation(NScheduler::TOperationIdOrAlias{operationId}, _))
        .WillOnce(Return(MakeFuture<TOperation>(TError("Scheduler is unavailable"))));
    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce(Return(MakeFuture(TPipelineState{.State = EPipelineState::Completed})));

    WaitPipeline(
        client,
        NYPath::TRichYPath("//tmp/pipeline"),
        TDuration::Hours(1),
        TVanillaOperationHandle{.Client = client, .OperationId = operationId});
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitPipelineStateTest, PassesExplicitRequestTimeout)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_TRUE(options.Timeout);
            EXPECT_EQ(options.Timeout.value_or(TDuration::Zero()), TDuration::Seconds(1));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1),
        TDuration::Seconds(1));
}

TEST(TWaitPipelineStateTest, UsesDefaultRequestTimeout)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_TRUE(options.Timeout);
            EXPECT_EQ(options.Timeout.value_or(TDuration::Zero()), TDuration::Seconds(60));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1));
}

TEST(TWaitPipelineStateTest, ClampsRequestTimeoutToRemainingWaitBudget)
{
    auto client = New<StrictMock<TMockClient>>();
    const auto waitTimeout = TDuration::Hours(1);
    std::optional<TDuration> actualTimeout;

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([&] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            actualTimeout = options.Timeout;
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    const auto started = TInstant::Now();
    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        waitTimeout,
        TDuration::Days(1));
    const auto elapsed = TInstant::Now() - started;

    ASSERT_TRUE(actualTimeout);
    EXPECT_LE(*actualTimeout, waitTimeout);
    EXPECT_GE(*actualTimeout, waitTimeout - elapsed);
}

TEST(TWaitPipelineStateTest, RetriesFailedRequest)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(2)
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_EQ(options.Timeout, TDuration::Seconds(1));
            return MakeFuture<TPipelineState>(TError("Transient state request failure"));
        })
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_EQ(options.Timeout, TDuration::Seconds(1));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1),
        TDuration::Seconds(1));
}

TEST(TWaitPipelineStateTest, PropagatesLastErrorAfterRetryLimit)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(10)
        .WillRepeatedly([] (const NYPath::TYPath&, const TGetPipelineStateOptions&) {
            return MakeFuture<TPipelineState>(TError("Persistent state request failure"));
        });

    EXPECT_THROW_WITH_SUBSTRING(
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::Hours(1),
            TDuration::Seconds(1)),
        "Persistent state request failure");
}

TEST(TWaitPipelineStateTest, AttachesLastErrorWhenWaitDeadlineExpires)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            Sleep(options.Timeout.value_or(TDuration::Zero()) + TDuration::MilliSeconds(10));
            return MakeFuture<TPipelineState>(TError("State request failed at the deadline"));
        });

    try {
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::MilliSeconds(500),
            TDuration::Seconds(1));
        ADD_FAILURE() << "WaitPipelineState did not throw";
    } catch (const TErrorException& ex) {
        EXPECT_THAT(ex.Error().GetMessage(), StartsWith("Timed out after"));
        ASSERT_EQ(ex.Error().InnerErrors().size(), 1u);
        EXPECT_EQ(ex.Error().InnerErrors()[0].GetMessage(), "State request failed at the deadline");
    }
}

TEST(TWaitPipelineStateTest, ZeroWaitDoesNotIssueRequest)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(0);

    EXPECT_THROW_WITH_SUBSTRING(
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::Zero()),
        "Timed out after 0 seconds waiting for pipeline state \"stopped\"; "
        "the pipeline is still in state \"unknown\"");
}

////////////////////////////////////////////////////////////////////////////////

TError WaitPipelineStateUntilTimeout(EPipelineState targetState, EPipelineState observedState)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillRepeatedly(Return(MakeFuture(TPipelineState{.State = observedState})));

    try {
        WaitPipelineState(client, "//tmp/pipeline", targetState, TDuration::Seconds(2));
    } catch (const TErrorException& ex) {
        return ex.Error();
    }
    ADD_FAILURE() << "WaitPipelineState did not throw";
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitPipelineStateTest, ReportsStatesAndTimeoutWhenWaitDeadlineExpires)
{
    auto error = WaitPipelineStateUntilTimeout(EPipelineState::Working, EPipelineState::Pausing);

    EXPECT_EQ(
        error.GetMessage(),
        "Timed out after 2 seconds waiting for pipeline state \"working\"; "
        "the pipeline is still in state \"pausing\"");
    EXPECT_EQ(error.Attributes().Get<std::string>("target_state"), "working");
    EXPECT_EQ(error.Attributes().Get<std::string>("last_observed_state"), "pausing");
    EXPECT_EQ(error.Attributes().Get<TDuration>("timeout"), TDuration::Seconds(2));
}

TEST(TWaitPipelineStateTest, HintsAtNonGracefulUpdateWhenDrainStalls)
{
    auto error = WaitPipelineStateUntilTimeout(EPipelineState::Stopped, EPipelineState::Draining);

    EXPECT_EQ(
        error.GetMessage(),
        "Timed out after 2 seconds waiting for pipeline state \"stopped\"; "
        "the pipeline is still in state \"draining\"; "
        "if it cannot drain (for example, its jobs fail every epoch), "
        "set YT_FLOW_GRACEFUL_UPDATE=0 to pause the pipeline instead of stopping it; "
        "see the hotfix constraints in the release documentation before doing so");
}

TEST(TWaitPipelineStateTest, NoHintWhenDrainingIsNotBlockingStop)
{
    // The post-start wait can also observe Draining, but there pausing is not the escape.
    auto error = WaitPipelineStateUntilTimeout(EPipelineState::Working, EPipelineState::Draining);

    EXPECT_EQ(
        error.GetMessage(),
        "Timed out after 2 seconds waiting for pipeline state \"working\"; "
        "the pipeline is still in state \"draining\"");
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TRichYPath MakePipelinePath()
{
    NYPath::TRichYPath path("//tmp/pipeline");
    path.SetCluster("primary");
    return path;
}

TIntrusivePtr<StrictMock<TMockClient>> MakeClientWithEmptyLog()
{
    auto client = New<StrictMock<TMockClient>>();
    // The controller log reader opens by reading the log table's total row count.
    EXPECT_CALL(*client, GetTabletInfos(_, _, _))
        .WillRepeatedly(Return(MakeFuture(std::vector<TTabletInfo>{{}})));

    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterName("data");
    auto emptyLogBatch = NQueueClient::CreateQueueRowset(
        CreateRowset(nameTable, TSharedRange<NTableClient::TUnversionedRow>()),
        /*startOffset*/ 0);
    EXPECT_CALL(*client, PullQueue(_, _, _, _, _))
        .WillRepeatedly(Return(MakeFuture(emptyLogBatch)));
    return client;
}

TFuture<TPipelineState> MakeUnavailableFuture()
{
    return MakeFuture<TPipelineState>(TError("Controller is unavailable"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitPipelineTest, DetachesWhenControllerStaysUnreachable)
{
    auto client = MakeClientWithEmptyLog();
    EXPECT_CALL(*client, GetPipelineState(_, _))
        .WillRepeatedly(Return(MakeUnavailableFuture()));

    auto startInstant = TInstant::Now();
    WaitPipeline(client, MakePipelinePath(), /*controllerUnavailableTimeout*/ TDuration::MilliSeconds(200));
    EXPECT_LT(TInstant::Now() - startInstant, TDuration::Seconds(30));
}

TEST(TWaitPipelineTest, TransientFailureDoesNotDetach)
{
    auto client = MakeClientWithEmptyLog();
    // Without the streak reset on the successful poll the second failure at ~300 ms would
    // exceed the 200 ms budget and the wait would detach before Completed.
    InSequence sequence;
    EXPECT_CALL(*client, GetPipelineState(_, _))
        .WillOnce(Return(MakeUnavailableFuture()));
    EXPECT_CALL(*client, GetPipelineState(_, _))
        .WillOnce(Return(MakeFuture(TPipelineState{.State = EPipelineState::Working})));
    EXPECT_CALL(*client, GetPipelineState(_, _))
        .WillOnce(Return(MakeUnavailableFuture()));
    EXPECT_CALL(*client, GetPipelineState(_, _))
        .WillOnce(Return(MakeFuture(TPipelineState{.State = EPipelineState::Completed})));

    WaitPipeline(client, MakePipelinePath(), /*controllerUnavailableTimeout*/ TDuration::MilliSeconds(200));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
