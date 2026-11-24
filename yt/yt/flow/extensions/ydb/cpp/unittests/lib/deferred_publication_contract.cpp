#include "deferred_publication_contract.h"

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NYdbTesting {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto PreparationAttemptTimeout = TDuration::Seconds(5);
constexpr auto LongDuration = TDuration::Hours(1);
constexpr int SimultaneousMessageCount = 20;

const TYdbWriteTarget ClusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
const TYdbWriteTarget ClusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};

struct TFederatedClients
{
    TFakeDeferredPublicationClientPtr A = New<TFakeDeferredPublicationClient>();
    TFakeDeferredPublicationClientPtr B = New<TFakeDeferredPublicationClient>();
};

TFederatedClients MakeFederated(const TSinkTestEnvironment& environment)
{
    TFederatedClients clients;
    environment.Resource->PublicationClients[{ClusterA.Endpoint, ClusterA.Database}] = clients.A;
    environment.Resource->PublicationClients[{ClusterB.Endpoint, ClusterB.Database}] = clients.B;
    environment.Resource->Resolutions = {
        {.Target = ClusterA, .Federated = true},
        {.Target = ClusterB, .Federated = true},
    };
    return clients;
}

TDynamicSinkContextPtr MakeDynamicContext(const std::vector<std::pair<std::string, INodePtr>>& parameters)
{
    auto context = New<TDynamicSinkContext>();
    context->DynamicSinkSpec = New<TDynamicSinkSpec>();
    for (const auto& [name, value] : parameters) {
        context->DynamicSinkSpec->Parameters->AddChild(name, CloneNode(value));
    }
    return context;
}

size_t WriteThreeMessagesInOneEpoch(const TSinkTestEnvironment& environment, const ISinkPtr& sink, i64 firstId)
{
    const auto writesBefore = environment.WriteState->Writes.size();
    for (i64 id = firstId; id < firstId + 3; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink);
    sink->Commit();
    return environment.WriteState->Writes.size() - writesBefore;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void CheckCheckpointIsWrittenBeforeDistributionCallbacks(const TSinkSpecShape& shape)
{
    TSinkTestEnvironment environment(shape, /*framing*/ false);
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    const auto lastMessage = environment.MakeMessage(2, "second");
    std::vector<std::pair<TMessageId, i64>> checkpointsSeenByCallbacks;
    auto recordCheckpoint = [&] {
        environment.StateManager->Sync();
        auto state = ConvertToNode(environment.StateManager->Get("/v0"))->AsMap();
        checkpointsSeenByCallbacks.emplace_back(
            state->GetChildValueOrThrow<TMessageId>("max_persisted_message_id"),
            state->GetChildValueOrThrow<i64>("max_persisted_seq_no"));
    };

    auto firstTracker = TDistributingTracker(recordCheckpoint);
    sink->Distribute(environment.MakeMessage(1, "first"), firstTracker.AddDestination());
    firstTracker.Activate();
    auto lastTracker = TDistributingTracker(recordCheckpoint);
    sink->Distribute(lastMessage, lastTracker.AddDestination());
    lastTracker.Activate();
    EXPECT_TRUE(checkpointsSeenByCallbacks.empty());

    sink->PreSyncFlush();

    const std::pair<TMessageId, i64> expected(lastMessage->MessageId, 2);
    EXPECT_EQ(checkpointsSeenByCallbacks, (std::vector<std::pair<TMessageId, i64>>{expected, expected}));
}

void CheckQueuedReconfigurationsApplyTheirOwnBatching(
    const TSinkSpecShape& shape,
    const std::string& parameterName,
    i64 valueA,
    size_t writesUnderA,
    i64 valueB,
    size_t writesUnderB)
{
    TSinkTestEnvironment environment(shape, /*framing*/ true, /*maxRowsPerBatch*/ 1000, 10, /*asyncInvoker*/ true);
    auto invoker = environment.SuspendableInvoker;
    auto sink = WaitFor(BIND([&] {
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        return sink;
    }).AsyncVia(invoker)
            .Run())
        .ValueOrThrow();

    auto writesUnderInitialOptions = BIND(&WriteThreeMessagesInOneEpoch, std::cref(environment), sink, 1)
        .AsyncVia(invoker)
        .Run();
    EXPECT_EQ(WaitFor(writesUnderInitialOptions).ValueOrThrow(), 1u);

    WaitFor(invoker->Suspend()).ThrowOnError();
    sink->Reconfigure(MakeDynamicContext({{parameterName, ConvertToNode(valueA)}}));
    auto writesAfterA = BIND(&WriteThreeMessagesInOneEpoch, std::cref(environment), sink, 4)
        .AsyncVia(invoker)
        .Run();
    auto contextB = MakeDynamicContext({{parameterName, ConvertToNode(valueB)}});
    sink->Reconfigure(contextB);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicContext({{parameterName, ConvertToNode("invalid")}})), std::exception);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicContext({{parameterName, ConvertToNode(0)}})), std::exception);
    EXPECT_EQ(DynamicPointerCast<TSinkBase>(sink)->GetDynamicContext(), contextB);
    invoker->Resume();

    EXPECT_EQ(WaitFor(writesAfterA).ValueOrThrow(), writesUnderA);
    auto writesAfterB = BIND(&WriteThreeMessagesInOneEpoch, std::cref(environment), sink, 7)
        .AsyncVia(invoker)
        .Run();
    EXPECT_EQ(WaitFor(writesAfterB).ValueOrThrow(), writesUnderB);

    WaitFor(BIND([&] {
        sink.Reset();
    }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();
}

void CheckRejectedPreparationTimeoutKeepsAcceptedOptions(const TSinkSpecShape& shape)
{
    TSinkTestEnvironment environment(shape, /*framing*/ false);
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
        "preparation_attempt_timeout",
        ConvertToNode(LongDuration));
    auto clients = MakeFederated(environment);
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 1;
    environment.WriteState->SecondNotifyAfterWriteCount = 2;

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto accepted = MakeDynamicContext({
        {"preparation_attempt_timeout", ConvertToNode(TDuration::MilliSeconds(200))},
        {"preparation_target_backoff", ConvertToNode(LongDuration)},
    });
    sink->Reconfigure(accepted);
    for (const auto& rejectedTimeout : {ConvertToNode("invalid"), ConvertToNode(TDuration::Zero())}) {
        EXPECT_THROW(
            sink->Reconfigure(MakeDynamicContext({{"preparation_attempt_timeout", rejectedTimeout}})),
            std::exception);
        EXPECT_EQ(DynamicPointerCast<TSinkBase>(sink)->GetDynamicContext(), accepted);
    }

    auto callbackCount = environment.Distribute(sink, environment.MakeMessage(1, "payload"));
    auto queue = New<TActionQueue>("RejectedTimeoutTest");
    auto prepared = BIND([sink] {
        sink->PreSyncFlush();
    }).AsyncVia(queue->GetInvoker())
        .Run();
    WaitFor(environment.WriteState->WriteCountReached.ToFuture()
            .WithTimeout(TDuration::Seconds(60)))
        .ThrowOnError();
    WaitFor(environment.WriteState->SecondWriteCountReached.ToFuture()
            .WithTimeout(TDuration::Seconds(60)))
        .ThrowOnError();
    WaitFor(BIND([state = environment.WriteState] {
        state->AcknowledgeAll();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    WaitFor(prepared).ThrowOnError();

    ASSERT_EQ(environment.Resource->WriterTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets[0], std::pair(ClusterA.Endpoint, ClusterA.Database));
    EXPECT_EQ(environment.Resource->WriterTargets[1], std::pair(ClusterB.Endpoint, ClusterB.Database));
    EXPECT_EQ(*callbackCount, 1);
}

void CheckPreparationAttemptUsesOneOptionsSnapshot(const TSinkSpecShape& shape)
{
    TSinkTestEnvironment environment(shape, /*framing*/ true, /*maxRowsPerBatch*/ 1);
    auto parameters = environment.DynamicContext->DynamicSinkSpec->Parameters;
    parameters->AddChild("preparation_attempt_timeout", ConvertToNode(PreparationAttemptTimeout));
    parameters->AddChild("preparation_target_backoff", ConvertToNode(LongDuration));
    auto clients = MakeFederated(environment);
    clients.A->PendingBegin = NewPromise<NYdb::NTopic::TBeginPublicationResult>();
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 2;
    environment.WriteState->SecondNotifyAfterWriteCount = 3;

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "first"));
    environment.Distribute(sink, environment.MakeMessage(2, "second"));

    auto queue = New<TActionQueue>("PreparationSnapshotTest");
    auto prepared = BIND([sink] {
        sink->PreSyncFlush();
    }).AsyncVia(queue->GetInvoker())
        .Run();

    auto beginIssued = BIND([&clients] {
        return !clients.A->BeginCalls.empty();
    }).AsyncVia(queue->GetInvoker());
    const auto beginDeadline = TInstant::Now() + TDuration::Seconds(4);
    while (!WaitFor(beginIssued.Run()).ValueOrThrow()) {
        ASSERT_LT(TInstant::Now(), beginDeadline);
        Sleep(TDuration::MilliSeconds(10));
    }

    WaitFor(BIND([&] {
        sink->Reconfigure(MakeDynamicContext({
            {"max_rows_per_batch", ConvertToNode(100)},
            {"preparation_attempt_timeout", ConvertToNode(LongDuration)},
            {"preparation_target_backoff", ConvertToNode(TDuration::Zero())},
        }));
        clients.A->PendingBegin->Set(NYdb::NTopic::TBeginPublicationResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{}),
            NYdb::NTopic::TDeferredPublication(100, environment.GetExternalPublicationId())));
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();

    WaitFor(environment.WriteState->WriteCountReached.ToFuture()
            .WithTimeout(TDuration::Seconds(60)))
        .ThrowOnError();
    WaitFor(environment.WriteState->SecondWriteCountReached.ToFuture()
            .WithTimeout(TDuration::Seconds(60)))
        .ThrowOnError();
    WaitFor(BIND([state = environment.WriteState] {
        state->AcknowledgeAll();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    WaitFor(prepared).ThrowOnError();

    EXPECT_EQ(environment.WriteState->Writes.size(), 3u);
    ASSERT_EQ(environment.Resource->WriterTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets[1], std::pair(ClusterB.Endpoint, ClusterB.Database));
    ASSERT_GE(environment.Resource->ResolutionExcludedTargets.size(), 2u);
    EXPECT_TRUE(environment.Resource->ResolutionExcludedTargets[1].contains(ClusterA));
}

void CheckSimultaneousComputationsUseTheirOwnResources(const TSinkSpecShape& shape)
{
    TSinkTestEnvironment left(shape, /*framing*/ false);
    TSinkTestEnvironment right(shape, /*framing*/ false);
    left.Context->SinkId = TSinkId("left");
    right.Context->SinkId = TSinkId("right");
    ASSERT_NE(left.Resource, right.Resource);

    auto runComputation = [] (const TSinkTestEnvironment& environment, const std::string& payloadPrefix) {
        auto queue = New<TActionQueue>(payloadPrefix);
        auto finished = BIND([&environment, payloadPrefix] {
            auto sink = environment.CreateSink();
            sink->Init(environment.StateManager->CreateContext());
            for (int id = 1; id <= SimultaneousMessageCount; ++id) {
                environment.Distribute(sink, environment.MakeMessage(id, Format("%v-%v", payloadPrefix, id)));
                Yield();
            }
            environment.Sync(sink);
            sink->Commit();
        }).AsyncVia(queue->GetInvoker())
            .Run();
        return std::pair(std::move(queue), std::move(finished));
    };
    auto [leftQueue, leftFinished] = runComputation(left, "left");
    auto [rightQueue, rightFinished] = runComputation(right, "right");
    WaitFor(AllSucceeded(std::vector{leftFinished, rightFinished})).ThrowOnError();

    auto checkOwnWrites = [] (const TSinkTestEnvironment& environment, const std::string& payloadPrefix) {
        ASSERT_EQ(std::ssize(environment.WriteState->Writes), SimultaneousMessageCount);
        for (int index = 0; index < SimultaneousMessageCount; ++index) {
            EXPECT_EQ(environment.WriteState->Writes[index].Data, Format("%v-%v", payloadPrefix, index + 1));
        }
        ASSERT_EQ(environment.DeferredClient->BeginCalls.size(), 1u);
        EXPECT_EQ(environment.DeferredClient->BeginCalls[0].ExternalPublicationId, environment.GetExternalPublicationId());
        EXPECT_EQ(environment.DeferredClient->PublishCalls.size(), 1u);
        EXPECT_EQ(environment.Resource->WriterCreationCount, 1);
    };
    checkOwnWrites(left, "left");
    checkOwnWrites(right, "right");
    EXPECT_NE(left.GetExternalPublicationId(), right.GetExternalPublicationId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
