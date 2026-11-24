#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/deferred_publication_contract.h>
#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/sink_test_environment.h>

#include <yt/yt/flow/extensions/ydb/cpp/deferred_publication.h>
#include <yt/yt/flow/extensions/ydb/cpp/options.h>
#include <yt/yt/flow/extensions/ydb/cpp/sink.h>
#include <yt/yt/flow/extensions/ydb/cpp/sink_engine.h>
#include <yt/yt/flow/extensions/ydb/cpp/state_access.h>
#include <yt/yt/flow/extensions/ydb/cpp/two_phase_commit_engine.h>
#include <yt/yt/flow/extensions/ydb/cpp/two_phase_commit_sink.h>
#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYdbTesting;
using NYdb::NTopic::TDeferredPublication;

////////////////////////////////////////////////////////////////////////////////

TSinkSpecShape PublicSinkShape()
{
    return {
        .SinkClassName = TypeName<TYdbTwoPhaseCommitSink>(),
        .ResourceClassName = TypeName<TYdbTopicDriver>(),
        .ConnectionParameters = {
            {"endpoint", NYTree::ConvertToNode("localhost:2135")},
            {"database", NYTree::ConvertToNode("/Root")},
            {"topic", NYTree::ConvertToNode("topic")},
        },
        .WriterIdentityType = "ytflow-ydb-two-phase-commit",
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYdbWriterEventTest, KeepsResourceUntilWriterIsDestroyed)
{
    TSinkTestEnvironment environment(PublicSinkShape(), /*framing*/ false);
    auto resource = MakeWeak(environment.Resource);
    auto writer = CreateYdbWriter(
        environment.Resource,
        /*topicClient*/ nullptr,
        NYdb::NTopic::TWriteSessionSettings(),
        GetNullInvoker(),
        TestLogger,
        CreateSyncStatusProfiler(TestLogger));
    environment.Context->StaticResources.clear();
    environment.Resource.Reset();
    EXPECT_TRUE(resource.Lock());
    writer.Reset();
    EXPECT_FALSE(resource.Lock());
}

TEST(TYdbWriterEventTest, LegacyDiscardRetainsMessageForRetry)
{
    auto writer = New<TControllableRetryableYdbWriter>();
    auto firstFuture = writer->Write({.SeqNo = 1, .Data = "one"});
    auto secondFuture = writer->Write({.SeqNo = 2, .Data = "two"});

    EXPECT_THROW_WITH_SUBSTRING(
        writer->ApplyAck(
            1,
            NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_DISCARDED),
        "permanently rejected write");

    EXPECT_FALSE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());
    EXPECT_EQ(writer->GetInflightSeqNos(), (std::vector<i64>{1, 2}));

    writer->ApplyAck(
        1,
        NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN);
    writer->ApplyAck(
        2,
        NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN);
    EXPECT_TRUE(NConcurrency::WaitFor(firstFuture).IsOK());
    EXPECT_TRUE(NConcurrency::WaitFor(secondFuture).IsOK());
    EXPECT_TRUE(writer->GetInflightSeqNos().empty());
}

TEST(TYdbWriterEventTest, DeferredDiscardFailsOnlyExactSeqNo)
{
    auto writer = New<TControllableRetryableYdbWriter>();
    auto firstFuture = writer->Write({
        .SeqNo = 1,
        .Data = "one",
        .DeferredPublication = TDeferredPublication(10, "publication"),
    });
    auto secondFuture = writer->Write({
        .SeqNo = 2,
        .Data = "two",
        .DeferredPublication = TDeferredPublication(10, "publication"),
    });

    writer->ApplyAck(
        1,
        NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_DISCARDED);

    ASSERT_TRUE(firstFuture.IsSet());
    EXPECT_FALSE(NConcurrency::WaitFor(firstFuture).IsOK());
    EXPECT_FALSE(secondFuture.IsSet());
    EXPECT_EQ(writer->GetInflightSeqNos(), std::vector<i64>{2});
}

TEST(TYdbWriterEventTest, WrittenInTransactionCompletesWrite)
{
    auto writer = New<TControllableRetryableYdbWriter>();
    auto future = writer->Write({.SeqNo = 1, .Data = "one"});

    writer->ApplyAck(
        1,
        NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX);

    EXPECT_TRUE(NConcurrency::WaitFor(future).IsOK());
    EXPECT_TRUE(writer->GetInflightSeqNos().empty());
}

TEST(TYdbWriterEventTest, NoRetrySessionCloseRetainsLegacyInflight)
{
    auto writer = New<TControllableRetryableYdbWriter>();
    auto firstFuture = writer->Write({.SeqNo = 1, .Data = "one"});
    auto secondFuture = writer->Write({.SeqNo = 2, .Data = "two"});

    EXPECT_THROW_WITH_SUBSTRING(
        writer->ApplySessionClosed(NYdb::EStatus::BAD_REQUEST),
        "write session closed");

    EXPECT_FALSE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());
    EXPECT_EQ(writer->GetInflightSeqNos(), (std::vector<i64>{1, 2}));
}

TEST(TYdbWriterEventTest, NoRetrySessionCloseFailsDeferredInflight)
{
    auto writer = New<TControllableRetryableYdbWriter>();
    auto future = writer->Write({
        .SeqNo = 1,
        .Data = "one",
        .DeferredPublication = TDeferredPublication(10, "publication"),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        writer->ApplySessionClosed(NYdb::EStatus::BAD_REQUEST),
        "write session closed");

    ASSERT_TRUE(future.IsSet());
    EXPECT_FALSE(NConcurrency::WaitFor(future).IsOK());
    EXPECT_TRUE(writer->GetInflightSeqNos().empty());
}

TEST(TYdbWriterEventTest, RetryableSessionCloseRetainsMessagesForNextSession)
{
    for (const auto status : {NYdb::EStatus::UNAVAILABLE, NYdb::EStatus::OVERLOADED}) {
        auto writer = New<TControllableRetryableYdbWriter>();
        auto future = writer->Write({.SeqNo = 1, .Data = "one"});

        EXPECT_THROW_WITH_SUBSTRING(
            writer->ApplySessionClosed(status),
            "write session closed");

        EXPECT_FALSE(future.IsSet());
        EXPECT_EQ(writer->GetInflightSeqNos(), std::vector<i64>{1});

        writer->ApplyAck(
            1,
            NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN);
        EXPECT_TRUE(NConcurrency::WaitFor(future).IsOK());
        EXPECT_TRUE(writer->GetInflightSeqNos().empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDeferredPublicationSinkTest
    : public ::testing::TestWithParam<bool>
{ };

TEST_P(TDeferredPublicationSinkTest, EmptyEpochDoesNotOpenPublication)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    auto sink = environment.CreateSink();
    EXPECT_EQ(environment.Resource->WriterCreationCount, 0);
    EXPECT_EQ(environment.Resource->PublicationClientCreationCount, 0);
    sink->Init(environment.StateManager->CreateContext());
    for (int epoch = 0; epoch < 2; ++epoch) {
        environment.Sync(sink);
        sink->Commit();
    }
    EXPECT_TRUE(environment.DeferredClient->Actions.empty());
    EXPECT_TRUE(environment.WriteState->Writes.empty());
    EXPECT_EQ(environment.Resource->WriterCreationCount, 0);
}

TEST_P(TDeferredPublicationSinkTest, PublishesInCommitAfterPersistingWholeWave)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 2);
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    std::vector<std::shared_ptr<int>> callbacks;
    for (int id = 1; id <= 3; ++id) {
        callbacks.push_back(environment.Distribute(sink, environment.MakeMessage(id, "payload")));
    }
    EXPECT_TRUE(environment.WriteState->Writes.empty());
    sink->PreSyncFlush();
    for (const auto& callback : callbacks) {
        EXPECT_EQ(*callback, 1);
    }
    EXPECT_EQ(environment.WriteState->Writes.size(), GetParam() ? 2u : 3u);
    environment.Sync(sink);
    auto cookie = *environment.GetPersistedDeferredPublication()->IntPublicationId;
    EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
    ASSERT_EQ(environment.DeferredClient->BeginCalls.size(), 1u);
    EXPECT_EQ(environment.DeferredClient->ListCallCount, 0);
    ASSERT_EQ(environment.WriteState->Writes.size(), GetParam() ? 2u : 3u);
    for (const auto& write : environment.WriteState->Writes) {
        ASSERT_TRUE(write.DeferredPublication);
        EXPECT_EQ(write.DeferredPublication->IntPublicationId, cookie);
    }
    EXPECT_EQ(sink->BuildInflight().at(SinkTestStreamId)->InflightMetrics->Count, 3);
    environment.Sync(sink);
    for (const auto& callback : callbacks) {
        EXPECT_EQ(*callback, 1);
    }
    ASSERT_EQ(environment.DeferredClient->BeginCalls.size(), 1u);
    sink->Commit();
    EXPECT_EQ(environment.DeferredClient->PublishCalls, std::vector<ui64>{cookie});
    EXPECT_TRUE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    // Publish cannot clear the cookie from an already committed YT transaction.
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->IntPublicationId, cookie);
    environment.Sync(sink);
    sink->Commit();
    EXPECT_FALSE(environment.GetPersistedDeferredPublication()->IntPublicationId);
    EXPECT_EQ(environment.DeferredClient->PublishCalls.size(), 1u);
}

TEST_P(TDeferredPublicationSinkTest, RefreshesTargetBetweenPublicationsAndReusesHealthyWriter)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Context->SinkSpec->Parameters->RemoveChild("update_partition_count_period");
    environment.Context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(0));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    auto clusterBClient = New<TFakeDeferredPublicationClient>();
    environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
    environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
    environment.Resource->Resolutions = {
        {.Target = clusterA},
        {.Target = clusterA},
        {.Target = clusterA},
        {.Target = clusterB},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    ASSERT_EQ(environment.Resource->WriterTargets, (std::vector<std::pair<std::string, std::string>>{{clusterA.Endpoint, clusterA.Database}}));
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, clusterA.Endpoint);
    sink->Commit();

    environment.Distribute(sink, environment.MakeMessage(2, "two"));
    environment.Sync(sink);
    EXPECT_EQ(environment.Resource->WriterCreationCount, 1);
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, clusterA.Endpoint);
    sink->Commit();

    environment.Distribute(sink, environment.MakeMessage(3, "three"));
    environment.Sync(sink);
    ASSERT_EQ(environment.Resource->WriterTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets.back(), std::make_pair(clusterB.Endpoint, clusterB.Database));
    EXPECT_EQ(environment.Resource->PublicationTargets.back(), environment.Resource->WriterTargets.back());
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, clusterB.Endpoint);
    ASSERT_EQ(environment.Resource->ResolutionPreferredTargets.size(), 4u);
    EXPECT_FALSE(environment.Resource->ResolutionPreferredTargets[0]);
    EXPECT_FALSE(environment.Resource->ResolutionPreferredTargets[1]);
    EXPECT_EQ(environment.Resource->ResolutionPreferredTargets[2], clusterA);
    EXPECT_EQ(environment.Resource->ResolutionPreferredTargets[3], clusterA);
}

TEST_P(TDeferredPublicationSinkTest, RepeatedPreSyncDoesNotRefreshPreparedPublication)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Context->SinkSpec->Parameters->RemoveChild("update_partition_count_period");
    environment.Context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(0));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    environment.Resource->Resolutions = {
        {.Target = clusterA},
        {.Target = clusterA},
        {.Target = clusterB},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto callback = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    sink->PreSyncFlush();
    const auto resolutionCount = environment.Resource->ResolutionCallCount;
    const auto writerCount = environment.Resource->WriterCreationCount;
    const auto publicationClientCount = environment.Resource->PublicationClientCreationCount;
    sink->PreSyncFlush();

    EXPECT_EQ(*callback, 1);
    EXPECT_EQ(environment.Resource->ResolutionCallCount, resolutionCount);
    EXPECT_EQ(environment.Resource->WriterCreationCount, writerCount);
    EXPECT_EQ(environment.Resource->PublicationClientCreationCount, publicationClientCount);
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, std::nullopt);
    sink->Sync(nullptr);
    environment.StateManager->Sync();
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, clusterA.Endpoint);
}

TEST_P(TDeferredPublicationSinkTest, RefreshFailureDoesNotReuseStaleTarget)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Context->SinkSpec->Parameters->RemoveChild("update_partition_count_period");
    environment.Context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(0));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    environment.Resource->Resolutions = {
        {.Target = clusterA},
        {.Target = clusterA},
        {.Error = TError("No writable cluster"), .Retryable = true},
        {.Target = clusterB},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    sink->Commit();

    environment.Distribute(sink, environment.MakeMessage(2, "two"));
    environment.Sync(sink);
    EXPECT_EQ(environment.Resource->ResolutionCallCount, 4);
    ASSERT_EQ(environment.Resource->WriterTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets.back(), std::make_pair(clusterB.Endpoint, clusterB.Database));
    EXPECT_FALSE(environment.Context->StatusProfiler->GetStatus().Errors.contains("/write_target_discovery"));
}

TEST_P(TDeferredPublicationSinkTest, FailedWriterReplacementKeepsTargetPairConsistent)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Context->SinkSpec->Parameters->RemoveChild("update_partition_count_period");
    environment.Context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(0));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    environment.Resource->Resolutions = {
        {.Target = clusterA},
        {.Target = clusterA},
        {.Target = clusterB},
        {.Target = clusterB},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    sink->Commit();

    environment.Resource->WriterCreationFailures = 1;
    environment.Distribute(sink, environment.MakeMessage(2, "two"));
    EXPECT_THROW_WITH_SUBSTRING(sink->PreSyncFlush(), "Failed to create writer");
    EXPECT_EQ(environment.Resource->PublicationTargets.size(), 2u);

    environment.Sync(sink);
    EXPECT_EQ(environment.Resource->WriterCreationCount, 3);
    EXPECT_EQ(environment.Resource->PublicationTargets.back(), std::make_pair(clusterB.Endpoint, clusterB.Database));
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->Endpoint, clusterB.Endpoint);
}

TEST_P(TDeferredPublicationSinkTest, FederatedPendingPublicationDoesNotBlockAnotherTarget)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    environment.Context->SinkSpec->Parameters->RemoveChild("update_partition_count_period");
    environment.Context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(0));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster:2135", .Database = "/Root/a"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster:2135", .Database = "/Root/b"};
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    auto clusterBClient = New<TFakeDeferredPublicationClient>();
    clusterAClient->AutoPublish = false;
    clusterAClient->PublishStatuses.push_back(NYdb::EStatus::UNAVAILABLE);
    environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
    environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
    environment.Resource->Resolutions = {
        {.Target = clusterA, .Federated = true},
        {.Target = clusterA, .Federated = true},
        {.Target = clusterB, .Federated = true},
        {.Target = clusterB, .Federated = true},
        {.Target = clusterA, .Federated = true},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "a"));
    environment.Sync(sink);
    sink->Commit();
    NConcurrency::WaitFor(clusterAClient->PendingPublishStarted.ToFuture()).ThrowOnError();
    ASSERT_GE(clusterAClient->PublishCalls.size(), 2u);
    EXPECT_FALSE(sink->BuildInflight().at(SinkTestStreamId)->Empty);

    environment.Distribute(sink, environment.MakeMessage(2, "b"));
    environment.Sync(sink);
    ASSERT_EQ(environment.GetPersistedDeferredState()->PendingPublications.size(), 2u);
    auto inflight = sink->BuildInflight().at(SinkTestStreamId);
    EXPECT_EQ(inflight->InflightMetrics->Count, 2);
    EXPECT_GT(inflight->InflightMetrics->ByteSize, 0);
    EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(1'700'000'000));
    EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(1'700'000'000));
    EXPECT_FALSE(inflight->Empty);
    sink->Commit();
    environment.DrainAsync();
    ASSERT_EQ(clusterBClient->PublishCalls.size(), 1u);
    const auto clusterAErrorPath = Format("/deferred_publication/control/targets/%v",
        TYdbWriteTargetHash()(clusterA));
    EXPECT_TRUE(environment.Context->StatusProfiler->GetStatus().Errors.contains(clusterAErrorPath));
    EXPECT_EQ(sink->BuildInflight().at(SinkTestStreamId)->InflightMetrics->Count, 2);

    environment.Distribute(sink, environment.MakeMessage(3, "b-again"));
    environment.Sync(sink);
    sink->Commit();
    WaitForPredicate([&] {
        environment.Sync(sink);
        sink->Commit();
        const auto state = environment.GetPersistedDeferredState();
        const auto& pending = state->PendingPublications;
        return pending.size() == 1 && *pending.front()->Database == clusterA.Database;
    },
        "Waiting for the healthy target publication to be harvested");
    EXPECT_EQ(sink->BuildInflight().at(SinkTestStreamId)->InflightMetrics->Count, 1);

    clusterAClient->CompletePublish();
    WaitForPredicate([&] {
        environment.Sync(sink);
        sink->Commit();
        const auto state = environment.GetPersistedDeferredState();
        return state->PendingPublications.empty() && sink->BuildInflight().at(SinkTestStreamId)->Empty;
    },
        "Waiting for the offline target publication to be harvested");
    EXPECT_TRUE(environment.GetPersistedDeferredState()->PendingPublications.empty());
    EXPECT_TRUE(sink->BuildInflight().at(SinkTestStreamId)->Empty);

    environment.Distribute(sink, environment.MakeMessage(4, "a-again"));
    environment.Sync(sink);
    sink->Commit();
    EXPECT_TRUE(clusterAClient->CancelCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, FederatedPreparationTimeoutRetriesWholeBatchOnAnotherTarget)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
        "preparation_attempt_timeout",
        NYTree::ConvertToNode(TDuration::Seconds(1)));
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
        "preparation_target_backoff",
        NYTree::ConvertToNode(TDuration::Hours(1)));
    if (GetParam()) {
        environment.DynamicContext->DynamicSinkSpec->Parameters->RemoveChild("max_rows_per_batch");
        environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild("max_rows_per_batch", NYTree::ConvertToNode(1));
    }
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    auto clusterBClient = New<TFakeDeferredPublicationClient>();
    environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
    environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
    environment.Resource->Resolutions = {
        {.Target = clusterA, .Federated = true},
        {.Target = clusterB, .Federated = true},
    };
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 2;
    environment.WriteState->SecondNotifyAfterWriteCount = 4;

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto firstCallback = environment.Distribute(sink, environment.MakeMessage(1, "first"));
    auto secondCallback = environment.Distribute(sink, environment.MakeMessage(2, "second"));
    auto queue = New<NConcurrency::TActionQueue>("PreparationFailoverTest");
    auto prepared = BIND([sink] {
        sink->PreSyncFlush();
    }).AsyncVia(queue->GetInvoker())
        .Run();
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(BIND([state = environment.WriteState] {
        state->AcknowledgeNext();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(environment.WriteState->SecondWriteCountReached.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(BIND([state = environment.WriteState] {
        state->AcknowledgeAll();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(prepared).ThrowOnError();

    ASSERT_EQ(environment.Resource->WriterTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets[0], std::make_pair(clusterA.Endpoint, clusterA.Database));
    EXPECT_EQ(environment.Resource->WriterTargets[1], std::make_pair(clusterB.Endpoint, clusterB.Database));
    ASSERT_EQ(environment.WriteState->Writes.size(), 4u);
    ASSERT_EQ(clusterAClient->BeginCalls.size(), 1u);
    ASSERT_EQ(clusterBClient->BeginCalls.size(), 1u);
    EXPECT_EQ(environment.WriteState->Writes[0].DeferredPublication->IntPublicationId, 100u);
    EXPECT_EQ(environment.WriteState->Writes[2].DeferredPublication->IntPublicationId, 100u);
    EXPECT_EQ(environment.WriteState->Writes[0].SeqNo, 1);
    EXPECT_EQ(environment.WriteState->Writes[1].SeqNo, 2);
    EXPECT_EQ(environment.WriteState->Writes[2].SeqNo, 3);
    EXPECT_EQ(environment.WriteState->Writes[3].SeqNo, 4);
    EXPECT_TRUE(clusterAClient->CancelCalls.empty());
    ASSERT_GE(environment.Resource->ResolutionExcludedTargets.size(), 2u);
    EXPECT_TRUE(environment.Resource->ResolutionExcludedTargets[1].contains(clusterA));
    const auto preparationErrorPath = Format("/deferred_publication/preparation/targets/%v",
        TYdbWriteTargetHash()(clusterA));
    const auto preparationStatus = environment.Context->StatusProfiler->GetStatus();
    ASSERT_TRUE(preparationStatus.Errors.contains(preparationErrorPath));
    const auto& preparationError = preparationStatus.Errors.at(preparationErrorPath);
    EXPECT_EQ(preparationError.Attributes().Get<std::string>("endpoint"), clusterA.Endpoint);
    EXPECT_EQ(preparationError.Attributes().Get<std::string>("database"), clusterA.Database);
    EXPECT_EQ(*firstCallback, 1);
    EXPECT_EQ(*secondCallback, 1);
    sink->Sync(nullptr);
    environment.StateManager->Sync();
    const auto state = environment.GetPersistedDeferredState();
    ASSERT_EQ(state->PendingPublications.size(), 1u);
    EXPECT_EQ(state->PendingPublications[0]->Endpoint, clusterB.Endpoint);
}

TEST_P(TDeferredPublicationSinkTest, LateBeginSuccessIsPassivelyIgnored)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
        "preparation_attempt_timeout",
        NYTree::ConvertToNode(TDuration::MilliSeconds(20)));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    auto clusterBClient = New<TFakeDeferredPublicationClient>();
    clusterAClient->PendingBegin = NewPromise<NYdb::NTopic::TBeginPublicationResult>();
    environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
    environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
    environment.Resource->Resolutions = {
        {.Target = clusterA, .Federated = true},
        {.Target = clusterB, .Federated = true},
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "payload"));
    sink->PreSyncFlush();
    EXPECT_TRUE(clusterAClient->CancelCalls.empty());
    clusterAClient->PendingBegin->Set(NYdb::NTopic::TBeginPublicationResult(
        NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{}),
        TDeferredPublication(7, environment.GetExternalPublicationId())));
    EXPECT_TRUE(clusterAClient->CancelCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, LateListResultIsPassivelyIgnored)
{
    for (bool newerJob : {false, true}) {
        TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
        environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
            "preparation_attempt_timeout",
            NYTree::ConvertToNode(TDuration::MilliSeconds(20)));
        const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
        const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
        auto clusterAClient = New<TFakeDeferredPublicationClient>();
        auto clusterBClient = New<TFakeDeferredPublicationClient>();
        clusterAClient->BeginStatus = NYdb::EStatus::ALREADY_EXISTS;
        clusterAClient->PendingList = NewPromise<NYdb::NTopic::TListPublicationsResult>();
        environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
        environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
        environment.Resource->Resolutions = {
            {.Target = clusterA, .Federated = true},
            {.Target = clusterB, .Federated = true},
        };

        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        environment.Distribute(sink, environment.MakeMessage(1, "payload"));
        sink->PreSyncFlush();
        const auto ownerGeneration = newerJob
            ? TUniqueSeqNo(environment.Context->JobGeneration.Underlying() + 1)
            : environment.Context->JobGeneration;
        std::vector<NYdb::NTopic::TPublicationSummary> publications{{
            .IntPublicationId = 7,
            .ExtPublicationId = environment.GetExternalPublicationId(),
            .WriterIdentity = environment.MakeWriterIdentity(ownerGeneration, 2),
        }};
        clusterAClient->PendingList->Set(NYdb::NTopic::TListPublicationsResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{}),
            std::move(publications)));
        EXPECT_NO_THROW(sink->Sync(nullptr));
        EXPECT_TRUE(clusterAClient->CancelCalls.empty());
    }
}

TEST_P(TDeferredPublicationSinkTest, PermanentWriteFailureWinsBeforePreparationTimeout)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild(
        "preparation_attempt_timeout",
        NYTree::ConvertToNode(TDuration::Seconds(1)));
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    environment.Resource->Resolutions = {
        {.Target = clusterA, .Federated = true},
        {.Target = clusterB, .Federated = true},
    };
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 1;

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "payload"));
    auto queue = New<NConcurrency::TActionQueue>("PermanentWriteFailureTest");
    auto prepared = BIND([sink] {
        sink->PreSyncFlush();
    }).AsyncVia(queue->GetInvoker())
        .Run();
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(BIND([state = environment.WriteState] {
        state->PendingWritePromises.front().Set(TError("permanent write failure"));
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_FALSE(NConcurrency::WaitFor(prepared).IsOK());
    EXPECT_EQ(environment.Resource->WriterTargets.size(), 1u);
}

TEST_P(TDeferredPublicationSinkTest, RestartRecoversOneTargetWhileAnotherContinues)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    const TYdbWriteTarget clusterA{.Endpoint = "cluster-a:2135", .Database = "/Root/db"};
    const TYdbWriteTarget clusterB{.Endpoint = "cluster-b:2135", .Database = "/Root/db"};
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    auto clusterBClient = New<TFakeDeferredPublicationClient>();
    clusterAClient->AutoPublish = false;
    environment.Resource->PublicationClients[{clusterA.Endpoint, clusterA.Database}] = clusterAClient;
    environment.Resource->PublicationClients[{clusterB.Endpoint, clusterB.Database}] = clusterBClient;
    environment.PersistCookies({{7, clusterA.Endpoint, clusterA.Database}});
    environment.Resource->Resolutions = {
        {.Target = clusterB, .Federated = true},
        {.Target = clusterB, .Federated = true},
        {.Target = clusterB, .Federated = true},
    };

    {
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        environment.Distribute(sink, environment.MakeMessage(1, "b"));
        environment.Sync(sink);
        sink->Commit();
        environment.DrainAsync();
        ASSERT_EQ(environment.GetPersistedDeferredState()->PendingPublications.size(), 2u);
    }

    auto restarted = environment.CreateSink();
    restarted->Init(environment.StateManager->CreateContext());
    environment.DrainAsync();
    environment.Distribute(restarted, environment.MakeMessage(2, "b-after-restart"));
    environment.Sync(restarted);
    restarted->Commit();
    EXPECT_EQ(environment.Resource->ResolutionCallCount, 3);
    EXPECT_FALSE(restarted->BuildInflight().at(SinkTestStreamId)->Empty);
    EXPECT_TRUE(clusterAClient->CancelCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, InitDefersSdkClientAndTargetResolutionFactories)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    environment.PersistCookie(7);
    NConcurrency::WaitFor(environment.SuspendableInvoker->Suspend()).ThrowOnError();

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    EXPECT_EQ(environment.Resource->PublicationClientCreationCount, 0);
    EXPECT_EQ(environment.Resource->ResolutionCallCount, 0);

    environment.SuspendableInvoker->Resume();
    NConcurrency::WaitFor(environment.Resource->PublicationClientCreated.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(environment.Resource->ResolutionCalled.ToFuture()).ThrowOnError();
    sink.Reset();
}

TEST_P(TDeferredPublicationSinkTest, RawSingleClusterEmptyRecoveryDoesNotCreateWriter)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    environment.PersistCookie(7);
    environment.DeferredClient->AutoPublish = false;
    environment.Resource->WriterCreationFailures = 1;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    NConcurrency::WaitFor(environment.DeferredClient->PublishStarted.ToFuture()).ThrowOnError();
    auto queue = New<NConcurrency::TActionQueue>("EmptyRecoveryTest");
    auto flushed = BIND([sink] {
        sink->PreSyncFlush();
    }).AsyncVia(queue->GetInvoker())
        .Run();
    NConcurrency::WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_FALSE(flushed.IsSet());
    EXPECT_EQ(environment.Resource->WriterCreationCount, 0);
    environment.DeferredClient->CompletePublish();
    NConcurrency::WaitFor(flushed).ThrowOnError();
    EXPECT_EQ(environment.Resource->WriterCreationCount, 0);
}

TEST_P(TDeferredPublicationSinkTest, DestroyingSinkCancelsRecoveryPublish)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    auto firstClient = New<TFakeDeferredPublicationClient>();
    auto secondClient = New<TFakeDeferredPublicationClient>();
    firstClient->AutoPublish = false;
    secondClient->AutoPublish = false;
    environment.Resource->PublicationClients[{"a:2135", "/Root/a"}] = firstClient;
    environment.Resource->PublicationClients[{"b:2135", "/Root/b"}] = secondClient;
    environment.PersistCookies({{7, "a:2135", "/Root/a"}, {8, "b:2135", "/Root/b"}});
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    NConcurrency::WaitFor(firstClient->PublishStarted.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(secondClient->PublishStarted.ToFuture()).ThrowOnError();
    ASSERT_TRUE(firstClient->LastPendingPublishFuture);
    ASSERT_TRUE(secondClient->LastPendingPublishFuture);
    sink.Reset();
    for (const auto& client : {firstClient, secondClient}) {
        auto error = NConcurrency::WaitFor(client->LastPendingPublishFuture);
        EXPECT_FALSE(error.IsOK());
        EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled));
    }
}

TEST_P(TDeferredPublicationSinkTest, RecoveryFailureKeepsCookieAndInflight)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.PersistCookie(7);
    environment.DeferredClient->PublishStatuses.push_back(NYdb::EStatus::BAD_REQUEST);
    auto sink = environment.CreateSink();
    EXPECT_NO_THROW(sink->Init(environment.StateManager->CreateContext()));
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    EXPECT_THROW_WITH_SUBSTRING(environment.Sync(sink), "Failed to finalize");
    EXPECT_FALSE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->IntPublicationId, 7u);
    EXPECT_TRUE(environment.DeferredClient->BeginCalls.empty());
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
    EXPECT_TRUE(environment.WriteState->Writes.empty());
}

TEST_P(TDeferredPublicationSinkTest, FailedCommitRecoversPersistedCookieInNextJob)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    const auto cookie = *environment.GetPersistedDeferredPublication()->IntPublicationId;
    environment.DeferredClient->PublishStatuses.push_back(NYdb::EStatus::BAD_REQUEST);
    EXPECT_THROW_WITH_SUBSTRING(sink->Commit(), "Failed to finalize");
    EXPECT_FALSE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    sink.Reset();
    environment.SetJobGeneration(11);
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(environment.DeferredClient->PublishCalls, (std::vector<ui64>{cookie, cookie}));
    EXPECT_FALSE(environment.GetPersistedDeferredPublication()->IntPublicationId);
    EXPECT_TRUE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, RecoverySkipsMessagesRetainedByOtherDestination)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto message = environment.MakeMessage(1, "one");
    int distributed = 0;
    auto tracker = TDistributingTracker([&distributed] {
        ++distributed;
    });
    sink->Distribute(message, tracker.AddDestination());
    auto otherDestination = tracker.AddDestination();
    tracker.Activate();
    environment.Sync(sink);
    EXPECT_EQ(distributed, 0);
    ASSERT_EQ(environment.WriteState->Writes.size(), 1u);
    const auto cookie = *environment.GetPersistedDeferredPublication()->IntPublicationId;

    // YT committed, but another destination still needs the persisted output on restart.
    sink.Reset();
    environment.SetJobGeneration(11);
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto replayed = environment.Distribute(sink, message);
    auto fresh = environment.Distribute(sink, environment.MakeMessage(2, "two"));
    environment.Sync(sink);
    EXPECT_EQ(*replayed, 1);
    EXPECT_EQ(*fresh, 1);
    ASSERT_EQ(environment.WriteState->Writes.size(), 2u);
    EXPECT_EQ(environment.WriteState->Writes.back().SeqNo, 2);
    EXPECT_EQ(environment.DeferredClient->PublishCalls, std::vector<ui64>{cookie});
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
    sink->Commit();
    otherDestination();
    EXPECT_EQ(distributed, 1);
}

TEST(TDeferredYdbFramingSinkTest, CancelledEpochCanUseDifferentBatchBounds)
{
    TSinkTestEnvironment environment(PublicSinkShape(), /*framing*/ true, /*maxRowsPerBatch*/ 2);
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    for (int id = 1; id <= 3; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink, /*persist*/ false);
    ASSERT_EQ(environment.WriteState->Writes.size(), 2u);
    const auto cancelledCookie = environment.WriteState->Writes.front().DeferredPublication->IntPublicationId;
    sink.Reset();

    environment.SetJobGeneration(11);
    environment.DynamicContext->DynamicSinkSpec->Parameters->RemoveChild("max_rows_per_batch");
    environment.DynamicContext->DynamicSinkSpec->Parameters->AddChild("max_rows_per_batch", NYTree::ConvertToNode(3));
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    for (int id = 1; id <= 3; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink);
    EXPECT_EQ(environment.DeferredClient->CancelCalls, std::vector<ui64>{cancelledCookie});
    ASSERT_EQ(environment.WriteState->Writes.size(), 3u);
    EXPECT_NE(environment.WriteState->Writes.back().DeferredPublication->IntPublicationId, cancelledCookie);
    EXPECT_EQ(sink->BuildInflight().at(SinkTestStreamId)->InflightMetrics->Count, 3);
    sink->Commit();
}

TEST_P(TDeferredPublicationSinkTest, AlreadyPublishedCookieIsTerminalOnRecovery)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.PersistCookie(7);
    auto sink = environment.CreateSink();
    CollectProfiler(environment.Registry);
    sink->Init(environment.StateManager->CreateContext());
    EXPECT_FALSE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    environment.Sync(sink);
    sink->Commit();
    EXPECT_TRUE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
    EXPECT_FALSE(environment.GetPersistedDeferredPublication()->IntPublicationId);
    CollectProfiler(environment.Registry);
    EXPECT_EQ(ReadTaggedCounter(environment.Registry, "yt/test/deferred_publication/publish/results", "outcome", "not_found"), 1);
    EXPECT_EQ(ReadCounter(environment.Registry, "yt/test/deferred_publication/recovery_publish/operations"), 1);
    EXPECT_EQ(ReadCounter(environment.Registry, "yt/test/deferred_publication/recovery_publish/not_found"), 1);
}

TEST_P(TDeferredPublicationSinkTest, CommitNotFoundIsTerminal)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    environment.DeferredClient->PublishStatuses.push_back(NYdb::EStatus::NOT_FOUND);
    EXPECT_NO_THROW(sink->Commit());
    EXPECT_TRUE(sink->BuildInflight().at(SinkTestStreamId)->Empty);
}

TEST_P(TDeferredPublicationSinkTest, PreSyncFlushWaitsForAllWriteAcks)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1);
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 2;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto first = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto second = environment.Distribute(sink, environment.MakeMessage(2, "two"));
    auto queue = New<NConcurrency::TActionQueue>("WriteAckTest");
    auto synced = BIND([sink] {
        sink->PreSyncFlush();
        sink->Sync(nullptr);
    }).AsyncVia(queue->GetInvoker())
        .Run();
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture()).ThrowOnError();
    environment.WriteState->AcknowledgeNext();
    EXPECT_FALSE(synced.IsSet());
    EXPECT_EQ(*first, 0);
    EXPECT_EQ(*second, 0);
    EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
    environment.WriteState->AcknowledgeAll();
    NConcurrency::WaitFor(synced).ThrowOnError();
    EXPECT_EQ(*first, 1);
    EXPECT_EQ(*second, 1);
    environment.StateManager->Sync();
    sink->Commit();
    EXPECT_EQ(environment.DeferredClient->PublishCalls.size(), 1u);
}

TEST_P(TDeferredPublicationSinkTest, FailedWriteDoesNotPrepareCookieOrAcknowledgeMessages)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 1;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto callback = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto queue = New<NConcurrency::TActionQueue>("FailedWriteTest");
    auto synced = BIND([sink] {
        sink->PreSyncFlush();
        sink->Sync(nullptr);
    }).AsyncVia(queue->GetInvoker())
        .Run();
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture()).ThrowOnError();
    environment.WriteState->PendingWritePromises.front().Set(TError("Write rejected"));
    EXPECT_FALSE(NConcurrency::WaitFor(synced).IsOK());
    environment.StateManager->Sync();
    EXPECT_FALSE(environment.GetPersistedDeferredPublication()->IntPublicationId);
    EXPECT_EQ(*callback, 0);
    EXPECT_EQ(environment.WriteState->Writes.size(), 1u);
    EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, CancelsOnlyExactOlderPublication)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    const auto extId = environment.GetExternalPublicationId();
    const auto oldIdentity = environment.MakeWriterIdentity(TUniqueSeqNo(1));
    environment.DeferredClient->SetActivePublication(7, extId, oldIdentity);
    environment.DeferredClient->BeforeCancel = [extId, oldIdentity] (auto& client, const auto& publication) {
        if (publication.IntPublicationId == 7) {
            client.SetActivePublication(8, extId, oldIdentity);
        }
    };
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    EXPECT_TRUE(environment.DeferredClient->Actions.empty());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    EXPECT_EQ(environment.DeferredClient->CancelCalls, (std::vector<ui64>{7, 8}));
    EXPECT_EQ(environment.DeferredClient->ListCallCount, 2);
    sink->Commit();
}

TEST_P(TDeferredPublicationSinkTest, RecoveryCookiePrecedesOrphanCancellation)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.PersistCookie(7);
    environment.DeferredClient->SetActivePublication(8, environment.GetExternalPublicationId(), environment.MakeWriterIdentity(TUniqueSeqNo(1)));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    EXPECT_EQ(environment.DeferredClient->PublishCalls, std::vector<ui64>{7});
    EXPECT_EQ(environment.DeferredClient->CancelCalls, std::vector<ui64>{8});
    EXPECT_EQ(environment.DeferredClient->Actions,
        (std::vector<std::string>{"publish", "begin", "list", "cancel", "begin"}));
}

TEST_P(TDeferredPublicationSinkTest, RecoveryUsesPersistedTargetBeforeNewDiscoveryTarget)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam(), 1000, 10, /*asyncInvoker*/ true);
    auto clusterAClient = New<TFakeDeferredPublicationClient>();
    clusterAClient->AutoPublish = false;
    environment.Resource->PublicationClients[{"cluster-a:2135", "/Root/a"}] = clusterAClient;
    environment.PersistCookie(7, "cluster-a:2135", "/Root/a");
    environment.Resource->PendingResolution = NewPromise<TYdbWriteTargetResolution>();
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    NConcurrency::WaitFor(clusterAClient->PublishStarted.ToFuture()).ThrowOnError();
    NConcurrency::WaitFor(environment.Resource->ResolutionCalled.ToFuture()).ThrowOnError();
    EXPECT_EQ(clusterAClient->PublishCalls, std::vector<ui64>{7});
    EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
    ASSERT_EQ(environment.Resource->PublicationTargets.size(), 1u);
    EXPECT_EQ(environment.Resource->PublicationTargets[0], std::make_pair(std::string("cluster-a:2135"), std::string("/Root/a")));

    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto queue = New<NConcurrency::TActionQueue>("EndpointBindingTest");
    auto synced = BIND([&] {
        environment.Sync(sink);
    }).AsyncVia(queue->GetInvoker())
        .Run();
    clusterAClient->CompletePublish(NYdb::EStatus::NOT_FOUND);
    EXPECT_FALSE(synced.IsSet());
    environment.Resource->PendingResolution->Set(TYdbWriteTargetResolution{
        .Target = TYdbWriteTarget{.Endpoint = "cluster-b:2135", .Database = "/Root/b"},
    });
    environment.Resource->PendingResolution.reset();
    NConcurrency::WaitFor(synced).ThrowOnError();
    ASSERT_EQ(environment.Resource->WriterTargets.size(), 1u);
    ASSERT_EQ(environment.Resource->PublicationTargets.size(), 2u);
    EXPECT_EQ(environment.Resource->WriterTargets[0], std::make_pair(std::string("cluster-b:2135"), std::string("/Root/b")));
    EXPECT_EQ(environment.Resource->PublicationTargets[1], environment.Resource->WriterTargets[0]);
    const auto persisted = environment.GetPersistedDeferredPublication();
    EXPECT_EQ(persisted->Endpoint, "cluster-b:2135");
    EXPECT_EQ(persisted->Database, "/Root/b");
    ASSERT_TRUE(persisted->IntPublicationId);
    EXPECT_NE(*persisted->IntPublicationId, 7u);
}

TEST_P(TDeferredPublicationSinkTest, DestroyingSinkCancelsPendingTargetDiscovery)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Resource->PendingResolution = NewPromise<TYdbWriteTargetResolution>();
    auto pending = environment.Resource->PendingResolution->ToFuture();
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    sink.Reset();
    EXPECT_TRUE(NConcurrency::WaitFor(pending).FindMatching(NYT::EErrorCode::Canceled));
}

TEST_P(TDeferredPublicationSinkTest, RetryableTargetDiscoveryDoesNotPoisonPreparation)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.Resource->Resolutions.push_back(TYdbWriteTargetResolution{
        .Error = TError("Temporary discovery failure"),
        .Retryable = true,
    });
    environment.Resource->Resolutions.push_back(TYdbWriteTargetResolution{
        .Target = TYdbWriteTarget{.Endpoint = "localhost:2135", .Database = "/Root"},
    });
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    EXPECT_NO_THROW(environment.Sync(sink));
    EXPECT_EQ(environment.Resource->ResolutionCallCount, 2);
    EXPECT_FALSE(environment.Context->StatusProfiler->GetStatus().Errors.contains("/write_target_discovery"));
}

TEST_P(TDeferredPublicationSinkTest, RejectsPendingCookieWithoutDestination)
{
    for (int variant = 0; variant < 3; ++variant) {
        TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
        auto state = New<TYdbDeferredPublicationState>();
        auto pending = New<TYdbPendingPublicationState>();
        pending->IntPublicationId = 7;
        if (variant != 0) {
            pending->Endpoint = variant == 1 ? "" : "cluster-a:2135";
            pending->Database = variant == 2 ? "" : "/Root/a";
        }
        state->PendingPublications.push_back(std::move(pending));
        environment.StateManager->Set("/deferred_publication/v0", NYson::ConvertToYsonString(state));
        auto sink = environment.CreateSink();
        EXPECT_THROW_WITH_SUBSTRING(sink->Init(environment.StateManager->CreateContext()), "no persisted destination");
    }
}

TEST_P(TDeferredPublicationSinkTest, SameGenerationAdoptsAmbiguousBegin)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->SetActivePublication(7, environment.GetExternalPublicationId(), environment.MakeWriterIdentity(environment.Context->JobGeneration));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
    EXPECT_EQ(environment.GetPersistedDeferredPublication()->IntPublicationId, 7u);
    sink->Commit();
}

TEST_P(TDeferredPublicationSinkTest, SameGenerationCancelsOlderPreparationAttempt)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->SetActivePublication(
        7,
        environment.GetExternalPublicationId(),
        environment.MakeWriterIdentity(environment.Context->JobGeneration, 0));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    EXPECT_EQ(environment.DeferredClient->CancelCalls, std::vector<ui64>{7});
    sink->Commit();
}

TEST_P(TDeferredPublicationSinkTest, SameGenerationRejectsNewerPreparationAttempt)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->SetActivePublication(
        7,
        environment.GetExternalPublicationId(),
        environment.MakeWriterIdentity(environment.Context->JobGeneration, 2));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    EXPECT_THROW_WITH_SUBSTRING(environment.Sync(sink), "newer job");
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
}

TEST_P(TDeferredPublicationSinkTest, RejectsNewerOrUnrecognizableOwnerWithoutCancellation)
{
    for (int owner = 0; owner < 4; ++owner) {
        TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
        const auto identity = owner == 0
            ? std::optional(environment.MakeWriterIdentity(TUniqueSeqNo(11)))
            : owner == 1
            ? std::optional<std::string>("foreign/1")
            : owner == 2
            ? std::optional<std::string>("ytflow-attempt/v1/not-a-number")
            : std::nullopt;
        environment.DeferredClient->SetActivePublication(7, environment.GetExternalPublicationId(), identity);
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        environment.Distribute(sink, environment.MakeMessage(1, "one"));
        EXPECT_THROW(environment.Sync(sink), std::exception);
        EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
        EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
        EXPECT_TRUE(environment.WriteState->Writes.empty());
    }
}

TEST_P(TDeferredPublicationSinkTest, ControlErrorsDoNotWriteMessages)
{
    for (int operation = 0; operation < 3; ++operation) {
        TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
        if (operation == 0) {
            environment.DeferredClient->BeginStatus = NYdb::EStatus::BAD_REQUEST;
        } else {
            environment.DeferredClient->SetActivePublication(7, environment.GetExternalPublicationId(), environment.MakeWriterIdentity(TUniqueSeqNo(1)));
            if (operation == 1) {
                environment.DeferredClient->ListStatus = NYdb::EStatus::BAD_REQUEST;
            } else {
                environment.DeferredClient->CancelStatus = NYdb::EStatus::BAD_REQUEST;
            }
        }
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        environment.Distribute(sink, environment.MakeMessage(1, "one"));
        EXPECT_THROW(environment.Sync(sink), std::exception);
        EXPECT_TRUE(environment.WriteState->Writes.empty());
        EXPECT_TRUE(environment.DeferredClient->PublishCalls.empty());
    }
}

TEST_P(TDeferredPublicationSinkTest, RetriesTransientControlOperationsWithoutRepeatingWrites)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->SetActivePublication(
        7,
        environment.GetExternalPublicationId(),
        environment.MakeWriterIdentity(TUniqueSeqNo(1)));
    environment.DeferredClient->BeginStatuses = {
        NYdb::EStatus::UNAVAILABLE,
        NYdb::EStatus::ALREADY_EXISTS,
        NYdb::EStatus::SUCCESS,
    };
    environment.DeferredClient->ListStatuses = {
        NYdb::EStatus::TIMEOUT,
        NYdb::EStatus::SUCCESS,
    };
    environment.DeferredClient->CancelStatuses = {
        NYdb::EStatus::UNAVAILABLE,
        NYdb::EStatus::SUCCESS,
    };
    environment.DeferredClient->PublishStatuses = {
        NYdb::EStatus::TIMEOUT,
        NYdb::EStatus::SUCCESS,
    };

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto callback = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    EXPECT_EQ(*callback, 1);
    EXPECT_EQ(environment.WriteState->Writes.size(), 1u);
    sink->Commit();
    EXPECT_EQ(environment.WriteState->Writes.size(), 1u);
    EXPECT_EQ(environment.DeferredClient->PublishCalls.size(), 2u);
}

TEST_P(TDeferredPublicationSinkTest, RetryableControlErrorIsVisibleUntilSuccess)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->BeginStatus = NYdb::EStatus::UNAVAILABLE;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto queue = New<NConcurrency::TActionQueue>("ControlRetryStatusTest");
    auto synced = BIND([sink] {
        sink->PreSyncFlush();
        sink->Sync(nullptr);
    }).AsyncVia(queue->GetInvoker())
        .Run();

    const auto errorPath = "/deferred_publication/control";
    for (int attempt = 0; attempt < 1000 && !environment.Context->StatusProfiler->GetStatus().Errors.contains(errorPath); ++attempt) {
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
    }
    const auto retryStatus = environment.Context->StatusProfiler->GetStatus();
    ASSERT_TRUE(retryStatus.Errors.contains(errorPath));
    const auto& retryError = retryStatus.Errors.at(errorPath);
    EXPECT_EQ(retryError.Attributes().Get<std::string>("endpoint"), "localhost:2135");
    EXPECT_EQ(retryError.Attributes().Get<std::string>("database"), "/Root");
    environment.DeferredClient->BeginStatus = NYdb::EStatus::SUCCESS;
    NConcurrency::WaitFor(synced).ThrowOnError();
    EXPECT_FALSE(environment.Context->StatusProfiler->GetStatus().Errors.contains(errorPath));
    sink->Commit();
}

TEST_P(TDeferredPublicationSinkTest, AdoptsBeginAndAcceptsCancelPublishAppliedBeforeTransientReply)
{
    TSinkTestEnvironment environment(PublicSinkShape(), GetParam());
    environment.DeferredClient->ApplyBeginBeforeTransientStatus = true;
    environment.DeferredClient->ApplyCancelBeforeTransientStatus = true;
    environment.DeferredClient->ApplyPublishBeforeTransientStatus = true;
    environment.DeferredClient->BeginStatuses = {NYdb::EStatus::TIMEOUT};

    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    auto callback = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    environment.Sync(sink);
    ASSERT_EQ(environment.DeferredClient->BeginCalls.size(), 2u);
    EXPECT_EQ(environment.DeferredClient->BeginCalls[0].ExternalPublicationId, environment.DeferredClient->BeginCalls[1].ExternalPublicationId);
    EXPECT_EQ(environment.DeferredClient->BeginCalls[0].WriterIdentity, environment.DeferredClient->BeginCalls[1].WriterIdentity);
    EXPECT_TRUE(environment.DeferredClient->CancelCalls.empty());
    EXPECT_EQ(*callback, 1);
    EXPECT_EQ(environment.WriteState->Writes.size(), 1u);

    environment.DeferredClient->PublishStatuses = {NYdb::EStatus::UNAVAILABLE};
    sink->Commit();
    ASSERT_EQ(environment.DeferredClient->PublishCalls.size(), 2u);
    EXPECT_EQ(environment.DeferredClient->PublishCalls[0], environment.DeferredClient->PublishCalls[1]);
    EXPECT_EQ(environment.WriteState->Writes.size(), 1u);

    sink.Reset();
    environment.SetJobGeneration(11);
    environment.DeferredClient->SetActivePublication(
        7,
        environment.GetExternalPublicationId(),
        environment.MakeWriterIdentity(TUniqueSeqNo(1)));
    environment.DeferredClient->CancelStatuses = {NYdb::EStatus::TIMEOUT};
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(2, "two"));
    environment.Sync(sink);
    ASSERT_EQ(environment.DeferredClient->CancelCalls.size(), 2u);
    EXPECT_EQ(environment.DeferredClient->CancelCalls[0], 7u);
    EXPECT_EQ(environment.DeferredClient->CancelCalls[1], 7u);
}

INSTANTIATE_TEST_SUITE_P(PlainAndFraming, TDeferredPublicationSinkTest, ::testing::Bool());

////////////////////////////////////////////////////////////////////////////////

TEST(TDeferredYdbFramingSinkTest, QueuedReconfigurationsApplyTheirOwnRowSnapshots)
{
    CheckQueuedReconfigurationsApplyTheirOwnBatching(PublicSinkShape(), "max_rows_per_batch", 2, 2u, 3, 1u);
}

TEST(TDeferredYdbFramingSinkTest, QueuedReconfigurationsApplyTheirOwnByteSnapshots)
{
    CheckQueuedReconfigurationsApplyTheirOwnBatching(PublicSinkShape(), "max_bytes_per_batch", 1, 3u, 5_MB, 1u);
}

TEST(TDeferredPublicationContractTest, CheckpointIsWrittenBeforeDistributionCallbacks)
{
    CheckCheckpointIsWrittenBeforeDistributionCallbacks(PublicSinkShape());
}

TEST(TDeferredPublicationContractTest, RejectedPreparationTimeoutKeepsAcceptedOptions)
{
    CheckRejectedPreparationTimeoutKeepsAcceptedOptions(PublicSinkShape());
}

TEST(TDeferredPublicationContractTest, PreparationAttemptUsesOneOptionsSnapshot)
{
    CheckPreparationAttemptUsesOneOptionsSnapshot(PublicSinkShape());
}

TEST(TDeferredPublicationContractTest, SimultaneousComputationsUseTheirOwnResources)
{
    CheckSimultaneousComputationsUseTheirOwnResources(PublicSinkShape());
}

TEST(TTwoPhaseCommitEngineTest, CheckpointPrecedesEventLagAndCallbackForEveryMessage)
{
    TSinkTestEnvironment environment(PublicSinkShape(), /*framing*/ false);
    std::vector<std::string> events;
    TYdbSinkCheckpoint checkpoint;
    auto options = MakeYdbTwoPhaseCommitOptions(
        *NYTree::ConvertTo<TYdbTwoPhaseCommitSinkParametersPtr>(environment.Context->SinkSpec->Parameters));
    IYdbClientFactoryPtr clientFactory = environment.Resource;
    auto writer = New<TYdbSinkEngine>(
        environment.Context,
        std::make_shared<const TYdbSinkOptions>(options->Sink),
        clientFactory,
        environment.Context->StatusProfiler,
        TestLogger);
    auto publication = New<TYdbDeferredPublication>(
        environment.Context,
        clientFactory,
        options->WriterIdentityType,
        CreateYdbStateAccess<TYdbDeferredPublicationState, TYdbPublicationSnapshot>("publication"));
    auto engine = New<TYdbTwoPhaseCommitEngine>(
        environment.Context,
        options,
        MakeYdbTwoPhaseCommitDynamicOptions(*New<TDynamicYdbTwoPhaseCommitSinkParameters>()),
        std::move(writer),
        std::move(publication),
        TYdbStateAccess<TYdbSinkCheckpoint>{
            .Init = BIND([] (IInitContextPtr /*initContext*/) {
            }),
            .Read = BIND([&checkpoint] {
                return checkpoint;
            }),
            .Write = BIND([&] (const TYdbSinkCheckpoint& value) {
                checkpoint = value;
                events.push_back(Format("checkpoint:%v", value.MaxPersistedSeqNo));
            }),
            .IsPersisted = BIND([&checkpoint] (const TMessageId& messageId) {
                return messageId <= checkpoint.MaxPersistedMessageId;
            }),
        });
    engine->Init(environment.StateManager->CreateContext());
    events.clear();

    std::vector<TDistributingTracker> trackers;
    trackers.reserve(2);
    for (i64 id : {1, 2}) {
        trackers.emplace_back([&events, &checkpoint, id] {
            events.push_back(Format("callback%v:%v", id, checkpoint.MaxPersistedSeqNo));
        });
        engine->Distribute(environment.MakeMessage(id, "payload"), trackers.back().AddDestination());
        trackers.back().Activate();
    }
    engine->PreSyncFlush([&] (const TStreamId& /*streamId*/, TSystemTimestamp /*eventTimestamp*/) {
        events.push_back(Format("lag:%v", checkpoint.MaxPersistedSeqNo));
    });

    EXPECT_EQ(events, (std::vector<std::string>{"checkpoint:2", "lag:2", "callback1:2", "lag:2", "callback2:2"}));
}

////////////////////////////////////////////////////////////////////////////////

class TYdbSinkCodecTest
    : public ::testing::TestWithParam<std::tuple<std::string, NYdb::NTopic::ECodec>>
{ };

TEST_P(TYdbSinkCodecTest, SessionUsesConfiguredCodec)
{
    const auto& [codecName, codec] = GetParam();
    TSinkTestEnvironment environment(PublicSinkShape(), /*framing*/ false);
    environment.Context->SinkSpec->SinkClassName = TypeName<TYdbSink>();
    environment.Context->SinkSpec->Parameters->AddChild("codec", NYTree::ConvertToNode(codecName));
    auto sink = environment.CreateSink();
    EXPECT_EQ(environment.Resource->WriterCreationCount, 0);
    sink->Init(environment.StateManager->CreateContext());
    ASSERT_EQ(environment.Resource->WriterSettings.size(), 1u);
    EXPECT_EQ(environment.Resource->WriterSettings.front().Codec_, codec);
}

INSTANTIATE_TEST_SUITE_P(
    SupportedCodecs,
    TYdbSinkCodecTest,
    ::testing::Values(
        std::tuple("raw", NYdb::NTopic::ECodec::RAW),
        std::tuple("gzip", NYdb::NTopic::ECodec::GZIP),
        std::tuple("lzop", NYdb::NTopic::ECodec::LZOP),
        std::tuple("zstd", NYdb::NTopic::ECodec::ZSTD)));

TEST(TYdbSinkCodecTest, RejectsUnsupportedCodecInSchema)
{
    TSinkTestEnvironment environment(PublicSinkShape(), /*framing*/ false);
    environment.Context->SinkSpec->SinkClassName = TypeName<TYdbSink>();
    environment.Context->SinkSpec->Parameters->AddChild("codec", NYTree::ConvertToNode("unsupported"));
    EXPECT_THROW_WITH_SUBSTRING(environment.CreateSink(), "Unsupported codec");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
