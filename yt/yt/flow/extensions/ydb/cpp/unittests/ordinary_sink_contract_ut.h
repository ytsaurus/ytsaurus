#pragma once

#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/ordinary_sink_contract_helpers.h>

#include <yt/yt/flow/extensions/ydb/cpp/helpers.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow::NOrdinarySinkContract {

template <class TPredicate>
bool WaitUntil(TPredicate predicate, TDuration timeout = TDuration::Seconds(20))
{
    const auto deadline = TInstant::Now() + timeout;
    while (!predicate()) {
        if (TInstant::Now() > deadline) {
            return false;
        }
        Sleep(TDuration::MilliSeconds(5));
    }
    return true;
}

template <class TShell>
class TOrdinarySinkContractTest
    : public ::testing::Test
{
protected:
    template <class TSink>
    static TSinkSpecShape MakeShape()
    {
        return TShell::MakeSinkShape(TypeName<TSink>(), "localhost:2135");
    }
};

TYPED_TEST_SUITE_P(TOrdinarySinkContractTest);

TYPED_TEST_P(TOrdinarySinkContractTest, WriteAcknowledgementsPrecedeDistributionCallbacks)
{
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TPlainSink>(), /*framing*/ false);
    environment.WriteState->AutoAcknowledge = false;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    auto firstCallbacks = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto secondCallbacks = environment.Distribute(sink, environment.MakeMessage(2, "two"));
    EXPECT_TRUE(environment.WriteState->Writes.empty());

    sink->Commit();
    ASSERT_EQ(GetSeqNos(environment.WriteState->Writes), (std::vector<i64>{1, 2}));
    EXPECT_EQ(environment.WriteState->Writes[0].Data, "one");
    EXPECT_EQ(environment.WriteState->Writes[1].Data, "two");

    environment.WriteState->AcknowledgeNext();
    environment.Sync(sink);
    EXPECT_EQ(*firstCallbacks, 0);
    sink->Commit();
    EXPECT_EQ(*firstCallbacks, 1);
    EXPECT_EQ(*secondCallbacks, 0);

    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(*secondCallbacks, 0);

    environment.WriteState->AcknowledgeNext();
    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(*firstCallbacks, 1);
    EXPECT_EQ(*secondCallbacks, 1);
}

TYPED_TEST_P(TOrdinarySinkContractTest, RestartRetriesUnacknowledgedWriteUnderSavedProducerAndSeqNo)
{
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TPlainSink>(), /*framing*/ false);
    environment.WriteState->AutoAcknowledge = false;
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(1, "one"));
    auto rejectedCallbacks = environment.Distribute(sink, environment.MakeMessage(2, "two"));
    sink->Commit();
    environment.WriteState->AcknowledgeNext();
    environment.WriteState->PendingWritePromises.front().Set(TError("Write failed"));
    environment.WriteState->PendingWritePromises.clear();
    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(*rejectedCallbacks, 0);

    sink.Reset();
    environment.WriteState->Writes.clear();
    environment.WriteState->AutoAcknowledge = true;
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    ASSERT_EQ(environment.Resource->WriterSettings.size(), 2u);
    const auto& firstSession = environment.Resource->WriterSettings[0];
    const auto& secondSession = environment.Resource->WriterSettings[1];
    EXPECT_EQ(firstSession.ProducerId_, secondSession.ProducerId_);
    EXPECT_EQ(secondSession.MessageGroupId_, secondSession.ProducerId_);
    EXPECT_EQ(secondSession.Path_, TypeParam::ExpectedTopicPath);
    EXPECT_EQ(secondSession.ValidateSeqNo_, true);

    auto persistedCallbacks = environment.Distribute(sink, environment.MakeMessage(1, "one"));
    EXPECT_EQ(*persistedCallbacks, 1);
    auto retriedCallbacks = environment.Distribute(sink, environment.MakeMessage(2, "two"));
    sink->Commit();
    ASSERT_EQ(GetSeqNos(environment.WriteState->Writes), std::vector<i64>{2});
    EXPECT_EQ(environment.WriteState->Writes[0].Data, "two");
    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(*retriedCallbacks, 1);
}

TYPED_TEST_P(TOrdinarySinkContractTest, OrderedFramingBatchesFollowAcceptedLimits)
{
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TFramingSink>(), /*framing*/ true, /*maxRowsPerBatch*/ 2);
    AddSinkParameter(environment.Context->SinkSpec, "write_flow_queue_meta", NYTree::ConvertToNode(true));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    std::vector<std::shared_ptr<int>> callbacks;
    for (i64 id = 1; id <= 3; ++id) {
        callbacks.push_back(environment.Distribute(sink, environment.MakeMessage(id, "payload")));
    }
    environment.Sync(sink);
    sink->Commit();
    ASSERT_EQ(GetSeqNos(environment.WriteState->Writes), (std::vector<i64>{1, 2}));
    const auto& firstBatch = environment.WriteState->Writes[0];
    ASSERT_EQ(firstBatch.MessageMeta.size(), 1u);
    EXPECT_EQ(firstBatch.MessageMeta[0].first, DefaultFlowQueueMetaName);
    EXPECT_EQ(ParseFlowQueueMeta(firstBatch.MessageMeta[0].second).EventTimestampDeltas.size(), 2u);
    environment.Sync(sink);
    sink->Commit();
    for (const auto& callbackCount : callbacks) {
        EXPECT_EQ(*callbackCount, 1);
    }

    const auto acceptedContext = MakeDynamicSinkContext("max_bytes_per_batch", NYTree::ConvertToNode(1));
    sink->Reconfigure(acceptedContext);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicSinkContext("max_bytes_per_batch", NYTree::ConvertToNode(0))), std::exception);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicSinkContext("max_rows_per_batch", NYTree::ConvertToNode("invalid"))), std::exception);
    EXPECT_EQ(DynamicPointerCast<TSinkBase>(sink)->GetDynamicContext(), acceptedContext);

    const auto writesBefore = environment.WriteState->Writes.size();
    for (i64 id = 4; id <= 6; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink);
    sink->Commit();
    EXPECT_EQ(environment.WriteState->Writes.size() - writesBefore, 3u);
}

TYPED_TEST_P(TOrdinarySinkContractTest, AtLeastOnceSessionsRestartProducerAndSeqNo)
{
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TAtLeastOnceSink>(), /*framing*/ false);
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    for (i64 id = 1; id <= 2; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink);
    environment.Distribute(sink, environment.MakeMessage(3, "payload"));
    environment.Sync(sink);
    EXPECT_EQ(GetSeqNos(environment.WriteState->Writes), (std::vector<i64>{1, 2, 3}));

    sink.Reset();
    environment.WriteState->Writes.clear();
    sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());
    environment.Distribute(sink, environment.MakeMessage(4, "payload"));
    environment.Sync(sink);
    EXPECT_EQ(GetSeqNos(environment.WriteState->Writes), std::vector<i64>{1});

    ASSERT_EQ(environment.Resource->WriterSettings.size(), 2u);
    EXPECT_NE(environment.Resource->WriterSettings[0].ProducerId_, environment.Resource->WriterSettings[1].ProducerId_);
}

TYPED_TEST_P(TOrdinarySinkContractTest, AttributesKeepOrderKafkaKeyAndTrailingFlowQueueMeta)
{
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TAtLeastOnceSink>(), /*framing*/ false);
    AddAttributesColumn(&environment, NTableClient::EValueType::Any);
    AddSinkParameter(environment.Context->SinkSpec, "write_flow_queue_meta", NYTree::ConvertToNode(true));
    AddSinkParameter(environment.Context->SinkSpec, "flow_queue_meta_name", NYTree::ConvertToNode("custom_meta"));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    environment.Distribute(sink, MakeAttributedMessage(environment, 1, {{"b", "2"}, {"a", "1"}, {KafkaKeyAttributeName, "key"}}));
    environment.Sync(sink);

    ASSERT_EQ(environment.WriteState->Writes.size(), 1u);
    const auto& messageMeta = environment.WriteState->Writes[0].MessageMeta;
    ASSERT_EQ(messageMeta.size(), 4u);
    EXPECT_EQ(messageMeta[0], (std::pair<std::string, std::string>{KafkaKeyAttributeName, "key"}));
    EXPECT_EQ(messageMeta[1], (std::pair<std::string, std::string>{"a", "1"}));
    EXPECT_EQ(messageMeta[2], (std::pair<std::string, std::string>{"b", "2"}));
    EXPECT_EQ(messageMeta[3].first, "custom_meta");
    EXPECT_EQ(ParseFlowQueueMeta(messageMeta[3].second).EventTimestamp, TSystemTimestamp(1'700'000'000));
}

TYPED_TEST_P(TOrdinarySinkContractTest, InvalidAttributesFailWholeSyncWithoutConsumingSeqNos)
{
    const std::vector<std::pair<TYdbTopicAttributes, std::string>> invalidCases = {
        {{{"custom_meta", "value"}}, "reserved for the flow queue meta"},
        {{{ReservedFlowAttributeNamePrefix + "name", "value"}}, "is reserved"},
        {{{"__partition_key", "value"}}, "is reserved"},
        {{{LegacyFlowQueueMetaName, "value"}}, "is reserved"},
        {{{"name", std::string(MaxYdbMessageMetaSize, 'x')}}, "too large"},
        {{{"name", std::string(MaxYdbMessageMetaSize - std::ssize(std::string("name")), 'x')}}, "too large"},
    };

    i64 nextId = 1;
    TSinkTestEnvironment environment(this->template MakeShape<typename TypeParam::TAtLeastOnceSink>(), /*framing*/ false);
    AddAttributesColumn(&environment, NTableClient::EValueType::Any);
    AddSinkParameter(environment.Context->SinkSpec, "write_flow_queue_meta", NYTree::ConvertToNode(true));
    AddSinkParameter(environment.Context->SinkSpec, "flow_queue_meta_name", NYTree::ConvertToNode("custom_meta"));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    for (const auto& [attributes, expectedError] : invalidCases) {
        environment.Distribute(sink, environment.MakeMessage(nextId++, "valid"));
        environment.Distribute(sink, MakeAttributedMessage(environment, nextId++, attributes));
        EXPECT_THROW_WITH_SUBSTRING(sink->Sync(/*transaction*/ nullptr), expectedError);
        EXPECT_TRUE(environment.WriteState->Writes.empty());
    }

    environment.Distribute(sink, environment.MakeMessage(nextId++, "valid"));
    environment.Sync(sink);
    EXPECT_EQ(GetSeqNos(environment.WriteState->Writes), std::vector<i64>{1});
}

TYPED_TEST_P(TOrdinarySinkContractTest, ConstructionRejectsMissingColumnsNamingTheConcreteSink)
{
    TSinkTestEnvironment missingPayload(this->template MakeShape<typename TypeParam::TPlainSink>(), /*framing*/ false);
    AddSinkParameter(missingPayload.Context->SinkSpec, "payload_column", NYTree::ConvertToNode("absent"));
    EXPECT_THROW_WITH_SUBSTRING(missingPayload.CreateSink(), TypeName<typename TypeParam::TPlainSink>());

    TSinkTestEnvironment missingAttributes(this->template MakeShape<typename TypeParam::TPlainSink>(), /*framing*/ false);
    AddSinkParameter(missingAttributes.Context->SinkSpec, "attributes_column", NYTree::ConvertToNode("absent"));
    EXPECT_THROW_WITH_SUBSTRING(missingAttributes.CreateSink(), "column to write topic message attributes");

    TSinkTestEnvironment scalarAttributes(this->template MakeShape<typename TypeParam::TPlainSink>(), /*framing*/ false);
    AddAttributesColumn(&scalarAttributes, NTableClient::EValueType::String);
    EXPECT_THROW_WITH_SUBSTRING(scalarAttributes.CreateSink(), "should have type any");
}

TYPED_TEST_P(TOrdinarySinkContractTest, AttributeFunctionsEnforceExactReservedNamesAndLimits)
{
    EXPECT_FALSE(TypeParam::IsReservedAttributeName(KafkaKeyAttributeName));
    EXPECT_TRUE(TypeParam::IsReservedAttributeName("__key2"));
    EXPECT_TRUE(TypeParam::IsReservedAttributeName("_ytflow_name"));
    EXPECT_TRUE(TypeParam::IsReservedAttributeName(LegacyFlowQueueMetaName));
    EXPECT_FALSE(TypeParam::IsReservedAttributeName("ytflow"));

    const std::string name = "n";
    const TYdbTopicAttributes largestAttributes{{name, std::string(MaxYdbMessageMetaSize - 1, 'x')}};
    EXPECT_EQ(TypeParam::UnpackAttributes(TypeParam::PackAttributes(largestAttributes)), largestAttributes);
    EXPECT_THROW_WITH_SUBSTRING(
        TypeParam::PackAttributes({{name, std::string(MaxYdbMessageMetaSize, 'x')}}),
        "too large");

    TYdbTopicAttributes mostAttributes;
    for (i64 index = 0; index < MaxYdbMessageMetaItemCount; ++index) {
        mostAttributes.emplace(Format("%v", index), "");
    }
    EXPECT_NO_THROW(TypeParam::ValidateAttributes(mostAttributes));
    mostAttributes.emplace("extra", "");
    EXPECT_THROW_WITH_SUBSTRING(TypeParam::ValidateAttributes(mostAttributes), "Too many");
}

TYPED_TEST_P(TOrdinarySinkContractTest, FramedAtLeastOnceUsesOneAcceptedSnapshotPerUpdate)
{
    TSinkTestEnvironment environment(
        this->template MakeShape<typename TypeParam::TAtLeastOnceFramingSink>(),
        /*framing*/ true,
        /*maxRowsPerBatch*/ 1000,
        /*jobGeneration*/ 10,
        /*asyncInvoker*/ true);
    const auto& invoker = environment.SuspendableInvoker;
    auto run = [&]<class TAction>(TAction action) {
        return BIND(action).AsyncVia(invoker).Run();
    };
    auto sink = NConcurrency::WaitFor(run([&] {
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        return sink;
    })).ValueOrThrow();
    auto writeThreeMessages = [&] (i64 firstId) {
        return run([&environment, sink, firstId] {
            const auto writesBefore = environment.WriteState->Writes.size();
            for (i64 id = firstId; id < firstId + 3; ++id) {
                environment.Distribute(sink, environment.MakeMessage(id, "payload"));
            }
            sink->Sync(/*transaction*/ nullptr);
            return environment.WriteState->Writes.size() - writesBefore;
        });
    };

    NConcurrency::WaitFor(invoker->Suspend()).ThrowOnError();
    sink->Reconfigure(MakeDynamicSinkContext("max_rows_per_batch", NYTree::ConvertToNode(1)));
    auto writesUnderRowLimit = writeThreeMessages(1);
    const auto twoMessageSize = 2 * environment.MakeMessage(0, "payload")->ByteSize;
    const auto byteLimitContext = MakeDynamicSinkContext("max_bytes_per_batch", NYTree::ConvertToNode(twoMessageSize));
    sink->Reconfigure(byteLimitContext);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicSinkContext("max_bytes_per_batch", NYTree::ConvertToNode("invalid"))), std::exception);
    EXPECT_THROW(sink->Reconfigure(MakeDynamicSinkContext("max_rows_per_batch", NYTree::ConvertToNode(0))), std::exception);
    EXPECT_EQ(DynamicPointerCast<TSinkBase>(sink)->GetDynamicContext(), byteLimitContext);
    invoker->Resume();
    EXPECT_EQ(NConcurrency::WaitFor(writesUnderRowLimit).ValueOrThrow(), 3u);

    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = environment.WriteState->Writes.size() + 2;
    auto writesInFlight = writeThreeMessages(4);
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture().WithTimeout(TDuration::Seconds(10))).ThrowOnError();
    sink->Reconfigure(MakeDynamicSinkContext("max_rows_per_batch", NYTree::ConvertToNode(1000)));
    environment.DrainAsync();
    environment.WriteState->AcknowledgeNext();
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_FALSE(writesInFlight.IsSet());
    environment.WriteState->AcknowledgeNext();
    EXPECT_EQ(NConcurrency::WaitFor(writesInFlight).ValueOrThrow(), 2u);

    environment.WriteState->AutoAcknowledge = true;
    EXPECT_EQ(NConcurrency::WaitFor(writeThreeMessages(7)).ValueOrThrow(), 1u);
    EXPECT_EQ(GetSeqNos(environment.WriteState->Writes), (std::vector<i64>{1, 2, 3, 4, 5, 6}));

    NConcurrency::WaitFor(run([&sink] {
        sink.Reset();
    })).ThrowOnError();
}

TYPED_TEST_P(TOrdinarySinkContractTest, OversizedFramedMetadataFailsWithoutLosingAcceptedLimits)
{
    TSinkTestEnvironment environment(
        this->template MakeShape<typename TypeParam::TAtLeastOnceFramingSink>(),
        /*framing*/ true,
        /*maxRowsPerBatch*/ 2000);
    AddSinkParameter(environment.Context->SinkSpec, "write_flow_queue_meta", NYTree::ConvertToNode(true));
    auto sink = environment.CreateSink();
    sink->Init(environment.StateManager->CreateContext());

    i64 nextId = 1;
    for (int index = 0; index < 1500; ++index) {
        environment.Distribute(sink, environment.MakeMessage(nextId++, "payload"));
    }
    EXPECT_THROW_WITH_SUBSTRING(sink->Sync(/*transaction*/ nullptr), "too large");
    EXPECT_TRUE(environment.WriteState->Writes.empty());

    for (int index = 0; index < 1100; ++index) {
        environment.Distribute(sink, environment.MakeMessage(nextId++, "payload"));
    }
    sink->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(GetSeqNos(environment.WriteState->Writes), std::vector<i64>{1});
}

TYPED_TEST_P(TOrdinarySinkContractTest, FailedBatchWriteFailsSyncAfterEarlierAcknowledgements)
{
    TSinkTestEnvironment environment(
        this->template MakeShape<typename TypeParam::TAtLeastOnceFramingSink>(),
        /*framing*/ true,
        /*maxRowsPerBatch*/ 1,
        /*jobGeneration*/ 10,
        /*asyncInvoker*/ true);
    const auto& invoker = environment.SuspendableInvoker;
    auto sink = NConcurrency::WaitFor(BIND([&] {
        auto sink = environment.CreateSink();
        sink->Init(environment.StateManager->CreateContext());
        return sink;
    }).AsyncVia(invoker)
            .Run())
        .ValueOrThrow();

    environment.WriteState->AutoAcknowledge = false;
    environment.WriteState->NotifyAfterWriteCount = 2;
    auto sync = BIND([&environment, sink] {
        environment.Distribute(sink, environment.MakeMessage(1, "payload"));
        environment.Distribute(sink, environment.MakeMessage(2, "payload"));
        sink->Sync(/*transaction*/ nullptr);
    }).AsyncVia(invoker)
        .Run();
    NConcurrency::WaitFor(environment.WriteState->WriteCountReached.ToFuture().WithTimeout(TDuration::Seconds(10))).ThrowOnError();

    environment.WriteState->AcknowledgeNext();
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_FALSE(sync.IsSet());
    environment.WriteState->PendingWritePromises.front().Set(TError("Write failed"));
    EXPECT_FALSE(NConcurrency::WaitFor(sync).IsOK());

    NConcurrency::WaitFor(BIND([&sink] {
        sink.Reset();
    }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();
}

TYPED_TEST_P(TOrdinarySinkContractTest, ClosedSessionResendsUnacknowledgedWritesInOrder)
{
    TFakeTopicServer server;
    server.HoldAcknowledgements();
    server.RejectSessionAfterWrites(2);
    const auto queue = New<NConcurrency::TActionQueue>("WriterRetryTest");
    auto resource = CreateTopicServerResource(
        TypeParam::MakeSinkShape(TypeName<typename TypeParam::TPlainSink>(), server.GetEndpoint()).ResourceClassName,
        server.GetEndpoint());
    auto topicClient = resource->CreateTopicClient(server.GetEndpoint(), "/Root");

    auto writer = TypeParam::CreateWriter(
        std::move(resource),
        std::move(topicClient),
        NYdb::NTopic::TWriteSessionSettings()
            .Path("topic")
            .ProducerId("producer")
            .MessageGroupId("producer")
            .ValidateSeqNo(true)
            .Codec(NYdb::NTopic::ECodec::RAW)
            .CompressionExecutor(CreateThreadPoolExecutorAdapter(queue->GetInvoker())),
        queue->GetInvoker(),
        TestLogger,
        CreateSyncStatusProfiler(TestLogger));

    auto first = writer->Write({.SeqNo = 1, .Data = "one", .MessageMeta = {{"name", "value"}}});
    auto second = writer->Write({.SeqNo = 2, .Data = "two"});

    auto getSessionsWithWrites = [&] {
        std::vector<TRecordedTopicSession> sessions;
        for (auto& session : server.GetSessions()) {
            if (!session.Writes.empty()) {
                sessions.push_back(std::move(session));
            }
        }
        return sessions;
    };
    ASSERT_TRUE(WaitUntil([&] {
        auto sessions = getSessionsWithWrites();
        return sessions.size() == 2 && sessions[0].Closed && sessions[1].Writes.size() == 2;
    })) << server.FormatSessions();
    EXPECT_FALSE(first.IsSet());
    EXPECT_FALSE(second.IsSet());

    const auto resendingSession = getSessionsWithWrites()[1];
    EXPECT_EQ(resendingSession.ProducerId, "producer");
    EXPECT_EQ(resendingSession.Writes[0].SeqNo, 1);
    EXPECT_EQ(resendingSession.Writes[0].Data, "one");
    EXPECT_EQ(resendingSession.Writes[0].MessageMeta, (TYdbMessageMeta{{"name", "value"}}));
    EXPECT_EQ(resendingSession.Writes[1].SeqNo, 2);

    server.Acknowledge(1);
    EXPECT_TRUE(NConcurrency::WaitFor(first).IsOK());
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_FALSE(second.IsSet());
    server.Acknowledge(2);
    EXPECT_TRUE(NConcurrency::WaitFor(second).IsOK());

    writer.Reset();
    EXPECT_TRUE(WaitUntil([&] {
        return server.GetSessions().back().Closed;
    }));
}

REGISTER_TYPED_TEST_SUITE_P(
    TOrdinarySinkContractTest,
    WriteAcknowledgementsPrecedeDistributionCallbacks,
    RestartRetriesUnacknowledgedWriteUnderSavedProducerAndSeqNo,
    OrderedFramingBatchesFollowAcceptedLimits,
    AtLeastOnceSessionsRestartProducerAndSeqNo,
    AttributesKeepOrderKafkaKeyAndTrailingFlowQueueMeta,
    InvalidAttributesFailWholeSyncWithoutConsumingSeqNos,
    ConstructionRejectsMissingColumnsNamingTheConcreteSink,
    AttributeFunctionsEnforceExactReservedNamesAndLimits,
    FramedAtLeastOnceUsesOneAcceptedSnapshotPerUpdate,
    OversizedFramedMetadataFailsWithoutLosingAcceptedLimits,
    FailedBatchWriteFailsSyncAfterEarlierAcknowledgements,
    ClosedSessionResendsUnacknowledgedWritesInOrder);

template <class TShell>
class TSinkHeartbeatContractTest
    : public ::testing::Test
{
protected:
    static constexpr auto LongPeriod = TDuration::Hours(1);
    static constexpr auto ShortPeriod = TDuration::MilliSeconds(20);

    TFakeTopicServer Server;
    TStateManagerMockPtr StateManager = New<TStateManagerMock>();
    const NConcurrency::TActionQueuePtr Queue = New<NConcurrency::TActionQueue>("HeartbeatTest");
    const ISuspendableInvokerPtr Invoker = NConcurrency::CreateSuspendableInvoker(Queue->GetInvoker());
    const TIntrusivePtr<TControlledTimeProvider> TimeProvider = New<TControlledTimeProvider>();
    TTopicServerResourcePtr Resource = CreateTopicServerResource(
        TShell::MakeSinkShape(TypeName<typename TShell::TPlainSink>(), Server.GetEndpoint()).ResourceClassName,
        Server.GetEndpoint());
    TSinkControllerContextPtr Context = CreateContext();
    ISinkControllerPtr Controller;

    void TearDown() override
    {
        DestroyController();
        Context.Reset();
        Resource.Reset();
    }

    template <class TAction>
    void Run(TAction action)
    {
        NConcurrency::WaitFor(BIND(action).AsyncVia(Invoker).Run()).ThrowOnError();
    }

    TSinkControllerContextPtr CreateContext() const
    {
        auto shape = TShell::MakeSinkShape(TypeName<typename TShell::TPlainSink>(), Server.GetEndpoint());
        auto context = New<TSinkControllerContext>();
        context->Logger = TestLogger;
        context->Invoker = Invoker;
        context->TimeProvider = TimeProvider;
        context->StatusProfiler = CreateSyncStatusProfiler(TestLogger);
        context->SinkId = TSinkId("sink");
        context->SinkSpec = New<TSinkSpec>();
        context->SinkSpec->SinkClassName = shape.SinkClassName;
        context->SinkSpec->InputStreamIds = {TStreamId("slow"), TStreamId("fast")};
        for (const auto& [name, value] : shape.ConnectionParameters) {
            context->SinkSpec->Parameters->AddChild(name, NYTree::CloneNode(value));
        }
        context->SinkSpec->Parameters->AddChild("write_flow_queue_meta", NYTree::ConvertToNode(true));
        context->SinkSpec->Parameters->AddChild("update_partition_count_period", NYTree::ConvertToNode(TDuration::MilliSeconds(50)));
        context->StaticResources.emplace(YdbDriverDefaultResourceId, Resource);
        return context;
    }

    static TDynamicSinkControllerContextPtr MakeDynamicContext(const NYTree::INodePtr& period)
    {
        auto context = New<TDynamicSinkControllerContext>();
        context->DynamicSinkSpec = New<TDynamicSinkSpec>();
        context->DynamicSinkSpec->Parameters->AddChild("flow_queue_meta_heartbeat_period", period);
        return context;
    }

    static TDynamicSinkControllerContextPtr MakeDynamicContext(TDuration period)
    {
        return MakeDynamicContext(NYTree::ConvertToNode(period));
    }

    void SaveHeartbeatProducerIdBase(const std::string& producerIdBase)
    {
        StateManager->Set(
            "/heartbeat",
            NYson::ConvertToYsonString(NYTree::BuildYsonNodeFluently()
                    .BeginMap()
                    .Item("producer_id_base")
                    .Value(producerIdBase)
                    .EndMap()));
    }

    std::string ReadHeartbeatProducerIdBase()
    {
        Run([this] {
            StateManager->Sync();
        });
        return NYTree::ConvertTo<TIntrusivePtr<typename TShell::THeartbeatState>>(StateManager->Get("/heartbeat"))->ProducerIdBase;
    }

    void UpdateWatermarks(ui64 slowWatermark = 50, ui64 fastWatermark = 100)
    {
        auto makeWatermarks = [] (ui64 eventWatermark) {
            auto watermarks = New<TWatermarks>();
            watermarks->EventWatermark = TSystemTimestamp(eventWatermark);
            return watermarks;
        };
        auto state = New<TWatermarkState>();
        state->Streams[TStreamId("slow")] = makeWatermarks(slowWatermark);
        state->Streams[TStreamId("fast")] = makeWatermarks(fastWatermark);
        Controller->UpdateWatermarkState(std::move(state));
    }

    void CreateController(TDuration period)
    {
        Run([&] {
            Controller = TRegistry::Get()->CreateSinkController(Context, MakeDynamicContext(period));
        });
    }

    void StartController(TDuration period)
    {
        CreateController(period);
        Run([this] {
            Controller->Init(StateManager->CreateContext());
        });
    }

    void DestroyController()
    {
        Run([this] {
            Controller.Reset();
        });
    }

    void RestartFromPersistedState()
    {
        Run([this] {
            StateManager->Sync();
        });
        DestroyController();
        auto storage = StateManager->GetStorage();
        StateManager = New<TStateManagerMock>();
        StateManager->SetStorage(std::move(storage));
    }

    int GetTickCount() const
    {
        return TimeProvider->TimestampCallCount.load();
    }

    bool WaitForMoreTicks(int count = 5) const
    {
        const auto target = GetTickCount() + count;
        return WaitUntil([&] {
            return GetTickCount() >= target;
        });
    }

    void ExpectNoMoreTicks()
    {
        Run([] {
        });
        Sleep(TDuration::MilliSeconds(100));
        const auto ticks = GetTickCount();
        Sleep(TDuration::MilliSeconds(300));
        EXPECT_EQ(GetTickCount(), ticks);
    }

    bool WaitForWrites(const std::string& producerId, int count = 1) const
    {
        const auto reached = WaitUntil([&] {
            return std::ssize(Server.GetWritesOfProducer(producerId)) >= count;
        });
        EXPECT_TRUE(reached) << producerId << " " << Server.FormatSessions();
        return reached;
    }

    bool WaitForReceiverChannelCount(i64 expectedCount)
    {
        return WaitUntil([&] {
            auto count = NConcurrency::WaitFor(BIND([this] {
                return Controller->GetReceiverChannelCount();
            }).AsyncVia(Invoker)
                    .Run())
                .ValueOrThrow();
            return count == expectedCount;
        });
    }

    TDynamicSinkControllerContextPtr GetAcceptedContext() const
    {
        return DynamicPointerCast<TSinkControllerBase>(Controller)->GetDynamicContext();
    }
};

TYPED_TEST_SUITE_P(TSinkHeartbeatContractTest);

TYPED_TEST_P(TSinkHeartbeatContractTest, PeriodChangesSubsequentRealScheduling)
{
    this->StartController(this->LongPeriod);
    this->UpdateWatermarks();
    Sleep(TDuration::MilliSeconds(200));

    const auto longPeriodTicks = this->GetTickCount();
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(this->GetTickCount(), longPeriodTicks);

    this->Controller->Reconfigure(this->MakeDynamicContext(this->ShortPeriod));
    EXPECT_TRUE(this->WaitForMoreTicks());

    this->Controller->Reconfigure(this->MakeDynamicContext(this->LongPeriod));
    this->ExpectNoMoreTicks();
}

TYPED_TEST_P(TSinkHeartbeatContractTest, InvalidHeartbeatTypePreservesAcceptedSchedule)
{
    this->StartController(this->ShortPeriod);
    this->UpdateWatermarks();
    EXPECT_TRUE(this->WaitForMoreTicks());

    const auto acceptedContext = this->GetAcceptedContext();
    EXPECT_THROW(
        this->Controller->Reconfigure(this->MakeDynamicContext(NYTree::ConvertToNode("invalid"))),
        std::exception);
    EXPECT_EQ(this->GetAcceptedContext(), acceptedContext);
    EXPECT_TRUE(this->WaitForMoreTicks());

    this->Controller->Reconfigure(this->MakeDynamicContext(this->LongPeriod));
    this->ExpectNoMoreTicks();
    const auto stableContext = this->GetAcceptedContext();
    EXPECT_THROW(
        this->Controller->Reconfigure(this->MakeDynamicContext(NYTree::ConvertToNode(TError("invalid")))),
        std::exception);
    EXPECT_EQ(this->GetAcceptedContext(), stableContext);
    this->ExpectNoMoreTicks();
}

TYPED_TEST_P(TSinkHeartbeatContractTest, QueuedPeriodUpdatesApplyInAcceptanceOrder)
{
    this->StartController(this->LongPeriod);
    this->UpdateWatermarks();
    Sleep(TDuration::MilliSeconds(200));

    NConcurrency::WaitFor(this->Invoker->Suspend()).ThrowOnError();
    this->Controller->Reconfigure(this->MakeDynamicContext(this->ShortPeriod));
    const auto contextB = this->MakeDynamicContext(this->LongPeriod);
    this->Controller->Reconfigure(contextB);
    EXPECT_THROW(
        this->Controller->Reconfigure(this->MakeDynamicContext(NYTree::ConvertToNode("invalid"))),
        std::exception);
    EXPECT_EQ(this->GetAcceptedContext(), contextB);
    this->Invoker->Resume();
    this->ExpectNoMoreTicks();

    NConcurrency::WaitFor(this->Invoker->Suspend()).ThrowOnError();
    this->Controller->Reconfigure(this->MakeDynamicContext(this->LongPeriod));
    const auto contextD = this->MakeDynamicContext(this->ShortPeriod);
    this->Controller->Reconfigure(contextD);
    EXPECT_EQ(this->GetAcceptedContext(), contextD);
    this->Invoker->Resume();
    EXPECT_TRUE(this->WaitForMoreTicks());
}

TYPED_TEST_P(TSinkHeartbeatContractTest, WritesCarryWatermarkUnderSavedProducerAndTimestampSeqNo)
{
    this->Server.SetPartitionCount(2);
    this->SaveHeartbeatProducerIdBase("saved");
    this->TimeProvider->Now = 1'000;
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();

    ASSERT_TRUE(this->WaitForWrites("saved-0"));
    ASSERT_TRUE(this->WaitForWrites("saved-1"));

    this->UpdateWatermarks(/*slowWatermark*/ 100, /*fastWatermark*/ 50);

    constexpr ui64 jumpedTime = 500'000;
    this->TimeProvider->Now = jumpedTime;
    auto wroteAfterJump = [&] (const std::string& producerId) {
        for (const auto& write : this->Server.GetWritesOfProducer(producerId)) {
            if (write.SeqNo >= static_cast<i64>(jumpedTime)) {
                return true;
            }
        }
        return false;
    };
    ASSERT_TRUE(WaitUntil([&] {
        return wroteAfterJump("saved-0") && wroteAfterJump("saved-1");
    }));
    EXPECT_EQ(this->ReadHeartbeatProducerIdBase(), "saved");

    for (const auto& session : this->Server.GetSessions()) {
        EXPECT_EQ(session.Path, TypeParam::ExpectedTopicPath);
        EXPECT_THAT(session.ProducerId, testing::AnyOf("saved-0", "saved-1"));
    }
    for (const auto* producerId : {"saved-0", "saved-1"}) {
        i64 previousSeqNo = 999;
        for (const auto& write : this->Server.GetWritesOfProducer(producerId)) {
            EXPECT_GT(write.SeqNo, previousSeqNo);
            previousSeqNo = write.SeqNo;
            EXPECT_EQ(write.UncompressedSize, 0);
            ASSERT_EQ(write.MessageMeta.size(), 1u);
            EXPECT_EQ(write.MessageMeta[0].first, DefaultFlowQueueMetaName);
            auto meta = ParseFlowQueueMeta(write.MessageMeta[0].second);
            EXPECT_EQ(meta.EventWatermark, TSystemTimestamp(50));
            EXPECT_TRUE(meta.PureHeartbeat);
            EXPECT_FALSE(meta.EventTimestamp);
        }
    }
}

TYPED_TEST_P(TSinkHeartbeatContractTest, SeqNoAdvancesPastAFrozenClock)
{
    this->TimeProvider->Now = 1'000;
    this->SaveHeartbeatProducerIdBase("saved");
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();

    ASSERT_TRUE(this->WaitForWrites("saved-0", 2));
    auto writes = this->Server.GetWritesOfProducer("saved-0");
    EXPECT_GE(writes[0].SeqNo, 1'000);
    EXPECT_GT(writes[1].SeqNo, writes[0].SeqNo);
}

TYPED_TEST_P(TSinkHeartbeatContractTest, RestartRecoversGeneratedProducerIdBase)
{
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();

    const auto producerIdBase = this->ReadHeartbeatProducerIdBase();
    ASSERT_FALSE(producerIdBase.empty());
    const auto producerId = producerIdBase + "-0";
    ASSERT_TRUE(this->WaitForWrites(producerId));

    this->RestartFromPersistedState();
    const auto sessionsBeforeRestart = this->Server.GetSessions().size();

    this->TimeProvider->Now = 1'000'000;
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();
    ASSERT_TRUE(WaitUntil([&] {
        return this->Server.GetSessions().size() > sessionsBeforeRestart &&
            !this->Server.GetSessions().back().Writes.empty();
    }));
    const auto restartedSession = this->Server.GetSessions().back();
    EXPECT_EQ(restartedSession.ProducerId, producerId);
    EXPECT_GE(restartedSession.Writes.front().SeqNo, 1'000'000);
    EXPECT_EQ(this->ReadHeartbeatProducerIdBase(), producerIdBase);
}

TYPED_TEST_P(TSinkHeartbeatContractTest, MissingWatermarkStateSkipsHeartbeatsUntilItAppears)
{
    this->SaveHeartbeatProducerIdBase("saved");
    this->StartController(this->ShortPeriod);
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(this->GetTickCount(), 0);
    EXPECT_TRUE(this->Server.GetSessions().empty());

    this->UpdateWatermarks();
    EXPECT_TRUE(this->WaitForWrites("saved-0"));
}

TYPED_TEST_P(TSinkHeartbeatContractTest, DisabledFlowQueueMetaNeverSchedulesHeartbeats)
{
    AddSinkParameter(this->Context->SinkSpec, "write_flow_queue_meta", NYTree::ConvertToNode(false));
    this->StartController(this->ShortPeriod);
    this->UpdateWatermarks();
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(this->GetTickCount(), 0);
    EXPECT_TRUE(this->Server.GetSessions().empty());
    EXPECT_TRUE(this->WaitForReceiverChannelCount(1));
}

TYPED_TEST_P(TSinkHeartbeatContractTest, UnavailableEndpointKeepsTickingWithoutWritersUntilDiscovered)
{
    this->Server.SetPartitionCount(2);
    this->Resource->DiscoveryAvailable = false;
    this->SaveHeartbeatProducerIdBase("saved");
    this->StateManager->Set(
        "/queue_info/v0",
        NYson::ConvertToYsonString(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                .Item("clusters")
                .BeginMap()
                .Item(this->Server.GetEndpoint())
                .BeginMap()
                .Item("partition_count")
                .Value(2)
                .EndMap()
                .EndMap()
                .EndMap()));
    this->StartController(this->ShortPeriod);
    this->UpdateWatermarks();

    EXPECT_TRUE(this->WaitForMoreTicks());
    EXPECT_TRUE(this->Server.GetSessions().empty());
    EXPECT_TRUE(this->WaitForReceiverChannelCount(2));

    this->Resource->DiscoveryAvailable = true;
    EXPECT_TRUE(this->WaitForWrites("saved-0"));
    EXPECT_TRUE(this->WaitForWrites("saved-1"));
}

TYPED_TEST_P(TSinkHeartbeatContractTest, DestructionAfterFailedInitIsClean)
{
    this->CreateController(this->ShortPeriod);
    this->Run([this] {
        EXPECT_THROW_WITH_SUBSTRING(
            this->Controller->Init(New<TFailingInitContext>()),
            "State storage is unavailable");
    });
    this->DestroyController();
    EXPECT_EQ(this->GetTickCount(), 0);
    EXPECT_TRUE(this->Server.GetSessions().empty());
}

TYPED_TEST_P(TSinkHeartbeatContractTest, DestructionClosesHeartbeatSessions)
{
    this->SaveHeartbeatProducerIdBase("saved");
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();
    ASSERT_TRUE(this->WaitForWrites("saved-0"));
    EXPECT_FALSE(this->Server.GetSessions().back().Closed);

    this->DestroyController();
    EXPECT_TRUE(WaitUntil([&] {
        for (const auto& session : this->Server.GetSessions()) {
            if (!session.Closed) {
                return false;
            }
        }
        return true;
    }));
}

TYPED_TEST_P(TSinkHeartbeatContractTest, PartitionCountChangeReplacesEveryWriter)
{
    this->SaveHeartbeatProducerIdBase("saved");
    this->StartController(TDuration::MilliSeconds(50));
    this->UpdateWatermarks();
    ASSERT_TRUE(this->WaitForWrites("saved-0"));

    this->Server.SetPartitionCount(2);
    ASSERT_TRUE(this->WaitForWrites("saved-1"));
    EXPECT_TRUE(WaitUntil([&] {
        int openSessions = 0;
        for (const auto& session : this->Server.GetSessions()) {
            openSessions += session.Closed ? 0 : 1;
        }
        return openSessions == 2;
    }));
}

REGISTER_TYPED_TEST_SUITE_P(
    TSinkHeartbeatContractTest,
    PeriodChangesSubsequentRealScheduling,
    InvalidHeartbeatTypePreservesAcceptedSchedule,
    QueuedPeriodUpdatesApplyInAcceptanceOrder,
    WritesCarryWatermarkUnderSavedProducerAndTimestampSeqNo,
    SeqNoAdvancesPastAFrozenClock,
    RestartRecoversGeneratedProducerIdBase,
    MissingWatermarkStateSkipsHeartbeatsUntilItAppears,
    DisabledFlowQueueMetaNeverSchedulesHeartbeats,
    UnavailableEndpointKeepsTickingWithoutWritersUntilDiscovered,
    DestructionAfterFailedInitIsClean,
    DestructionClosesHeartbeatSessions,
    PartitionCountChangeReplacesEveryWriter);

} // namespace NYT::NFlow::NOrdinarySinkContract
