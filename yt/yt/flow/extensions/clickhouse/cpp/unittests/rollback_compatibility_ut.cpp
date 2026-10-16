#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

TEST(TClickHouseRollbackCompatibilityTest, OldRegistryRejectsShardedSpecWithPendingBatch)
{
    TRegistry oldRegistry;
    oldRegistry.RegisterSink<TClickHouseBatchingSink>();
    oldRegistry.RegisterSink<TAtLeastOnceClickHouseSink>();
    oldRegistry.RegisterSink<TAtMostOnceClickHouseSink>();
    auto stateManager = New<TStateManagerMock>();
    TMutableStateClient<TOrderedBatchingAsyncSinkState> state;
    stateManager->CreateContext()->InitClient<TOrderedBatchingAsyncSinkState>(state, "v0");
    state->ProducerId = "producer";
    state->BatchBounds.push_back(TMessageId("batch"));
    stateManager->Sync();
    const auto before = stateManager->GetStorage();
    auto context = New<TSinkContext>();
    context->SinkSpec = NYTree::ConvertTo<TSinkSpecPtr>(NYson::TYsonStringBuf(
        R"({sink_class_name="NYT::NFlow::TShardedClickHouseBatchingSink";input_stream_ids=[test];parameters={shard_hosts={a=[h1];b=[h2]};table=t};})"));
    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = New<TDynamicSinkSpec>();
    EXPECT_NO_THROW(TRegistry::Get()->ParseSinkParameters(context->SinkSpec));
    bool initialized = false;
    EXPECT_THROW(([&] {
        auto sink = oldRegistry.CreateSink(context, dynamicContext);
        initialized = true;
        sink->Init(stateManager->CreateContext());
    }()),
        TErrorException);
    EXPECT_FALSE(initialized);
    EXPECT_EQ(stateManager->GetStorage(), before);
    EXPECT_EQ(state->BatchBounds, (std::deque<TMessageId>{TMessageId("batch")}));
}

} // namespace
} // namespace NYT::NFlow
