#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/codec.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr MakeStreamSpecs(
    const TStreamId& streamId,
    TStreamSpecId specId,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][specId] = std::move(streamSpec);
    return New<TStreamSpecs>(streamSpecs);
}

TEST(TCodecTest, ParsesMessagesTimersVisitsWatermarks)
{
    auto schema = NTesting::DefaultTestKeySchema();
    auto streamId = TStreamId("input");
    auto specs = MakeStreamSpecs(streamId, TStreamSpecId(5), schema);

    auto key = MakeKey(ui64{42});
    auto message = NTesting::MakeTestMessage(
        streamId,
        key,
        schema,
        [] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64{42}, "key");
        });
    auto timer = NTesting::MakeTestTimer(key, TSystemTimestamp(1700000000));
    auto visit = NTesting::MakeTestVisit(key, streamId);

    NProto::NCompanion::TReqProcessBatch request;
    {
        auto* protoMessage = request.add_messages();
        ToProto(protoMessage->mutable_message(), *message, specs);
        ToProto(protoMessage->mutable_key(), message->Key);
        ToProto(request.add_timers(), *timer);
        ToProto(request.add_visits(), static_cast<const TVisit&>(*visit));
        auto* protoWatermark = request.add_watermarks();
        protoWatermark->set_stream_id("input");
        protoWatermark->set_watermark(123);
    }

    auto input = ParseProcessBatchRequest(request, specs, schema);

    ASSERT_EQ(std::ssize(input.Messages), 1);
    const auto& parsedMessage = input.Messages[0];
    EXPECT_EQ(parsedMessage->MessageId, message->MessageId);
    EXPECT_EQ(parsedMessage->StreamId, streamId);
    EXPECT_EQ(parsedMessage->Key, key);
    EXPECT_EQ(GetColumnValue<ui64>(parsedMessage, "key"), ui64{42});

    ASSERT_EQ(std::ssize(input.Timers), 1);
    EXPECT_EQ(input.Timers[0]->TriggerTimestamp, TSystemTimestamp(1700000000));
    EXPECT_EQ(input.Timers[0]->Key, key);
    // The wire timer carries no key schema; the parser installs the
    // computation's group-by schema.
    EXPECT_EQ(input.Timers[0]->KeySchema, schema);

    ASSERT_EQ(std::ssize(input.Visits), 1);
    EXPECT_EQ(input.Visits[0]->Key, key);
    // The wire visit carries no alignment timestamp; the parser substitutes
    // the system timestamp.
    EXPECT_EQ(input.Visits[0]->AlignmentTimestamp, input.Visits[0]->SystemTimestamp);

    ASSERT_EQ(std::ssize(input.Watermarks), 1);
    EXPECT_EQ(input.Watermarks[TStreamId("input")], TSystemTimestamp(123));
}

TEST(TCodecTest, ParsesStates)
{
    auto key = MakeKey(ui64{7});
    auto stateSchema = NTesting::DefaultTestKeySchema();

    TPayloadBuilder payloadBuilder(stateSchema);
    payloadBuilder.Set(ui64{7}, "key");
    auto payload = payloadBuilder.Finish();

    NProto::NCompanion::TReqProcessBatch request;
    {
        auto* internalState = request.add_internal_states();
        internalState->set_name("counter");
        auto* internalItem = internalState->add_stateitems();
        ToProto(internalItem->mutable_key(), key);
        internalItem->set_reset(false);
        internalItem->set_state("raw_bytes");

        auto* externalState = request.add_external_states();
        externalState->set_name("profile");
        externalState->set_schema(ToProto(NYson::ConvertToYsonString(stateSchema)));
        auto* externalItem = externalState->add_stateitems();
        ToProto(externalItem->mutable_key(), key);
        externalItem->set_reset(false);
        externalItem->set_state(ToProto<TProtobufString>(payload));

        auto* joinedState = request.add_joined_external_states();
        joinedState->set_name("joined");
        joinedState->set_schema(ToProto(NYson::ConvertToYsonString(stateSchema)));
    }

    auto input = ParseProcessBatchRequest(request, nullptr, NTesting::DefaultTestKeySchema());

    ASSERT_TRUE(input.InternalStates.contains("counter"));
    const auto& internalHolder = input.InternalStates["counter"];
    ASSERT_EQ(std::ssize(internalHolder.StateItems), 1);
    EXPECT_EQ(internalHolder.StateItems[0].Key, key);
    EXPECT_FALSE(internalHolder.StateItems[0].Reset);
    EXPECT_EQ(internalHolder.StateItems[0].State, "raw_bytes");

    ASSERT_TRUE(input.ExternalStates.contains("profile"));
    const auto& externalHolder = input.ExternalStates["profile"];
    ASSERT_TRUE(externalHolder.Schema);
    ASSERT_EQ(std::ssize(externalHolder.StateItems), 1);
    EXPECT_EQ(
        GetColumnValue<ui64>(externalHolder.StateItems[0].State, 0),
        ui64{7});

    ASSERT_TRUE(input.JoinedExternalStates.contains("joined"));
    EXPECT_TRUE(input.JoinedExternalStates["joined"].StateItems.empty());
}

TEST(TCodecTest, DuplicateStateNameThrows)
{
    NProto::NCompanion::TReqProcessBatch request;
    request.add_internal_states()->set_name("counter");
    request.add_internal_states()->set_name("counter");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseProcessBatchRequest(request, nullptr, NTesting::DefaultTestKeySchema()),
        "Duplicate state");
}

TEST(TCodecTest, SerializesResponseGroups)
{
    auto schema = NTesting::DefaultTestKeySchema();
    auto streamId = TStreamId("output");
    auto specs = MakeStreamSpecs(streamId, TStreamSpecId(9), schema);

    auto message = NTesting::MakeTestRawMessage(
        streamId,
        schema,
        [] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64{1}, "key");
        });

    std::vector<TOutputGroup> groups;
    auto& group = groups.emplace_back();
    group.Messages.push_back(message);
    group.Distribute = {false};
    group.Timers.push_back(NCompanion::TNewTimer{
        .TriggerTimestamp = TSystemTimestamp(100),
        .EventTimestamp = TSystemTimestamp(50),
        .StreamId = TStreamId("timer_stream"),
    });
    group.ParentIds.push_back(TMessageId("parent-1"));

    NProto::NCompanion::TResponseData data;
    SerializeProcessBatchResponse(&data, groups, {}, {}, specs);

    ASSERT_EQ(data.output_size(), 1);
    const auto& protoGroup = data.output(0);
    ASSERT_EQ(protoGroup.messages_size(), 1);
    EXPECT_EQ(protoGroup.messages(0).stream_spec_id(), 9);
    ASSERT_EQ(protoGroup.distribute_size(), 1);
    EXPECT_FALSE(protoGroup.distribute(0));
    ASSERT_EQ(protoGroup.timers_size(), 1);
    EXPECT_EQ(protoGroup.timers(0).trigger_timestamp(), ui64{100});
    EXPECT_EQ(protoGroup.timers(0).event_timestamp(), ui64{50});
    EXPECT_EQ(protoGroup.timers(0).stream_id(), "timer_stream");
    ASSERT_EQ(protoGroup.parent_ids_size(), 1);
    EXPECT_EQ(protoGroup.parent_ids(0), "parent-1");
}

TEST(TCodecTest, DistributeOmittedWhenAllTrue)
{
    auto schema = NTesting::DefaultTestKeySchema();
    auto streamId = TStreamId("output");
    auto specs = MakeStreamSpecs(streamId, TStreamSpecId(9), schema);

    std::vector<TOutputGroup> groups;
    auto& group = groups.emplace_back();
    group.Messages.push_back(NTesting::MakeTestRawMessage(
        streamId,
        schema,
        [] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64{1}, "key");
        }));
    group.Distribute = {true};
    group.ParentIds.push_back(TMessageId("parent-1"));

    NProto::NCompanion::TResponseData data;
    SerializeProcessBatchResponse(&data, groups, {}, {}, specs);
    EXPECT_EQ(data.output(0).distribute_size(), 0);
}

TEST(TCodecTest, EmptyParentIdsThrow)
{
    std::vector<TOutputGroup> groups;
    groups.emplace_back();

    NProto::NCompanion::TResponseData data;
    EXPECT_THROW_WITH_SUBSTRING(
        SerializeProcessBatchResponse(&data, groups, {}, {}, nullptr),
        "without parent ids");
}

TEST(TCodecTest, SerializesModifiedStates)
{
    auto key = MakeKey(ui64{3});

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    auto& holder = internalStates.emplace_back();
    holder.StateName = "counter";
    holder.StateItems.push_back({.Key = key, .Reset = false, .State = "value"});
    holder.StateItems.push_back({.Key = key, .Reset = true, .State = {}});

    NProto::NCompanion::TResponseData data;
    SerializeProcessBatchResponse(&data, {}, internalStates, {}, nullptr);

    ASSERT_EQ(data.internal_states_size(), 1);
    const auto& protoState = data.internal_states(0);
    EXPECT_EQ(protoState.name(), "counter");
    ASSERT_EQ(protoState.stateitems_size(), 2);
    EXPECT_FALSE(protoState.stateitems(0).reset());
    EXPECT_EQ(protoState.stateitems(0).state(), "value");
    EXPECT_TRUE(protoState.stateitems(1).reset());
}

TEST(TCodecTest, NonResetEmptyStateThrows)
{
    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    auto& holder = internalStates.emplace_back();
    holder.StateName = "counter";
    holder.StateItems.push_back({.Key = MakeKey(ui64{3}), .Reset = false, .State = {}});

    NProto::NCompanion::TResponseData data;
    EXPECT_THROW_WITH_SUBSTRING(
        SerializeProcessBatchResponse(&data, {}, internalStates, {}, nullptr),
        "Empty state value");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
