#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batch.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <util/generic/map.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr MakeStreamSpecs(const TStreamId& streamId, const TTableSchemaPtr& schema, TStreamSpecId specId)
{
    auto streamSpecsMap = THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>();
    auto spec = New<TStreamSpec>();
    spec->Schema = schema;
    streamSpecsMap[streamId].emplace(specId, std::move(spec));
    return New<TStreamSpecs>(streamSpecsMap);
}

TComputationStreamSpecStoragePtr MakeComputationStreamSpecStorage(const TStreamSpecsPtr& specs)
{
    return New<TComputationStreamSpecStorage>(
        specs,
        /*groupBySchema*/ New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
}

TMessage MakeMessage(const TStreamId& streamId, const TTableSchemaPtr& schema, const TMessageId& messageId)
{
    TMessageBuilder builder(streamId, schema);
    builder.Payload().SetValue(MakeUnversionedUint64Value(123, 0));
    builder.Payload().SetValue(MakeUnversionedStringValue("hello", 1));
    builder.SetMessageId(messageId);
    builder.SetSystemTimestamp(TSystemTimestamp(1000));
    builder.SetAlignmentTimestamp(TSystemTimestamp(2000));
    builder.SetEventTimestamp(TSystemTimestamp(3000));
    return builder.Finish();
}

TOutputMessageConstPtr MakeOutputMessage(
    const TStreamId& streamId,
    const TTableSchemaPtr& schema,
    const TComputationStreamSpecStoragePtr& specStorage,
    const TMessageId& messageId)
{
    return New<TOutputMessage>(MakeMessage(streamId, schema, messageId), specStorage);
}

TSharedRef SerializeMessages(const std::deque<TOutputMessageConstPtr>& messages, const TStreamSpecsPtr& specs)
{
    TMessageBatchSerializer serializer(specs);
    for (const auto& message : messages) {
        serializer.AddMessage(message);
    }
    return serializer.Finish();
}

void ExpectMessagesEqual(const TMessage& actual, const TMessage& expected)
{
    EXPECT_EQ(actual.MessageId, expected.MessageId);
    EXPECT_EQ(actual.SystemTimestamp, expected.SystemTimestamp);
    EXPECT_EQ(actual.AlignmentTimestamp, expected.AlignmentTimestamp);
    EXPECT_EQ(actual.EventTimestamp, expected.EventTimestamp);
    EXPECT_EQ(actual.StreamId, expected.StreamId);
    EXPECT_EQ(actual.PayloadSchema, expected.PayloadSchema);
    EXPECT_EQ(
        ConvertToYsonString(actual.Payload.Underlying(), EYsonFormat::Text).ToString(),
        ConvertToYsonString(expected.Payload.Underlying(), EYsonFormat::Text).ToString());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMessageBatchTest, RoundTrip)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name="a"; type="uint64";}; {name="b"; type="string";}])")));
    auto specs = MakeStreamSpecs("stream", schema, TStreamSpecId(0));
    auto specStorage = MakeComputationStreamSpecStorage(specs);

    std::deque<TOutputMessageConstPtr> messages;
    messages.push_back(MakeOutputMessage("stream", schema, specStorage, TMessageId("msg-0")));
    messages.push_back(MakeOutputMessage("stream", schema, specStorage, TMessageId("msg-1-longer-id")));
    messages.push_back(MakeOutputMessage("stream", schema, specStorage, TMessageId("m2")));

    auto buffer = SerializeMessages(messages, specs);
    auto parsed = ParseMessageBatch(buffer, specs);

    ASSERT_EQ(parsed.size(), messages.size());
    for (size_t index = 0; index < messages.size(); ++index) {
        ExpectMessagesEqual(parsed[index], *messages[index]);
    }
}

TEST(TMessageBatchTest, Empty)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name="a"; type="uint64";}])")));
    auto specs = MakeStreamSpecs("stream", schema, TStreamSpecId(0));

    // An empty batch still carries the version header and round-trips to no messages.
    auto emptyBatch = SerializeMessages({}, specs);
    EXPECT_FALSE(emptyBatch.empty());
    EXPECT_TRUE(ParseMessageBatch(emptyBatch, specs).empty());
    // A wholly empty ref (no attachment at all) also parses to no messages.
    EXPECT_TRUE(ParseMessageBatch(TRef(), specs).empty());
}

// Canonical wire encoding of MakeMessage(schema, "msg-0") in format version 1.
//
// If this test fails, the message batch wire format has changed, which is
// backward-incompatible: a peer on the old format will misparse the new bytes. Do NOT just
// re-capture these bytes — you most likely want a new protocol version instead (bump
// CurrentMessageBatchFormatVersion in message_batch.cpp and dispatch parsing by the version).
// clang-format off
constexpr unsigned char CanonicalBatchV1[] = {
    0x01,                                           // format version = 1
    0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // system timestamp = 1000
    0xd0, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // alignment timestamp = 2000
    0xb8, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // event timestamp = 3000
    0x00,                                           // stream spec id = 0
    0x05, 0x6d, 0x73, 0x67, 0x2d, 0x30,             // message id length = 5, "msg-0"
    0x00, 0x02, 0x00, 0x04, 0x7b, 0x01, 0x10, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // payload row [123u, "hello"]
};
// clang-format on

TEST(TMessageBatchTest, CanonicalV1RoundTrip)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name="a"; type="uint64";}; {name="b"; type="string";}])")));
    auto specs = MakeStreamSpecs("stream", schema, TStreamSpecId(0));
    auto specStorage = MakeComputationStreamSpecStorage(specs);

    TRef canonical(reinterpret_cast<const char*>(CanonicalBatchV1), sizeof(CanonicalBatchV1));

    // The canonical bytes parse into exactly the known message.
    auto parsed = ParseMessageBatch(canonical, specs);
    ASSERT_EQ(parsed.size(), 1u);
    ExpectMessagesEqual(parsed[0], MakeMessage("stream", schema, TMessageId("msg-0")));

    // The message re-serializes byte-for-byte back to the canonical bytes.
    std::deque<TOutputMessageConstPtr> messages;
    messages.push_back(MakeOutputMessage("stream", schema, specStorage, TMessageId("msg-0")));
    auto reserialized = SerializeMessages(messages, specs);
    EXPECT_EQ(
        TStringBuf(reserialized.Begin(), reserialized.Size()),
        TStringBuf(canonical.Begin(), canonical.Size()));
}

TEST(TMessageBatchTest, UnsupportedVersionThrows)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name="a"; type="uint64";}])")));
    auto specs = MakeStreamSpecs("stream", schema, TStreamSpecId(0));

    const char buffer[] = {0x7f}; // A single-byte varint version unknown to the parser.
    EXPECT_THROW(ParseMessageBatch(TRef(buffer, sizeof(buffer)), specs), std::exception);
}

TEST(TMessageBatchTest, TruncatedThrows)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name="a"; type="uint64";}; {name="b"; type="string";}])")));
    auto specs = MakeStreamSpecs("stream", schema, TStreamSpecId(0));
    auto specStorage = MakeComputationStreamSpecStorage(specs);

    std::deque<TOutputMessageConstPtr> messages;
    messages.push_back(MakeOutputMessage("stream", schema, specStorage, TMessageId("msg-0")));
    auto buffer = SerializeMessages(messages, specs);

    auto truncated = buffer.Slice(buffer.Begin(), buffer.Begin() + 4);
    EXPECT_THROW(ParseMessageBatch(truncated, specs), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
