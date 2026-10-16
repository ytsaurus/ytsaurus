#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/flow/library/cpp/common/message.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr auto ValidTs = TSystemTimestamp(1500000000);
constexpr auto TooLargeTs = TSystemTimestamp(MaxAdequateTimestamp.Underlying() + 1);

////////////////////////////////////////////////////////////////////////////////

TEST(TMessageTest, Move)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""([{name="order_id"; type="uint64";};])""")));
    auto messageId = TMessageId(std::string(30, 'x'));

    TMessageBuilder builder("stream", schema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
    builder.SetMessageId(messageId);
    auto message = builder.Finish();

    auto movedMessage = std::move(message);

    EXPECT_EQ(movedMessage.MessageId, messageId);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_EQ(message.MessageId, TMessageId(""));

    EXPECT_TRUE(movedMessage.Payload);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_FALSE(message.Payload);

    EXPECT_TRUE(movedMessage.PayloadSchema);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_FALSE(message.PayloadSchema);
}

TEST(TMessageTest, ValidateMessageSchema)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64"; sort_order="ascending";};
            {name="is_click"; type="boolean";};
        ]
    )""")));

    TMessageBuilder builder("stream", schema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
    builder.Payload().SetValue(NTableClient::MakeUnversionedBooleanValue(true, 1));
    builder.SetMessageId(TMessageId("<id>"));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    auto message = builder.Finish();

    EXPECT_NO_THROW(ValidateMessage(message));
    EXPECT_NO_THROW(ValidateMessage(message, {.ExpectedSchema = schema}));

    message.PayloadSchema = nullptr;
    EXPECT_NO_THROW(ValidateMessage(message, {.AllowEmptySchema = true}));
    EXPECT_THROW(ValidateMessage(message, {.AllowEmptySchema = false}), TErrorException);

    message.PayloadSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64"; sort_order="ascending";};
            {name="ISCLK"; type="boolean";};
        ]
    )""")));
    EXPECT_NO_THROW(ValidateMessage(message));
    EXPECT_THROW(ValidateMessage(message, {.ExpectedSchema = schema}), TErrorException);

    message.PayloadSchema = schema;
    EXPECT_NO_THROW(ValidateMessage(message));
}

TEST(TMessageTest, ValidateTimestampAdequacy)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""([{name="order_id"; type="uint64";};])""")));

    auto createMessage = [&] (TSystemTimestamp systemTs, TSystemTimestamp eventTs, TSystemTimestamp alignmentTs) {
        TMessageBuilder builder("stream", schema);
        builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
        builder.SetMessageId(TMessageId("<id>"));
        builder.SetSystemTimestamp(systemTs);
        builder.SetAlignmentTimestamp(alignmentTs);
        builder.SetEventTimestamp(eventTs);
        auto message = builder.Finish();
        return message;
    };

    // Valid timestamps.
    EXPECT_NO_THROW(ValidateMessage(createMessage(ValidTs, ValidTs, ValidTs)));
    EXPECT_NO_THROW(ValidateMessage(createMessage(MaxAdequateTimestamp, MaxAdequateTimestamp, MaxAdequateTimestamp)));

    // Invalid timestamps.
    for (auto invalidTs : {ZeroSystemTimestamp, TooLargeTs, InfinitySystemTimestamp}) {
        EXPECT_THROW(ValidateMessage(createMessage(invalidTs, ValidTs, ValidTs)), TErrorException);
        EXPECT_THROW(ValidateMessage(createMessage(ValidTs, invalidTs, ValidTs)), TErrorException);
        EXPECT_THROW(ValidateMessage(createMessage(ValidTs, ValidTs, invalidTs)), TErrorException);
    }
}

TEST(TMessageTest, ValidateIdIndexEqualityInvariant)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {"name" = "key"; "type" = "uint64";};
            {"name" = "value"; "type" = "string";};
            {"name" = "second_value"; "type" = "int64";};
        ]
    )""")));
    YT_VERIFY(schema);

    TMessage message;
    message.MessageId = TMessageId{"id123"};
    message.StreamId = "stream";
    message.SystemTimestamp = ValidTs;
    message.AlignmentTimestamp = ValidTs;
    message.EventTimestamp = ValidTs;
    message.PayloadSchema = schema;

    // Check that ValidateMessage throw only if invariant is broken.
    for (const bool breakInvariant : {false, true}) {
        // Can't use PayloadBuilder for creating invalid payload.
        // There is broken ivariant about value.Id and value_index equality.
        NTableClient::TUnversionedOwningRowBuilder builder(3);
        if (breakInvariant) {
            builder.AddValue(NTableClient::MakeUnversionedStringValue("abc", 1));
        }
        builder.AddValue(NTableClient::MakeUnversionedUint64Value(5, 0));
        if (!breakInvariant) {
            builder.AddValue(NTableClient::MakeUnversionedStringValue("abc", 1));
        }
        builder.AddValue(NTableClient::MakeUnversionedInt64Value(34, 2));
        message.Payload = TPayload{TPayload::TUnderlying(builder.FinishRow())};

        if (breakInvariant) {
            EXPECT_THROW(ValidateMessage(message), TErrorException);
        } else {
            ValidateMessage(message);
        }
    }
}

TEST(TMessageTest, ValidateBigValue)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="big_string"; type="string";};
        ]
    )""")));

    TMessageBuilder builder("stream", schema);
    std::string longString("a", 20_MB);
    builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(longString), "big_string");
    builder.SetMessageId(TMessageId("<id>"));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    auto message = builder.Finish();

    EXPECT_THROW(ValidateMessage(message), TErrorException);
    EXPECT_NO_THROW(ValidateMessage(message, {.ValidateValues = false}));
}

TEST(TMessageTest, ValidateBigMessage)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="first_string"; type="string";};
            {name="second_string"; type="string";};
        ]
    )""")));

    {
        TMessageBuilder builder("stream", schema);
        std::string longString("a", 10_MB);
        builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(longString), "first_string");
        builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(longString), "second_string");
        builder.SetMessageId(TMessageId("<id>"));
        builder.SetSystemTimestamp(ValidTs);
        builder.SetAlignmentTimestamp(ValidTs);
        builder.SetEventTimestamp(ValidTs);
        auto message = builder.Finish();
        EXPECT_THROW(ValidateMessage(message), TErrorException);
        EXPECT_NO_THROW(ValidateMessage(message, {.ValidateValues = false}));
    }
    {
        TMessageBuilder builder("stream", schema);
        std::string longString("a", 5_MB);
        builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(longString), "first_string");
        builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue(longString), "second_string");
        builder.SetMessageId(TMessageId("<id>"));
        builder.SetSystemTimestamp(ValidTs);
        builder.SetAlignmentTimestamp(ValidTs);
        builder.SetEventTimestamp(ValidTs);
        auto message = builder.Finish();
        EXPECT_NO_THROW(ValidateMessage(message));
    }
}

TEST(TMessageTest, InputMessage)
{
    auto computationId = TComputationId("C1");
    auto messageId = TMessageId("msg1");
    ui64 bannerId = 123;

    auto sourceSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));
    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedUint64Value(bannerId, 0));
    builder.Payload().SetValue(MakeUnversionedBooleanValue(true, 1));
    builder.SetMessageId(messageId);
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    auto message = builder.Finish();

    auto keySchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="banner_id"; type="uint64";};
        ]
    )""")));
    TPayloadBuilder expectedBuilder(keySchema);
    expectedBuilder.SetValue(MakeUnversionedUint64Value(bannerId, 0));
    auto key = TKey(expectedBuilder.Finish().Underlying());

    TInputMessageConstPtr inputMessage = New<TInputMessage>(std::move(message), key);

    EXPECT_EQ(inputMessage->MessageId, messageId);
    EXPECT_EQ(inputMessage->Key.Underlying(), key.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
