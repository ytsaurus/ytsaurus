#include <yt/yt/flow/library/cpp/computation/message_filter.h>

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MakeSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="user_id"; type="string"; sort_order="ascending";};
            {name="value"; type="int64";};
        ]
    )""")));
}

struct TMessageParams
{
    std::string StreamId = "s1";
    std::string UserId = "u1";
    i64 Value = 0;
    ui64 SystemTimestamp = 1000;
    ui64 EventTimestamp = 1000;
    ui64 AlignmentTimestamp = 1000;
};

TMessage MakeMessage(const TTableSchemaPtr& schema, const TMessageParams& params)
{
    TMessageBuilder builder(TStreamId(params.StreamId), schema);
    builder.Payload().SetValue(MakeUnversionedStringValue(params.UserId, 0));
    builder.Payload().SetValue(MakeUnversionedInt64Value(params.Value, 1));
    builder.SetMessageId(TMessageId("m1"));
    builder.SetSystemTimestamp(TSystemTimestamp(params.SystemTimestamp));
    builder.SetEventTimestamp(TSystemTimestamp(params.EventTimestamp));
    builder.SetAlignmentTimestamp(TSystemTimestamp(params.AlignmentTimestamp));
    return builder.Finish();
}

TInputMessageConstPtr MakeInputMessage(const TTableSchemaPtr& schema, const TMessageParams& params)
{
    return New<TInputMessage>(MakeMessage(schema, params), TKey());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMessageFilterTest, DisabledWhenNoExpression)
{
    EXPECT_FALSE(CreateMessageFilter(std::nullopt)->IsEnabled());
    EXPECT_FALSE(CreateMessageFilter(std::optional<std::string>(""))->IsEnabled());
    EXPECT_TRUE(CreateMessageFilter("value > 0")->IsEnabled());
}

TEST(TMessageFilterTest, Reconfigure)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter(std::nullopt);
    EXPECT_FALSE(filter->IsEnabled());

    filter->Reconfigure("value > 10");
    EXPECT_TRUE(filter->IsEnabled());
    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema, {.Value = 11})));

    filter->Reconfigure("value < 10");
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.Value = 11})));

    filter->Reconfigure(std::nullopt);
    EXPECT_FALSE(filter->IsEnabled());
}

TEST(TMessageFilterTest, BlacklistKey)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter("user_id = \"bad\"");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema, {.UserId = "bad"})));
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.UserId = "good"})));
}

TEST(TMessageFilterTest, BlacklistStream)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter("[$stream_id] = \"s2\"");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema, {.StreamId = "s2"})));
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.StreamId = "s1"})));
}

TEST(TMessageFilterTest, TimestampCutoff)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter("[$system_timestamp] < 1000u");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema, {.SystemTimestamp = 999})));
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.SystemTimestamp = 1000})));
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.SystemTimestamp = 1001})));
}

TEST(TMessageFilterTest, PayloadColumnPredicate)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter("value > 10");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema, {.Value = 11})));
    EXPECT_FALSE(filter->ShouldSkip(MakeMessage(schema, {.Value = 10})));
}

TEST(TMessageFilterTest, Partition)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter("user_id = \"bad\"");
    ASSERT_TRUE(filter->IsEnabled());

    std::vector<TInputMessageConstPtr> messages;
    messages.push_back(MakeInputMessage(schema, {.UserId = "good"}));
    messages.push_back(MakeInputMessage(schema, {.UserId = "bad"}));
    messages.push_back(MakeInputMessage(schema, {.UserId = "good"}));

    auto result = filter->Partition(messages);
    EXPECT_EQ(2u, result.Kept.size());
    ASSERT_EQ(1u, result.Skipped.size());
    EXPECT_EQ("bad", GetColumnValue<std::string>(result.Skipped[0], "user_id"));
}

TEST(TMessageFilterTest, PartitionDisabledKeepsAll)
{
    auto schema = MakeSchema();
    auto filter = CreateMessageFilter(std::nullopt);

    std::vector<TInputMessageConstPtr> messages;
    messages.push_back(MakeInputMessage(schema, {.UserId = "good"}));
    messages.push_back(MakeInputMessage(schema, {.UserId = "bad"}));

    auto result = filter->Partition(messages);
    EXPECT_EQ(2u, result.Kept.size());
    EXPECT_TRUE(result.Skipped.empty());
}

TEST(TMessageFilterTest, SchemaCacheReuse)
{
    // The same filter must work over two distinct payload schemas.
    auto schema1 = MakeSchema();
    auto schema2 = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="user_id"; type="string"; sort_order="ascending";};
            {name="value"; type="int64";};
            {name="extra"; type="string";};
        ]
    )""")));

    auto filter = CreateMessageFilter("user_id = \"bad\"");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_TRUE(filter->ShouldSkip(MakeMessage(schema1, {.UserId = "bad"})));

    TMessageBuilder builder(TStreamId("s1"), schema2);
    builder.Payload().SetValue(MakeUnversionedStringValue("bad", 0));
    builder.Payload().SetValue(MakeUnversionedInt64Value(0, 1));
    builder.Payload().SetValue(MakeUnversionedStringValue("x", 2));
    builder.SetMessageId(TMessageId("m1"));
    EXPECT_TRUE(filter->ShouldSkip(builder.Finish()));
}

TEST(TMessageFilterTest, NonBooleanResultThrows)
{
    auto schema = MakeSchema();
    // Expression yields an int64, not a boolean => misconfiguration.
    auto filter = CreateMessageFilter("value + 1");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_THROW(filter->ShouldSkip(MakeMessage(schema, {.Value = 5})), TErrorException);
}

TEST(TMessageFilterTest, NullResultThrows)
{
    auto schema = MakeSchema();
    // A NULL result is a misconfigured expression, not silently kept.
    auto filter = CreateMessageFilter("if(value > 1000000, true, null)");
    ASSERT_TRUE(filter->IsEnabled());

    EXPECT_THROW(filter->ShouldSkip(MakeMessage(schema, {.Value = 5})), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
