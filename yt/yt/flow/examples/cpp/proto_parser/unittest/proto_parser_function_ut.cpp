#include <yt/yt/flow/examples/cpp/proto_parser/lib/proto_parser_function.h>

#include <yt/yt/flow/library/cpp/process_function/testing/unittest.h>

#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow::NExample {
namespace {

using namespace NTesting;

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr InputSchema()
{
    return NYTree::ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(
        "[{name=data;type=string}]")));
}

TInputMessageConstPtr MakeProtoMessage(const TKey& key, TStringBuf level, TStringBuf text)
{
    TLogRecordProto proto;
    proto.set_level(level);
    proto.set_text(text);

    return MakeTestMessage("input", key, InputSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(proto.SerializeAsString(), "data");
    });
}

TInputMessageConstPtr MakeMalformedMessage(const TKey& key)
{
    return MakeTestMessage("input", key, InputSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("\xff\xff\xff\xff"), "data");
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProtoParserFunctionTest, ParsesRecordsAndAccumulatesPerLevelCounts)
{
    TTestStateEnvironment stateEnvironment;
    TProcessFunctionTestHarness harness(
        stateEnvironment,
        New<TProtoLogParserFunction>(),
        TTestRuntimeContextBuilder().RegisterStream<TLogRecordMessage>("records").Build());

    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({
        MakeProtoMessage(key, "info", "started"),
        MakeProtoMessage(key, "warning", "slow"),
    });

    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "level"), "info");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "text"), "started");
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "seen_at_level"), 1);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[1].Message, "level"), "warning");
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[1].Message, "seen_at_level"), 1);

    harness.RunEpoch({MakeProtoMessage(key, "info", "connected")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "seen_at_level"), 2);
}

TEST(TProtoParserFunctionTest, DropsMalformedRecords)
{
    TTestStateEnvironment stateEnvironment;
    TProcessFunctionTestHarness harness(
        stateEnvironment,
        New<TProtoLogParserFunction>(),
        TTestRuntimeContextBuilder().RegisterStream<TLogRecordMessage>("records").Build());

    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({MakeMalformedMessage(key)});

    EXPECT_TRUE(harness.GetMessages().empty());

    harness.RunEpoch({MakeProtoMessage(key, "info", "started")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "seen_at_level"), 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NExample
