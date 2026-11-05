#include <yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.h>

#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <string>
#include <vector>

namespace NYT::NFlow::NExample {
namespace {

using namespace NYT::NFlow::NTesting;

TInputMessageConstPtr MakeLineMessage(ui64 key, TStringBuf line)
{
    auto inputSchema = NYTree::ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(
        R"([{name=line;type=string}])")));
    return MakeTestMessage("input", MakeKey(key), inputSchema, [&] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string(line), "line");
    });
}

TEST(TLogParserProcessFunctionTest, ParsesLineAndDropsMalformedSegments)
{
    TTestStateEnvironment env;
    auto function = New<TLogParserProcessFunction>();
    TProcessFunctionTestHarness harness(
        env,
        function,
        TTestRuntimeContextBuilder().RegisterStream<TLogRecordMessage>("records").Build());

    harness.RunEpoch({MakeLineMessage(1, "info:a;garbage;error:b")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "level"), "info");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "text"), "a");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "worst_level_so_far"), "info");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[1].Message, "level"), "error");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[1].Message, "text"), "b");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[1].Message, "worst_level_so_far"), "error");
}

TEST(TLogParserProcessFunctionTest, CarriesWorstSeverityAcrossEpochs)
{
    TTestStateEnvironment env;
    auto function = New<TLogParserProcessFunction>();
    TProcessFunctionTestHarness harness(
        env,
        function,
        TTestRuntimeContextBuilder().RegisterStream<TLogRecordMessage>("records").Build());

    harness.RunEpoch({MakeLineMessage(1, "error:boom")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "level"), "error");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "text"), "boom");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "worst_level_so_far"), "error");

    harness.RunEpoch({MakeLineMessage(1, "info:ok")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "level"), "info");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "text"), "ok");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "worst_level_so_far"), "error");
}

} // namespace
} // namespace NYT::NFlow::NExample
