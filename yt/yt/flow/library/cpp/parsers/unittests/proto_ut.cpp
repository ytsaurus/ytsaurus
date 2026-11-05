#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/parsers/proto.h>

#include <yt/yt/flow/library/cpp/parsers/unittests/proto/test_record.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {
namespace {

using namespace NDetail;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DataColumn = "data";
constexpr TStringBuf MalformedProto = "\x08";

using TTestProto = NTest::TTestRecordProto;
using TTestRequiredProto = NTest::TTestRequiredRecordProto;

TMessage MakeMessage(std::optional<TStringBuf> data)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema(std::string(DataColumn), ESimpleLogicalValueType::String),
    });

    TMessageBuilder builder(TStreamId("input"), std::move(schema));
    if (data) {
        builder.Payload().SetValue(MakeUnversionedStringValue(*data), DataColumn);
    }
    return builder.Finish();
}

TString SerializeRecord(i64 value)
{
    TTestProto record;
    record.set_value(value);
    return record.SerializeAsString();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProtoColumnParsingTest, PropagatingHookErrorsDispatchesParsedProto)
{
    auto data = SerializeRecord(42);
    auto message = MakeMessage(data);

    std::vector<i64> values;
    int unparsedCount = 0;

    ParseProtoColumnPropagatingHookErrors<TTestProto>(
        message,
        DataColumn,
        [&] (TTestProto&& proto) {
            values.push_back(proto.value());
        },
        [&] (TError /*error*/) {
            ++unparsedCount;
        });

    EXPECT_EQ(values, std::vector<i64>{42});
    EXPECT_EQ(unparsedCount, 0);
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsReportsEmptyData)
{
    auto message = MakeMessage(std::nullopt);

    int protoCount = 0;
    std::vector<TError> errors;

    ParseProtoColumnPropagatingHookErrors<TTestProto>(
        message,
        DataColumn,
        [&] (TTestProto&&) {
            ++protoCount;
        },
        [&] (TError error) {
            errors.push_back(std::move(error));
        });

    EXPECT_EQ(protoCount, 0);
    ASSERT_EQ(std::ssize(errors), 1);
    EXPECT_TRUE(ToString(errors[0]).Contains("empty data")) << ToString(errors[0]);
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsReportsMalformedProto)
{
    auto message = MakeMessage(MalformedProto);

    int protoCount = 0;
    int unparsedCount = 0;

    ParseProtoColumnPropagatingHookErrors<TTestProto>(
        message,
        DataColumn,
        [&] (TTestProto&&) {
            ++protoCount;
        },
        [&] (TError /*error*/) {
            ++unparsedCount;
        });

    EXPECT_EQ(protoCount, 0);
    EXPECT_EQ(unparsedCount, 1);
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsLetsMutatedStateFail)
{
    auto data = SerializeRecord(7);
    auto message = MakeMessage(data);

    i64 state = 0;
    int unparsedCount = 0;

    EXPECT_THROW(
        ParseProtoColumnPropagatingHookErrors<TTestProto>(
            message,
            DataColumn,
            [&] (TTestProto&& proto) {
                state += proto.value();
                THROW_ERROR_EXCEPTION("Emitting the output message failed");
            },
            [&] (TError /*error*/) {
                ++unparsedCount;
            }),
        std::exception);

    EXPECT_EQ(state, 7);
    EXPECT_EQ(unparsedCount, 0);
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsAttachesDataSizeWithoutInitializationError)
{
    auto message = MakeMessage(MalformedProto);

    std::vector<TError> errors;

    ParseProtoColumnPropagatingHookErrors<TTestProto>(
        message,
        DataColumn,
        [&] (TTestProto&&) {
        },
        [&] (TError error) {
            errors.push_back(std::move(error));
        });

    ASSERT_EQ(std::ssize(errors), 1);
    EXPECT_EQ(errors[0].Attributes().Get<i64>("data_size"), std::ssize(MalformedProto));
    EXPECT_FALSE(errors[0].Attributes().Find<TString>("initialization_error").has_value());
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsAttachesInitializationErrorForMissingRequiredField)
{
    auto message = MakeMessage(TStringBuf(""));

    std::vector<TError> errors;

    ParseProtoColumnPropagatingHookErrors<TTestRequiredProto>(
        message,
        DataColumn,
        [&] (TTestRequiredProto&&) {
        },
        [&] (TError error) {
            errors.push_back(std::move(error));
        });

    ASSERT_EQ(std::ssize(errors), 1);
    EXPECT_EQ(errors[0].Attributes().Get<i64>("data_size"), 0);
    EXPECT_TRUE(errors[0].Attributes().Find<TString>("initialization_error").has_value());
}

TEST(TProtoColumnParsingTest, PropagatingHookErrorsReportsWrongDataColumnThroughHook)
{
    auto data = SerializeRecord(7);
    auto message = MakeMessage(data);

    int protoCount = 0;
    std::vector<TError> errors;

    EXPECT_NO_THROW(
        ParseProtoColumnPropagatingHookErrors<TTestProto>(
            message,
            "wrong_column",
            [&] (TTestProto&&) {
                ++protoCount;
            },
            [&] (TError error) {
                errors.push_back(std::move(error));
            }));

    EXPECT_EQ(protoCount, 0);
    ASSERT_EQ(std::ssize(errors), 1);
    EXPECT_EQ(errors[0].Attributes().Get<TString>("data_column"), "wrong_column");
}

TEST(TProtoColumnParsingTest, RoutingHookErrorsReportsHookError)
{
    auto data = SerializeRecord(7);
    auto message = MakeMessage(data);

    std::vector<TError> errors;

    EXPECT_NO_THROW(
        ParseProtoColumnRoutingHookErrors<TTestProto>(
            message,
            DataColumn,
            [&] (TTestProto&&) {
                THROW_ERROR_EXCEPTION("Emitting the output message failed");
            },
            [&] (TError error) {
                errors.push_back(std::move(error));
            }));

    ASSERT_EQ(std::ssize(errors), 1);
    EXPECT_TRUE(ToString(errors[0]).Contains("Emitting the output message failed")) << ToString(errors[0]);
}

TEST(TProtoColumnParsingTest, RoutingHookErrorsReportsMalformedProto)
{
    auto message = MakeMessage(MalformedProto);

    int protoCount = 0;
    int unparsedCount = 0;

    ParseProtoColumnRoutingHookErrors<TTestProto>(
        message,
        DataColumn,
        [&] (TTestProto&&) {
            ++protoCount;
        },
        [&] (TError /*error*/) {
            ++unparsedCount;
        });

    EXPECT_EQ(protoCount, 0);
    EXPECT_EQ(unparsedCount, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
