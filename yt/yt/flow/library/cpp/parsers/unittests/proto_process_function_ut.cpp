#include <yt/yt/flow/library/cpp/parsers/proto.h>

#include <yt/yt/flow/library/cpp/parsers/unittests/proto/test_record.pb.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <string>
#include <vector>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYT::NFlow::NTesting;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf MalformedProto = "\x08";
//! Error text used by tests that make ProcessProto fail.
constexpr TStringBuf ProcessProtoErrorText = "Writing the state failed";

using TTestProto = NTest::TTestRecordProto;

//! The two hook-error flavors of the class under test.
using TRoutingBase = TProtoParsingProcessFunctionBase<TTestProto>;
using TPropagatingBase = TProtoParsingProcessFunctionBase<
    TTestProto,
    TProtoParsingProcessFunctionParameters,
    /*PropagateHookErrors*/ true>;

//! Records dispatched protos for assertions without needing an output stream. Leaves
//! ProcessUnparsed at the default (rethrow).
template <class TBase>
class TRecordingFunction
    : public TBase
{
public:
    std::vector<i64> Values;

protected:
    void ProcessProto(
        const TInputMessageConstPtr& /*message*/,
        TTestProto&& proto,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        Values.push_back(proto.value());
    }
};

//! Records the errors reaching ProcessUnparsed instead of rethrowing them.
template <class TBase>
class TSkippingFunction
    : public TRecordingFunction<TBase>
{
public:
    std::vector<TError> UnparsedErrors;

protected:
    void ProcessUnparsed(
        const TInputMessageConstPtr& /*message*/,
        TError error,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        UnparsedErrors.push_back(std::move(error));
    }
};

//! ProcessProto always fails, to test how processing failures (not parsing failures) are handled.
template <class TBase>
class TFailingFunction
    : public TSkippingFunction<TBase>
{
protected:
    void ProcessProto(
        const TInputMessageConstPtr& /*message*/,
        TTestProto&& /*proto*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        THROW_ERROR_EXCEPTION(TRuntimeFormat(ProcessProtoErrorText));
    }
};

//! Function-specific static parameters, used to test parameter extension.
struct TValueOffsetParameters
    : public TProtoParsingProcessFunctionParameters
{
    i64 ValueOffset = 0;

    REGISTER_YSON_STRUCT(TValueOffsetParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value_offset", &TThis::ValueOffset)
            .Default(0);
    }
};

//! Reads its own parameter alongside the inherited ``data_column``.
class TOffsettingFunction
    : public TRecordingFunction<TProtoParsingProcessFunctionBase<TTestProto, TValueOffsetParameters>>
{
protected:
    void ProcessProto(
        const TInputMessageConstPtr& /*message*/,
        TTestProto&& proto,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        Values.push_back(proto.value() + GetParameters()->ValueOffset);
    }
};

//! Hooks into initialization without chaining to the base.
class TInitHookFunction
    : public TRecordingFunction<TRoutingBase>
{
public:
    int InitCount = 0;
    std::string DataColumnAtInit;

protected:
    void DoInit(const IRuntimeInitContextPtr& /*initContext*/) override
    {
        ++InitCount;
        DataColumnAtInit = GetParameters()->DataColumn;
    }
};

std::string SerializeRecord(i64 value)
{
    TTestProto record;
    record.set_value(value);
    return record.SerializeAsString();
}

TTableSchemaPtr MakeSchema(TStringBuf column)
{
    return New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema(std::string(column), ESimpleLogicalValueType::String),
    });
}

TInputMessageConstPtr MakeProtoMessage(ui64 key, TStringBuf column, TStringBuf data)
{
    return MakeTestMessage("input", MakeKey(key), MakeSchema(column), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(TStringBuf(data), column);
    });
}

//! Message whose data column is present in the schema but null.
TInputMessageConstPtr MakeNullDataMessage(ui64 key, TStringBuf column)
{
    return MakeTestMessage("input", MakeKey(key), MakeSchema(column), [&] (TMessageBuilder& builder) {
        builder.Payload().SetValue(MakeUnversionedSentinelValue(EValueType::Null), column);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProtoParsingProcessFunctionBaseTest, DispatchesParsedProtoToProcessProto)
{
    TTestStateEnvironment env;
    auto function = New<TRecordingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "data", SerializeRecord(42))});

    EXPECT_EQ(function->Values, std::vector<i64>{42});
}

TEST(TProtoParsingProcessFunctionBaseTest, RoutesParseFailureToProcessUnparsed)
{
    TTestStateEnvironment env;
    auto function = New<TSkippingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "data", MalformedProto)});

    EXPECT_TRUE(function->Values.empty());
    ASSERT_EQ(std::ssize(function->UnparsedErrors), 1);
    const auto& error = function->UnparsedErrors[0];
    EXPECT_TRUE(ToString(error).Contains("Failed to parse protobuf message")) << ToString(error);
    EXPECT_EQ(error.Attributes().Get<i64>("data_size"), std::ssize(MalformedProto));
}

TEST(TProtoParsingProcessFunctionBaseTest, RoutesEmptyDataToProcessUnparsed)
{
    TTestStateEnvironment env;
    auto function = New<TSkippingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeNullDataMessage(1, "data")});

    EXPECT_TRUE(function->Values.empty());
    ASSERT_EQ(std::ssize(function->UnparsedErrors), 1);
    EXPECT_TRUE(ToString(function->UnparsedErrors[0]).Contains("empty data")) << ToString(function->UnparsedErrors[0]);
}

TEST(TProtoParsingProcessFunctionBaseTest, RoutesMissingDataColumnToProcessUnparsed)
{
    TTestStateEnvironment env;
    auto function = New<TSkippingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "other", SerializeRecord(42))});

    EXPECT_TRUE(function->Values.empty());
    ASSERT_EQ(std::ssize(function->UnparsedErrors), 1);
    EXPECT_EQ(function->UnparsedErrors[0].Attributes().Get<std::string>("data_column"), "data");
}

TEST(TProtoParsingProcessFunctionBaseTest, DefaultProcessUnparsedRethrows)
{
    TTestStateEnvironment env;
    auto function = New<TRecordingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    EXPECT_THROW(harness.RunEpoch({MakeProtoMessage(1, "data", MalformedProto)}), std::exception);
}

TEST(TProtoParsingProcessFunctionBaseTest, RoutesProcessProtoFailureToProcessUnparsed)
{
    TTestStateEnvironment env;
    auto function = New<TFailingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    EXPECT_NO_THROW(harness.RunEpoch({MakeProtoMessage(1, "data", SerializeRecord(42))}));

    ASSERT_EQ(std::ssize(function->UnparsedErrors), 1);
    EXPECT_TRUE(ToString(function->UnparsedErrors[0]).Contains(ProcessProtoErrorText))
        << ToString(function->UnparsedErrors[0]);
}

TEST(TProtoParsingProcessFunctionBaseTest, PropagatesProcessProtoFailure)
{
    TTestStateEnvironment env;
    auto function = New<TFailingFunction<TPropagatingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    EXPECT_THROW(harness.RunEpoch({MakeProtoMessage(1, "data", SerializeRecord(42))}), std::exception);

    EXPECT_TRUE(function->UnparsedErrors.empty());
}

TEST(TProtoParsingProcessFunctionBaseTest, PropagatingFlavorStillRoutesParseFailures)
{
    TTestStateEnvironment env;
    auto function = New<TSkippingFunction<TPropagatingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    EXPECT_NO_THROW(harness.RunEpoch({MakeProtoMessage(1, "data", MalformedProto)}));

    EXPECT_TRUE(function->Values.empty());
    ASSERT_EQ(std::ssize(function->UnparsedErrors), 1);
    EXPECT_TRUE(ToString(function->UnparsedErrors[0]).Contains("Failed to parse protobuf message"))
        << ToString(function->UnparsedErrors[0]);
}

TEST(TProtoParsingProcessFunctionBaseTest, UsesConfiguredDataColumn)
{
    TTestStateEnvironment env;
    auto parameters = New<TProtoParsingProcessFunctionParameters>();
    parameters->DataColumn = "body";
    env.SetStaticParameters(parameters);

    auto function = New<TRecordingFunction<TRoutingBase>>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "body", SerializeRecord(7))});

    EXPECT_EQ(function->Values, std::vector<i64>{7});
}

TEST(TProtoParsingProcessFunctionBaseTest, UsesFunctionSpecificParameters)
{
    TTestStateEnvironment env;
    auto parameters = New<TValueOffsetParameters>();
    parameters->DataColumn = "body";
    parameters->ValueOffset = 100;
    env.SetStaticParameters(parameters);

    auto function = New<TOffsettingFunction>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "body", SerializeRecord(7))});

    EXPECT_EQ(function->Values, std::vector<i64>{107});
}

TEST(TProtoParsingProcessFunctionBaseTest, CallsDoInitOnceAfterFetchingParameters)
{
    TTestStateEnvironment env;
    auto parameters = New<TProtoParsingProcessFunctionParameters>();
    parameters->DataColumn = "body";
    env.SetStaticParameters(parameters);

    auto function = New<TInitHookFunction>();
    TProcessFunctionTestHarness harness(env, function);

    harness.RunEpoch({MakeProtoMessage(1, "body", SerializeRecord(42))});
    harness.RunEpoch({MakeProtoMessage(2, "body", SerializeRecord(43))});

    EXPECT_EQ(function->InitCount, 1);
    EXPECT_EQ(function->DataColumnAtInit, "body");
    EXPECT_EQ(function->Values, (std::vector<i64>{42, 43}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
