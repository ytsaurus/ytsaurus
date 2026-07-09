#include <yt/yt/flow/tests/state_joiner/pipeline/lib/state_joiner_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NStateJoinerTest;

////////////////////////////////////////////////////////////////////////////////

// Output-stream message shapes. The functions build raw payloads by column name; these only give
// the runtime context a schema for the "users" and "results" output streams.
struct TUserMessage
    : public TYsonMessage
{
    std::string UserId;
    ui64 Bucket = 0;

    REGISTER_YSON_STRUCT(TUserMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("UserId", &TThis::UserId)
            .Default();
        registrar.Parameter("Bucket", &TThis::Bucket)
            .Default(0);
    }
};

struct TResultMessage
    : public TYsonMessage
{
    std::string UserId;
    i64 Total = 0;

    REGISTER_YSON_STRUCT(TResultMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("UserId", &TThis::UserId)
            .Default();
        registrar.Parameter("Total", &TThis::Total)
            .Default(0);
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TUserMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TResultMessage);

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr EventSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=UserId;type=string};{name=Amount;type=int64}])")));
}

TInputMessageConstPtr MakeEvent(const TKey& key, const std::string& userId, i64 amount)
{
    return MakeTestMessage("events", key, EventSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set<TString>(TString(userId), "UserId");
        builder.Payload().Set<i64>(amount, "Amount");
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStateJoinerFunctionTest, AccumulatorForwardsUserId)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TUserMessage>("users")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TAccumulatorFunction>(), context);

    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({
        MakeEvent(key, "user-1", 10),
        MakeEvent(key, "user-1", 30),
    });

    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(GetColumnValue<TString>(harness.GetMessages()[0].Message, "UserId"), "user-1");
    EXPECT_EQ(GetColumnValue<ui64>(harness.GetMessages()[0].Message, "Bucket"), 0u);
}

TEST(TStateJoinerFunctionTest, JoinerEmitsAccumulatedTotal)
{
    TTestStateEnvironment stateEnv;
    auto stateJoinerSpec = stateEnv.RegisterStateJoiner("user_total", "/total");

    auto key = MakeKey<ui64>(1);

    // Run the accumulator to populate the internal "/total" state for the user. Its RunEpoch
    // commits the state while the accumulator is alive, which the state manager requires (it holds
    // the mutable provider weakly).
    {
        auto context = TTestRuntimeContextBuilder()
            .RegisterStream<TUserMessage>("users")
            .Build();
        TProcessFunctionTestHarness accumulator(stateEnv, New<TAccumulatorFunction>(), context);
        accumulator.RunEpoch({
            MakeEvent(key, "user-1", 10),
            MakeEvent(key, "user-1", 30),
        });
    }

    // Run the joiner; it reads the accumulated total through the "/user_total" state joiner.
    auto joinerSpec = New<TComputationSpec>();
    joinerSpec->StateJoiners["/user_total"] = stateJoinerSpec;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TResultMessage>("results")
        .SetSpec(joinerSpec)
        .Build();
    TProcessFunctionTestHarness joiner(stateEnv, New<TJoinerFunction>(), context);

    auto userSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=UserId;type=string};{name=Bucket;type=uint64}])")));
    auto userMessage = MakeTestMessage("users", key, userSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set<TString>("user-1", "UserId");
        builder.Payload().Set<ui64>(0, "Bucket");
    });
    joiner.RunEpoch({userMessage});

    ASSERT_EQ(std::ssize(joiner.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<TString>(joiner.GetMessages()[0].Message, "UserId"), "user-1");
    EXPECT_EQ(GetColumnValue<i64>(joiner.GetMessages()[0].Message, "Total"), 40);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
