#include <yt/yt/flow/examples/cpp/external_state_join/lib/external_state_join_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/in_memory_external_state_manager.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
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
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// The output message must be registered so the runtime context can validate/convert it.
YT_FLOW_DEFINE_YSON_MESSAGE(TEnrichedRow);

////////////////////////////////////////////////////////////////////////////////

// Schema of the read-only reference state: a single "name" column keyed by "key".
TTableSchemaPtr ReferenceStateSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=name;type=string}]")));
}

TInputMessageConstPtr MakeEvent(const TKey& key, ui64 eventKey)
{
    return MakeTestMessage("event", key, GetYsonMessagePayloadSchema<TEventRow>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(eventKey, "key");
    });
}

// Seeds the reference state for |key| with a "name" value the joiner will look up.
void SeedReference(const TInMemorySimpleExternalStateJoinerPtr& joiner, const TKey& key, const std::string& name)
{
    auto holder = joiner->GetMutableState(key);
    auto& state = holder->Get();
    TPayloadBuilder builder(state.Schema);
    builder.Set(name, "name");
    state.Payload = builder.Finish();
}

IRuntimeContextPtr MakeContext()
{
    return TTestRuntimeContextBuilder()
        .RegisterStream<TEnrichedRow>("enriched")
        .Build();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExternalStateJoinExampleTest, EnrichesEventFromReference)
{
    TTestStateEnvironment stateEnv;
    auto joiner = stateEnv.RegisterExternalStateJoiner("/reference", ReferenceStateSchema());

    auto key = MakeKey<ui64>(1);
    SeedReference(joiner, key, "alice");

    TProcessFunctionTestHarness harness(stateEnv, New<TLookupJoinFunction>(), MakeContext());
    harness.RunEpoch({MakeEvent(key, /*eventKey*/ 1)});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& message = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(message, "key"), 1u);
    EXPECT_EQ(GetColumnValue<std::string>(message, "name"), "alice");
}

TEST(TExternalStateJoinExampleTest, EmptyStateProducesNoOutput)
{
    TTestStateEnvironment stateEnv;
    stateEnv.RegisterExternalStateJoiner("/reference", ReferenceStateSchema());

    // Key 2 is never seeded; the joiner returns an empty state, so nothing is emitted.
    auto key = MakeKey<ui64>(2);
    TProcessFunctionTestHarness harness(stateEnv, New<TLookupJoinFunction>(), MakeContext());
    harness.RunEpoch({MakeEvent(key, /*eventKey*/ 2)});

    EXPECT_TRUE(harness.GetMessages().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
