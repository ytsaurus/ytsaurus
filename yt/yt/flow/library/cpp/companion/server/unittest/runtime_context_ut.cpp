#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/runtime_context.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCompanionRuntimeContextPtr MakeRuntimeContext()
{
    auto schema = NTesting::DefaultTestKeySchema();

    auto spec = New<TComputationSpec>();
    spec->InputStreamIds = {TStreamId("in1"), TStreamId("in2")};
    spec->OutputStreamIds = {TStreamId("output")};

    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecMap;
    int specId = 1;
    for (const auto& streamId : {TStreamId("in1"), TStreamId("in2"), TStreamId("output")}) {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = New<NTableClient::TTableSchema>(schema->Columns());
        streamSpecMap[streamId][TStreamSpecId(specId++)] = std::move(streamSpec);
    }
    auto streamSpecs = New<TStreamSpecs>(streamSpecMap);

    auto converterCache = CreatePayloadConverterCache(/*evaluatorCache*/ nullptr);
    auto storage = New<TComputationStreamSpecStorage>(streamSpecs, schema, converterCache);

    return New<TCompanionRuntimeContext>(
        spec,
        storage,
        schema,
        converterCache,
        /*throttlerFactory*/ nullptr);
}

TEST(TCompanionRuntimeContextTest, WatermarksAndDynamicParameters)
{
    auto context = MakeRuntimeContext();

    auto watermarkState = BuildWatermarkState({
        {TStreamId("in1"), TSystemTimestamp(100)},
        {TStreamId("in2"), TSystemTimestamp(50)},
    });
    auto dynamicParameters = ConvertTo<IMapNodePtr>(NYson::TYsonString(TStringBuf("{limit=7}")));
    context->RefreshEpochState(watermarkState, dynamicParameters);

    EXPECT_EQ(context->GetWatermark(TStreamId("in1")), TSystemTimestamp(100));
    EXPECT_EQ(context->GetWatermark(TStreamId("in2")), TSystemTimestamp(50));
    EXPECT_EQ(context->GetInputEventWatermark(), TSystemTimestamp(50));
    EXPECT_EQ(
        context->GetDynamicParametersNode()->GetChildOrThrow("limit")->AsInt64()->GetValue(),
        7);
}

TEST(TCompanionRuntimeContextTest, OutputMessageBuilder)
{
    auto context = MakeRuntimeContext();
    context->RefreshEpochState(BuildWatermarkState({}), nullptr);

    auto builder = context->MakeOutputMessageBuilder(std::nullopt);
    builder.SetMessageId(TMessageId("m1"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.Payload().Set(ui64{5}, "key");
    auto message = builder.Finish();
    EXPECT_EQ(message.StreamId, TStreamId("output"));
    EXPECT_EQ(GetColumnValue<ui64>(message, 0), ui64{5});
}

TEST(TCompanionRuntimeContextTest, ThrottlerThrows)
{
    auto context = MakeRuntimeContext();
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(context->GetThrottlerOrThrow(TThrottlerId("my_throttler"))),
        "not available in a companion process");
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(context->TryGetThrottler(TThrottlerId("my_throttler"))),
        "not available in a companion process");
}

TEST(TCompanionRuntimeContextTest, CurrentTimestampThrows)
{
    auto context = MakeRuntimeContext();
    context->RefreshEpochState(BuildWatermarkState({}), nullptr);
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(context->GetCurrentTimestamp()),
        "not available in a companion process");
}

TEST(TCompanionRuntimeContextTest, EpochUniqueSeqNoThrows)
{
    auto context = MakeRuntimeContext();
    context->RefreshEpochState(BuildWatermarkState({}), nullptr);
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(context->GetEpochUniqueSeqNo()),
        "not available in a companion process");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
