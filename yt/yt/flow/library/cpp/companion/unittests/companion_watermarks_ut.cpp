#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/swift_map_companion_computation.h>
#include <yt/yt/flow/library/cpp/companion/swift_ordered_source_companion_computation.h>
#include <yt/yt/flow/library/cpp/companion/transform_companion_computation.h>
#include <yt/yt/flow/library/cpp/companion/transform_ordered_source_companion_computation.h>

#include <yt/yt/flow/library/cpp/companion/client/companion_model.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TStreamId InputStreamId("input");
const TStreamId TimerStreamId("timer");
const TStreamId SourceStreamId("source");

//! A spec declaring an input, a timer and a source stream, so a shim picking the wrong
//! streams is caught.
TComputationSpecPtr MakeSpec()
{
    auto spec = New<TComputationSpec>();
    spec->InputStreamIds = {InputStreamId};
    spec->TimerStreams[TimerStreamId] = New<TTimerSpec>();
    spec->SourceStreams[SourceStreamId] = New<TSourceSpec>();
    return spec;
}

//! Distinct event and system watermarks for every stream of #MakeSpec().
TWatermarkStatePtr MakeWatermarkState()
{
    auto state = New<TWatermarkState>();
    ui64 base = 100;
    for (const auto& streamId : {InputStreamId, TimerStreamId, SourceStreamId}) {
        auto watermarks = New<TWatermarks>();
        watermarks->EventWatermark = TSystemTimestamp(base);
        watermarks->SystemWatermark = TSystemTimestamp(base + 1);
        state->Streams[streamId] = std::move(watermarks);
        base += 100;
    }
    return state;
}

using TAddWatermarks = void (*)(
    const TCompanionProcessRequestPtr& request,
    const TComputationSpec& spec,
    const TWatermarkStatePtr& watermarkState);

//! Runs |addWatermarks| and checks the request carries exactly |expectedStreamIds|, each
//! with its event and system watermarks from #MakeWatermarkState().
void ExpectWatermarks(TAddWatermarks addWatermarks, const THashSet<TStreamId>& expectedStreamIds)
{
    auto state = MakeWatermarkState();
    auto request = New<TCompanionProcessRequest>();
    addWatermarks(request, *MakeSpec(), state);

    THashSet<TStreamId> streamIds;
    for (const auto& watermark : request->Watermarks) {
        EXPECT_TRUE(streamIds.insert(watermark.StreamId).second) << ToString(watermark.StreamId);
        EXPECT_EQ(watermark.EventWatermark, state->GetEventWatermark(watermark.StreamId));
        EXPECT_EQ(watermark.SystemWatermark, state->GetSystemWatermark(watermark.StreamId));
    }
    EXPECT_EQ(streamIds, expectedStreamIds);
}

TEST(TCompanionWatermarksTest, SwiftMapSendsInputStreams)
{
    ExpectWatermarks(&TSwiftMapCompanionComputation::AddWatermarks, {InputStreamId});
}

TEST(TCompanionWatermarksTest, SwiftOrderedSourceSendsSourceStreams)
{
    ExpectWatermarks(&TSwiftOrderedSourceCompanionComputation::AddWatermarks, {SourceStreamId});
}

TEST(TCompanionWatermarksTest, TransformSendsInputAndTimerStreams)
{
    ExpectWatermarks(&TTransformCompanionComputation::AddWatermarks, {InputStreamId, TimerStreamId});
}

TEST(TCompanionWatermarksTest, TransformOrderedSourceSendsSourceStreams)
{
    ExpectWatermarks(&TTransformOrderedSourceCompanionComputation::AddWatermarks, {SourceStreamId});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
