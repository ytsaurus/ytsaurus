#include <yt/yt/flow/library/cpp/common/buffer_warmup.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TPartitionBufferWarmup MakeWarmup(double inputSpeed, double outputSpeed, double epochCycleSeconds)
{
    TPartitionBufferWarmup warmup;
    warmup.InputSpeeds[TStreamId("input")] = inputSpeed;
    warmup.OutputSpeeds[TStreamId("output")] = outputSpeed;
    warmup.EpochCycleSeconds = epochCycleSeconds;
    return warmup;
}

TEST(TBufferWarmupTest, SmallDriftIsNotWorthPersisting)
{
    auto oldWarmup = MakeWarmup(/*inputSpeed*/ 1e6, /*outputSpeed*/ 2e6, /*epochCycleSeconds*/ 10);
    EXPECT_FALSE(WarmupDiffers(oldWarmup, oldWarmup));
    // Ten percent on every component is below the rewrite threshold.
    EXPECT_FALSE(WarmupDiffers(oldWarmup, MakeWarmup(1.1e6, 2.2e6, 11)));
}

TEST(TBufferWarmupTest, LargeDriftOfAnyComponentIsPersisted)
{
    auto oldWarmup = MakeWarmup(/*inputSpeed*/ 1e6, /*outputSpeed*/ 2e6, /*epochCycleSeconds*/ 10);
    EXPECT_TRUE(WarmupDiffers(oldWarmup, MakeWarmup(2e6, 2e6, 10)));
    EXPECT_TRUE(WarmupDiffers(oldWarmup, MakeWarmup(1e6, 4e6, 10)));
    EXPECT_TRUE(WarmupDiffers(oldWarmup, MakeWarmup(1e6, 2e6, 30)));
}

TEST(TBufferWarmupTest, AppearingAndVanishingStreamsCount)
{
    auto oldWarmup = MakeWarmup(/*inputSpeed*/ 1e6, /*outputSpeed*/ 2e6, /*epochCycleSeconds*/ 10);

    auto extended = oldWarmup;
    extended.InputSpeeds[TStreamId("another")] = 5e6;
    EXPECT_TRUE(WarmupDiffers(oldWarmup, extended));

    auto shrunk = oldWarmup;
    shrunk.InputSpeeds.erase(TStreamId("input"));
    EXPECT_TRUE(WarmupDiffers(oldWarmup, shrunk));
}

TEST(TBufferWarmupTest, SurvivesYsonRoundTrip)
{
    auto warmup = MakeWarmup(/*inputSpeed*/ 1e6, /*outputSpeed*/ 2e6, /*epochCycleSeconds*/ 10);
    auto restored = NYTree::ConvertTo<TPartitionBufferWarmup>(NYTree::ConvertToNode(warmup));

    EXPECT_FALSE(WarmupDiffers(warmup, restored));
    EXPECT_EQ(restored.InputSpeeds.at(TStreamId("input")), 1e6);
    EXPECT_EQ(restored.OutputSpeeds.at(TStreamId("output")), 2e6);
    EXPECT_EQ(restored.EpochCycleSeconds, 10);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
