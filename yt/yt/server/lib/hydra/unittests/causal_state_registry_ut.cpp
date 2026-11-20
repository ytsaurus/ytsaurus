#include <yt/yt/server/lib/hydra/causal_state_registry.h>
#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NHydra {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCausalStateRegistryTest
    : public ::testing::Test
{ };

TCausalStateRegistryPtr CreateRegistry(
    TCausalStateRegistryConfigPtr config = New<TCausalStateRegistryConfig>())
{
    return New<TCausalStateRegistry>(
        std::move(config),
        MakeStrong(GetCurrentInvoker()),
        NProfiling::TProfiler());
}

void ExpectState(const TCausalState& state, ui64 causalTime, i64 sequenceNumber, int segmentId)
{
    EXPECT_EQ(state.CausalTime, TCausalTime(causalTime));
    EXPECT_EQ(state.SequenceNumber, sequenceNumber);
    EXPECT_EQ(state.SegmentId, segmentId);
}

TEST_W(TCausalStateRegistryTest, EmptyHistory)
{
    auto registry = CreateRegistry();

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(std::nullopt),
        EErrorCode::CausalTimeNotFound);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(TCausalTime(0)),
        EErrorCode::CausalTimeNotFound);
}

TEST_W(TCausalStateRegistryTest, ExactCutoffAndGap)
{
    auto registry = CreateRegistry();
    registry->RegisterState({.CausalTime = TCausalTime(10), .SequenceNumber = 4, .SegmentId = 2});
    registry->RegisterState({.CausalTime = TCausalTime(30), .SequenceNumber = 5, .SegmentId = 3});

    ExpectState(registry->GetStateOrThrow(TCausalTime(10)), 10, 4, 2);
    ExpectState(registry->GetStateOrThrow(TCausalTime(29)), 10, 4, 2);
    ExpectState(registry->GetStateOrThrow(TCausalTime(30)), 30, 5, 3);
    ExpectState(registry->GetStateOrThrow(std::nullopt), 30, 5, 3);

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(TCausalTime(9)),
        EErrorCode::CausalTimeNotFound);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(TCausalTime(31)),
        EErrorCode::CausalTimeNotReached);
}

TEST_W(TCausalStateRegistryTest, RecoveryReplacesRetainedHistory)
{
    auto registry = CreateRegistry();
    registry->RegisterState({.CausalTime = TCausalTime(100), .SequenceNumber = 40, .SegmentId = 20});
    registry->Clear();

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(std::nullopt),
        EErrorCode::CausalTimeNotFound);

    registry->RegisterState({.CausalTime = TCausalTime(30), .SequenceNumber = 5, .SegmentId = 3});
    ExpectState(registry->GetStateOrThrow(std::nullopt), 30, 5, 3);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(TCausalTime(29)),
        EErrorCode::CausalTimeNotFound);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetStateOrThrow(TCausalTime(100)),
        EErrorCode::CausalTimeNotReached);
}

TEST_W(TCausalStateRegistryTest, EvictionRetainsLatestState)
{
    auto config = New<TCausalStateRegistryConfig>();
    config->ExpirationTimeout = TDuration::Zero();
    config->EvictionPeriod = TDuration::MilliSeconds(1);
    auto registry = CreateRegistry(std::move(config));

    registry->RegisterState({.CausalTime = TCausalTime(1)});
    registry->RegisterState({.CausalTime = TCausalTime(2)});

    WaitForPredicate([&] {
        try {
            registry->GetStateOrThrow(TCausalTime(1));
        } catch (const TErrorException& ex) {
            EXPECT_TRUE(ex.Error().FindMatching(EErrorCode::CausalTimeNotFound).has_value());
            return true;
        }
        return false;
    }, "Old causal state was not evicted");

    EXPECT_EQ(registry->GetStateOrThrow(std::nullopt).CausalTime, TCausalTime(2));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
