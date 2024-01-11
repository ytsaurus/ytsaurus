#include "checkers.h"

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraStressTestLogger;

//////////////////////////////////////////////////////////////////////////////////

void TConsistencyChecker::Check(TValue value)
{
    auto guard = Guard(Lock_);
    if (value < Value_) {
        YT_LOG_FATAL("Consistency check failed (PrevValue: %v, Value: %v)",
            Value_,
            value);
    }
    Value_ = value;
}

//////////////////////////////////////////////////////////////////////////////////

TLinearizabilityChecker::TLinearizabilityChecker(int peerCount)
    : PeerCount_(peerCount)
{ }

void TLinearizabilityChecker::SubmitMutation(ui64 randomSeed, i64 index)
{
    auto guard = Guard(Lock_);

    YT_LOG_DEBUG("Submitting mutation to linearizability checker (RandomSeed: %x, Index: %v)",
        randomSeed,
        index);

    if (auto it = Mutations_.find(randomSeed)) {
        auto& [oldIndex, count] = it->second;
        ++count;

        if (oldIndex != index) {
            YT_LOG_FATAL("Mutation indices differ (RandomSeed: %x, OldIndex: %v, Index: %v)",
                randomSeed,
                oldIndex,
                index);
        }
    } else {
        Mutations_.emplace(randomSeed, std::pair(index, 1));
    }

    if (Mutations_.at(randomSeed).second == PeerCount_) {
        YT_LOG_DEBUG("Erasing mutation from linearizability checker (RandomSeed: %x)", randomSeed);
        YT_VERIFY(Mutations_.erase(randomSeed) == 1);
    }
}

//////////////////////////////////////////////////////////////////////////////////

TLivenessChecker::TLivenessChecker(TConfigPtr config)
    : Config_(config)
    , LastSuccess_(TInstant::Now())
{ }

void TLivenessChecker::IncrementErrorCount(int delta)
{
    auto guard = Guard(Lock_);
    ErrorCount_ += delta;
    YT_LOG_INFO("Change error count (ErrorCount: %v)",
        ErrorCount_);
    LastStateChangeTime_ = TInstant::Now();
}

void TLivenessChecker::Report(bool isOk)
{
    auto guard = Guard(Lock_);

    YT_LOG_DEBUG("Availability result reported (IsOK: %v, ErrorCount: %v, LastSuccess: %v, LastStateChange: %v)",
        isOk,
        ErrorCount_,
        LastSuccess_,
        LastStateChangeTime_);

    auto now = TInstant::Now();
    if (isOk) {
        LastSuccess_ = now;
    }

    if (!isOk
        && ErrorCount_ == 0
        && now - LastSuccess_ > Config_->UnavailabilityTimeout
        && now - LastStateChangeTime_ > Config_->ResurrectionTimeout)
    {
        YT_LOG_FATAL("Liveness check failed");
    }
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
