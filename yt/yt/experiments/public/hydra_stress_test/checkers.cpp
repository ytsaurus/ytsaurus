#include "checkers.h"

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = HydraStressTestLogger;

////////////////////////////////////////////////////////////////////////////////

void TConsistencyChecker::Check(TValue value)
{
    auto guard = Guard(Lock_);
    if (value < Value_) {
        YT_TLOG_FATAL("Consistency check failed")
            .With("PrevValue", Value_)
            .With("Value", value);
    }
    Value_ = value;
}

////////////////////////////////////////////////////////////////////////////////

TLinearizabilityChecker::TLinearizabilityChecker(int peerCount)
    : PeerCount_(peerCount)
{ }

void TLinearizabilityChecker::SubmitMutation(ui64 randomSeed, i64 index)
{
    auto guard = Guard(Lock_);

    YT_TLOG_DEBUG("Submitting mutation to linearizability checker")
        .WithFormat("RandomSeed", "%x", randomSeed)
        .With("Index", index);

    if (auto it = Mutations_.find(randomSeed)) {
        auto& [oldIndex, count] = it->second;
        ++count;

        if (oldIndex != index) {
            YT_TLOG_FATAL("Mutation indices differ")
                .WithFormat("RandomSeed", "%x", randomSeed)
                .With("OldIndex", oldIndex)
                .With("Index", index);
        }
    } else {
        Mutations_.emplace(randomSeed, std::pair(index, 1));
    }

    if (Mutations_.at(randomSeed).second == PeerCount_) {
        YT_TLOG_DEBUG("Erasing mutation from linearizability checker")
            .WithFormat("RandomSeed", "%x", randomSeed);
        YT_VERIFY(Mutations_.erase(randomSeed) == 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

TLivenessChecker::TLivenessChecker(TConfigPtr config)
    : Config_(config)
    , LastSuccess_(TInstant::Now())
{ }

void TLivenessChecker::IncrementErrorCount(int delta)
{
    auto guard = Guard(Lock_);
    ErrorCount_ += delta;
    YT_TLOG_INFO("Change error count")
        .With("ErrorCount", ErrorCount_);
    LastStateChangeTime_ = TInstant::Now();
}

void TLivenessChecker::Report(bool isOk)
{
    auto guard = Guard(Lock_);

    YT_TLOG_DEBUG("Availability result reported")
        .With("IsOK", isOk)
        .With("ErrorCount", ErrorCount_)
        .With("LastSuccess", LastSuccess_)
        .With("LastStateChange", LastStateChangeTime_);

    auto now = TInstant::Now();
    if (isOk) {
        LastSuccess_ = now;
    }

    if (!isOk
        && ErrorCount_ == 0
        && now - LastSuccess_ > Config_->UnavailabilityTimeout
        && now - LastStateChangeTime_ > Config_->ResurrectionTimeout)
    {
        YT_TLOG_FATAL("Liveness check failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
