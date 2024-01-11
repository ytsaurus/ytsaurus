#pragma once

#include "public.h"
#include "helpers.h"
#include "config.h"

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TConsistencyChecker
    : public TRefCounted
{
public:
    void Check(TValue value);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TValue Value_ = Min<TValue>();
};

DEFINE_REFCOUNTED_TYPE(TConsistencyChecker)

//////////////////////////////////////////////////////////////////////////////////

class TLinearizabilityChecker
    : public TRefCounted
{
public:
    explicit TLinearizabilityChecker(int peerCount);

    void SubmitMutation(ui64 randomSeed, i64 index);

private:
    const int PeerCount_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<ui64, std::pair<i64, int>> Mutations_;
};

DEFINE_REFCOUNTED_TYPE(TLinearizabilityChecker)

//////////////////////////////////////////////////////////////////////////////////

class TLivenessChecker
    : public TRefCounted
{
public:
    explicit TLivenessChecker(TConfigPtr config);

    void IncrementErrorCount(int delta);

    void Report(bool isOk);

private:
    const TConfigPtr Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    int ErrorCount_ = 0;
    TInstant LastStateChangeTime_ = TInstant::Now();
    TInstant LastSuccess_ = TInstant::Now();
};

DEFINE_REFCOUNTED_TYPE(TLivenessChecker)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
