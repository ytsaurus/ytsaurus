#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

class TLineageTracker
    : public TRefCounted
{
public:
    void Commit(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta);
    void Commit(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta,
        TInstant now);

    TLineageRates GetRates(TInstant now);

private:
    struct TCounterState
    {
        TSimpleEmaCounter CountCounter{LineageRateDecayTime};
        TSimpleEmaCounter ByteCounter{LineageRateDecayTime};
        TSimpleEmaCounter InputCountCounter{LineageRateDecayTime};
        TSimpleEmaCounter InputByteCounter{LineageRateDecayTime};
        TInstant LastUpdateTime;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TStreamId, THashMap<TStreamId, TCounterState>> Counters_;
    TInstant LastCommitTime_;

    void DoCommit(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta,
        TInstant now);
    TLineageRates DoGetRates(TInstant now);
};

DEFINE_REFCOUNTED_TYPE(TLineageTracker);

////////////////////////////////////////////////////////////////////////////////

IJobLineageTrackerPtr CreateJobLineageTracker(
    TLineageTrackerPtr lineageTracker,
    TComputationId computationId,
    TComputationSpecPtr computationSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
