#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/decayed_sum.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

class TLineageTracker
    : public TRefCounted
{
public:
    void Add(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta);
    void Add(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta,
        TInstant now);

    TLineageRatios GetRatios(TInstant now);

private:
    struct TCounterState
    {
        TDecayedSum CountCounter{LineageDecayTime};
        TDecayedSum ByteCounter{LineageDecayTime};
        TDecayedSum InputCountCounter{LineageDecayTime};
        TDecayedSum InputByteCounter{LineageDecayTime};
        TInstant LastUpdateTime;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TStreamId, THashMap<TStreamId, TCounterState>> Counters_;
    TInstant LastObservationTime_;

    void DoAdd(
        const TComputationId& computationId,
        const TComputationSpecPtr& computationSpec,
        const TLineageDelta& delta,
        TInstant now);
    TLineageRatios DoGetRatios(TInstant now);
};

DEFINE_REFCOUNTED_TYPE(TLineageTracker);

////////////////////////////////////////////////////////////////////////////////

IJobLineageTrackerPtr CreateJobLineageTracker(
    TLineageTrackerPtr lineageTracker,
    TComputationId computationId,
    TComputationSpecPtr computationSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
