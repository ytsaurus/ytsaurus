#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/datetime/base.h>
#include <util/generic/array_ref.h>

#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Estimates a producer's rate (bytes per event-time second) from the accepted
//! history plus the currently announced backlog.
//!
//! The history is an exponentially decayed byte counter clocked by the
//! alignment timestamps of accepted batches, so the estimate is independent of
//! both the processing clock and the issued limit. The announced backlog gives
//! an instant (but noisy) signal; once enough history is observed it is capped
//! by a multiple of the historical rate, so a backlog announced within a single
//! timestamp cannot claim an arbitrarily large rate.
class TOfferedRateEstimator
    : public TRefCounted
{
public:
    static constexpr TDuration DefaultWindow = TDuration::Seconds(60);
    //! How far above the historical rate the announced backlog may push the
    //! estimate at once; growth beyond it is confirmed by acceptance, one
    //! factor per window.
    static constexpr double MaxHistoryGain = 4.0;

    explicit TOfferedRateEstimator(TDuration window = DefaultWindow);

    //! Takes effect smoothly: the accumulated history is kept and decays at the
    //! new rate from now on.
    void SetWindow(TDuration window);

    //! |timestampSeconds| is the batch's alignment timestamp. Out-of-order
    //! timestamps are legal (their contribution is decayed by age); the clock
    //! advances at most one window per call, so a lone timestamp from the far
    //! future dents the history by at most a factor of e.
    void RecordAccepted(i64 inflatedBytes, i64 timestampSeconds);

    //! |pendingBuckets| are (alignment timestamp seconds, inflated bytes) of
    //! the currently announced backlog. Returns 0 for an empty backlog.
    double EstimateRate(TConstArrayRef<std::pair<i64, i64>> pendingBuckets) const;

private:
    double WindowSeconds_;
    //! Accepted bytes exponentially decayed to |Frontier_|.
    double DecayedBytes_ = 0;
    i64 Frontier_ = -1;
    i64 FirstAcceptedTimestamp_ = -1;
};

DEFINE_REFCOUNTED_TYPE(TOfferedRateEstimator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
