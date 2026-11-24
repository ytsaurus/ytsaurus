#pragma once

#include "public.h"

#include <library/cpp/yt/cpu_clock/public.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <util/generic/hash.h>

#include <compare>
#include <queue>
#include <utility>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

//! Compares the I/O consumptions of two jobs. Lower consumption per unit of weight
//! is ordered as |less|. A job with zero weight is ordered after any job with
//! a positive weight; jobs with zero weight are compared by raw consumption.
std::partial_ordering CompareJobIOConsumptions(
    i64 lhsConsumption,
    double lhsWeight,
    i64 rhsConsumption,
    double rhsWeight);

////////////////////////////////////////////////////////////////////////////////

struct TJobIOConsumption
{
    //! max(G, L) + F, capped at the maximum i64 value; see TIOJobState.
    i64 Consumption = 0;
    double Weight = 0;
};

//! The I/O fair-share state of a job on one location.
/*!
 *  The consumption of a job is estimated as max(G, L) + F, capped at the maximum i64 value, where
 *  - G is the newest consumption reported by the job, linearly decayed over the window;
 *  - L is the local I/O completed within the window;
 *  - F is the cost of the local I/O dispatched but not completed yet.
 *
 *  Instants are monotonic (see GetCpuInstant); completion times are sampled under the lock.
 *
 *  Thread affinity: any.
 */
class TIOJobState
    : public TRefCounted
{
public:
    explicit TIOJobState(TDuration window);

    //! Replaces the reported consumption unless the stored report is newer.
    void UpdateReport(i64 ioConsumed, double weight, TCpuInstant snapshotAt);

    TJobIOConsumption ComputeConsumption(TCpuInstant now);

    //! Keeps the reservations and the completion timestamps of the retained local history.
    //! Drops history outside the new window; previously expired history is not restored.
    void SetWindow(TDuration window, TCpuInstant now);
    TCpuDuration GetWindow();

    void Reserve(i64 cost);
    void Complete(i64 cost);

    //! Counts queued requests referring to the state; the state is not expired while referred to.
    void AcquireRequest();
    void ReleaseRequest();

    //! Returns true if no queued requests or local I/O remain
    //! and the last report is absent or its window has expired.
    bool IsExpired(TCpuInstant now);

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);

    TCpuDuration Window_;

    bool HasReport_ = false;
    i64 ReportedIOConsumed_ = 0;
    double Weight_ = 0;
    TCpuInstant SnapshotAt_ = 0;

    std::queue<std::pair<TCpuInstant, i64>> CompletedHistory_;
    i64 CompletedInWindow_ = 0;

    i64 Reserved_ = 0;
    int RequestCount_ = 0;

    i64 ComputeDecayedReport(TCpuInstant now) const;
    void TrimHistory(TCpuInstant now);
};

DEFINE_REFCOUNTED_TYPE(TIOJobState)

////////////////////////////////////////////////////////////////////////////////

//! Compares the jobs of the head requests of two slots. A request without a job state,
//! i.e. without statistics, is ordered before any request with one.
//! Requests of one job are equivalent. Otherwise the jobs are compared by |consumptions|,
//! which must contain both of them.
std::partial_ordering CompareHeadRequestJobs(
    TIOJobState* lhsJobState,
    TIOJobState* rhsJobState,
    const THashMap<TIOJobState*, TJobIOConsumption>& consumptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
