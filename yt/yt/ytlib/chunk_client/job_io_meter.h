#pragma once

#include "public.h"

#include <library/cpp/yt/system/atomic_object.h>
#include <library/cpp/yt/system/spin_lock.h>

#include <chrono>
#include <deque>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Tracks I/O operations (reads and writes) within a sliding time window.
class TJobIoMeter
    : public TRefCounted
{
public:
    TJobIoMeter(TDuration window, bool enabled);

    //! Returns whether job I/O statistics and fair-share weight reporting is enabled.
    bool IsEnabled() const;

    //! Accounts disk bytes read or written by the job.
    void Account(i64 bytes);

    //! Includes all one-second buckets that overlap the job's sliding window.
    i64 GetIoConsumedInWindow();

    //! Sets the fair-share weight associated with the tracked job.
    void SetIoFairShareWeight(double weight);

    //! Returns the fair-share weight associated with the tracked job, if set.
    std::optional<double> GetIoFairShareWeight() const;

private:
    using TClock = std::chrono::steady_clock;

    const TDuration BucketLifetime_;
    const bool Enabled_;

    struct TBucket
    {
        std::chrono::time_point<TClock, std::chrono::seconds> Start;
        i64 Bytes = 0;
    };

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::deque<TBucket> History_;
    i64 BytesInWindow_ = 0;

    TAtomicObject<std::optional<double>> IoFairShareWeight_;

    void ExpireBuckets(TClock::time_point now);
};

DEFINE_REFCOUNTED_TYPE(TJobIoMeter)

////////////////////////////////////////////////////////////////////////////////

//! Returns no weight when reporting is disabled by the job I/O meter.
//! Otherwise returns the explicitly configured I/O fair-share weight when it is set;
//! if not, returns the weight associated with the job I/O meter, if any.
std::optional<double> GetEffectiveIoFairShareWeight(
    std::optional<double> configuredWeight,
    const TJobIoMeterPtr& jobIoMeter);

//! Reports the job's recently consumed I/O to the data node via the
//! io_consumed request field. No-op when no enabled meter is attached.
template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoConsumed(
    const TRequestPtr& req,
    const TClientChunkOptions& options);

//! Reports the effective I/O fair-share weight to the data node via the
//! io_fair_share_weight request field. No-op when the weight is not set.
template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoFairShareWeight(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    std::optional<double> configuredWeight);

//! Forwards the upstream I/O fair-share snapshot when present; otherwise uses
//! the local job I/O meter and the configured weight.
template <class TRequestPtr>
void SetRequestReadIoFairShareState(
    const TRequestPtr& req,
    const TClientChunkReadOptions& options,
    std::optional<double> configuredWeight);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define JOB_IO_METER_INL_H_
#include "job_io_meter-inl.h"
#undef JOB_IO_METER_INL_H_
