#pragma once

#include "public.h"

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <deque>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Tracks I/O operations (reads and writes) within a sliding time window.
class TJobIoMeter
    : public TRefCounted
{
public:
    //! History older than #maxHistoryDuration is dropped.
    TJobIoMeter(TDuration maxHistoryDuration, bool enabled);

    //! Returns whether job I/O statistics and fair-share weight reporting is enabled.
    bool IsEnabled() const;

    //! Accounts a read operation of #size bytes.
    void AccountRead(i64 size);

    //! Accounts a write operation of #size bytes.
    void AccountWrite(i64 size);

    //! Returns the total number of I/O bytes accounted within the last #window.
    i64 GetIoConsumedInWindow(TDuration window) const;

    //! Sets the fair-share weight associated with the tracked job.
    void SetIoFairShareWeight(double weight);

    //! Returns the fair-share weight associated with the tracked job, if set.
    std::optional<double> GetIoFairShareWeight() const;

private:
    // I/O is aggregated into one-minute buckets, so history size grows with the
    // tracked time span rather than with the number of accounted operations.
    // History older than #MaxHistoryDuration_ is dropped.
    const TDuration MaxHistoryDuration_;

    const bool Enabled_;

    struct TIoBucket
    {
        TInstant Minute;
        i64 Size = 0;
    };

    // Buckets are appended in accounting order, thus ordered by time.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    std::deque<TIoBucket> History_;

    NThreading::TAtomicObject<std::optional<double>> IoFairShareWeight_;

    void Account(i64 size);
};

DEFINE_REFCOUNTED_TYPE(TJobIoMeter)

////////////////////////////////////////////////////////////////////////////////

//! Returns no weight when reporting is disabled by the job I/O meter.
//! Otherwise returns the explicitly configured I/O fair-share weight when it is set;
//! if not, returns the weight associated with the job I/O meter, if any.
std::optional<double> GetEffectiveIoFairShareWeight(
    std::optional<double> configuredWeight,
    const TJobIoMeterPtr& jobIoMeter);

//! Reports the job's recently consumed I/O over #window to the data node via the
//! io_consumed request field. No-op when no enabled meter is attached.
template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoConsumed(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    TDuration window);

//! Reports the effective I/O fair-share weight to the data node via the
//! io_fair_share_weight request field. No-op when the weight is not set.
template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoFairShareWeight(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    std::optional<double> configuredWeight);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define JOB_IO_METER_INL_H_
#include "job_io_meter-inl.h"
#undef JOB_IO_METER_INL_H_
