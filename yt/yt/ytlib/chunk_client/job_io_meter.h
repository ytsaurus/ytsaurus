#pragma once

#include "public.h"

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
    explicit TJobIoMeter(TDuration maxHistoryDuration);

    //! Accounts a read operation of #size bytes.
    void AccountRead(i64 size);

    //! Accounts a write operation of #size bytes.
    void AccountWrite(i64 size);

    //! Returns the total number of I/O bytes accounted within the last #window.
    i64 GetIoConsumedInWindow(TDuration window) const;

private:
    // I/O is aggregated into one-minute buckets, so history size grows with the
    // tracked time span rather than with the number of accounted operations.
    // History older than #MaxHistoryDuration_ is dropped.
    const TDuration MaxHistoryDuration_;

    struct TIoBucket
    {
        TInstant Minute;
        i64 Size = 0;
    };

    // Buckets are appended in accounting order, thus ordered by time.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    std::deque<TIoBucket> History_;

    void Account(i64 size);
};

DEFINE_REFCOUNTED_TYPE(TJobIoMeter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
