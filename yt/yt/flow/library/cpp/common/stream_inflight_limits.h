#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

#include <atomic>
#include <limits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStreamUsage
{
    i64 CumulativeByteIn = 0;
    i64 CumulativeByteOut = 0;
    i64 CumulativeCountIn = 0;
    i64 CumulativeCountOut = 0;
    //! Announced-but-not-admitted backlog, inflated bytes. Not used for
    //! back-pressure (that compares in-flight against the limit), but the v2
    //! buffer manager reads it to bootstrap the drain cap and grow headroom.
    i64 PendingInflatedBytes = 0;

    //! In-flight bytes inflated by the per-message technical cost — the quantity
    //! back-pressure compares against the limit.
    i64 GetInflatedInflightBytes(i64 inflationPerMessage) const
    {
        return (CumulativeByteIn - CumulativeByteOut) + (CumulativeCountIn - CumulativeCountOut) * inflationPerMessage;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Per-stream usage + limit slot: several independent atomic channels shared
//! between the owning buffer/store and the buffer manager. Channel ownership:
//!   - usage snapshot + peak watermark: written by the owning buffer/store
//!     (#Update), read by the manager tick; the peak is a destructive read
//!     with a SINGLE-consumer contract — only the manage tick may call
//!     #ReadAndResetMaxInflatedInflightBytes.
//!   - limit: written by the manager, read by the owner's admission checks.
//!   - offered rate: written by the input buffer (or the source computation),
//!     read by the manager tick.
//!   - estimated speed: written by the manager, read by the warmup poller.
class alignas(64) TStreamLimitUsageState
    : public TRefCounted
{
public:
    explicit TStreamLimitUsageState(i64 inflationPerMessage = 0);

    //! Cumulative counters must be monotonically non-decreasing across calls
    //! — sum-as-seq relies on that.
    void Update(const TStreamUsage& usage);
    TStreamUsage Read() const;

    //! Maximum inflated in-flight bytes seen by #Update since the previous call;
    //! captures usage peaks between periodic manager reads.
    i64 ReadAndResetMaxInflatedInflightBytes();

    //! Estimated stream speed, inflated bytes per second; published by the buffer
    //! manager, consumed for warm-start persistence.
    void SetEstimatedInflatedSpeed(double inflatedBytesPerSecond);
    double GetEstimatedInflatedSpeed() const;

    //! Producer rate offered to this stream, in INFLATED bytes per second: an
    //! instant, limit-independent demand signal. Published either by the input
    //! buffer from the announced backlog (Σ inflated bytes over the backlog's
    //! time span) or, for a source computation's outputs, from the source's own
    //! raw estimate — #SetOfferedRawRate converts it using this stream's
    //! per-message inflation. Zero when the backlog is too fresh or absent to
    //! estimate. Recomputed only when offers arrive, so it can go stale after
    //! the producers vanish; harmless since an empty backlog drops the issued
    //! limit to zero anyway.
    void SetOfferedInflatedBytesPerSecond(double rate);
    void SetOfferedRawRate(double bytesPerSecond, double messagesPerSecond);
    double GetOfferedInflatedBytesPerSecond() const;

    void SetLimitBytes(i64 limitBytes);
    i64 GetLimitBytes() const;

    i64 GetInflationPerMessage() const;
    bool IsUsageWithinLimits(const TStreamUsage& usage) const;

private:
    std::atomic<ui64> CumulativeByteIn_{0};
    std::atomic<ui64> CumulativeByteOut_{0};
    std::atomic<ui64> CumulativeCountIn_{0};
    std::atomic<ui64> CumulativeCountOut_{0};
    std::atomic<ui64> PendingInflatedBytes_{0};
    std::atomic<ui64> Seq_{0};
    std::atomic<i64> LimitBytes_{std::numeric_limits<i64>::max()};
    std::atomic<i64> MaxInflatedInflightBytes_{0};
    std::atomic<double> EstimatedInflatedSpeed_{0};
    std::atomic<double> OfferedInflatedBytesPerSecond_{0};
    const i64 InflationPerMessage_;
};

DEFINE_REFCOUNTED_TYPE(TStreamLimitUsageState);

////////////////////////////////////////////////////////////////////////////////

using TStreamLimitUsageStateMap = THashMap<TStreamId, TStreamLimitUsageStatePtr>;

////////////////////////////////////////////////////////////////////////////////

//! Returns the subset of |states| whose current cumulative usage is still
//! within the back-pressure limit.
THashSet<TStreamId> GetStreamsWithinLimits(const TStreamLimitUsageStateMap& states);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
