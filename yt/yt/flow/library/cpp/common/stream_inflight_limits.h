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
    //! Diagnostic-only; not used for back-pressure. Inflated bytes.
    i64 PendingInflatedBytes = 0;

    //! In-flight bytes inflated by the per-message technical cost — the quantity
    //! back-pressure compares against the limit.
    i64 GetInflatedInflightBytes(i64 inflationPerMessage) const
    {
        return (CumulativeByteIn - CumulativeByteOut) + (CumulativeCountIn - CumulativeCountOut) * inflationPerMessage;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Per-stream usage + limit slot with a single writer of the cumulative
//! counters (the owning buffer/store) and many readers (manager + observers).
class alignas(64) TStreamLimitUsageState
    : public TRefCounted
{
public:
    explicit TStreamLimitUsageState(i64 inflationPerMessage = 0);

    //! Cumulative counters must be monotonically non-decreasing across calls
    //! — sum-as-seq relies on that.
    void Update(const TStreamUsage& usage);
    TStreamUsage Read() const;

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
