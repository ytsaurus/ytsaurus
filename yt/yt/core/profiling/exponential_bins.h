#pragma once

#include "profiler.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Helper class for visualizing distribution of some integer value over time.
//!
//! Each input value x is being accounted in one of the bins, incrementing the counter
//! for that bin. Each bin is represented by a monotonic counter with an assigned arbitrary tag
//! (i.e. bin_index or bin_limit depending on setup). In other words, this helper class
//! allows you to estimate rate of values falling into each of the bins. For instance, this may
//! be useful for visualizing response time distribution for some API.
//!
//! Bins are distributed _almost_ exponentially with step of 2; the only difference is that 64
//! is followed by 125, 64'000 is followed by 125'000 and so on for the sake of better human-readability
//! of upper limit.
//!
//! The first several bin marks are:
//! 1, 2, 4, 8, 16, 32, 64, 125, 250, 500, 1000, 2000, 4000, 8000, 16'000, 32'000, 64'000, 125'000, ...
//!
//! In terms of time this can be read as:
//! 1us, 2us, 4us, 8us, ..., 500us, 1ms, 2ms, ..., 500ms, 1s, ...
//!
//! By default, bin index is mapped into profiling tag as if the value is a duration expressed in ms.
//! In this case profiling tags look like nice human-readable values similar to the previous paragraph.
class TExponentialBins
{
public:
    //! Argument specifies which tag should be exported for each particular bin.
    TExponentialBins(
        const NProfiling::TProfiler& profiler,
        const NYPath::TYPath& path = {},
        const TTagIdList& tagIds = {},
        std::function<TTagId(int)> binIndexToTagId = TExponentialBins::BinIndexToHumanReadableDuration,
        TDuration interval = TCounterBase::DefaultInterval);

    //! Assuming that input values for bins are microseconds, return a human-readable description for bin upper limit
    //! e.g. "bin_time=8us", "bin_time=500ms" or "bin_time=2s".
    static TTagId BinIndexToHumanReadableDuration(int binIndex);

    //! Simply return bin index as a string, e.g. "bin_index=23".
    static TTagId BinIndexToString(int binIndex);

    //! Return upper bound for bin index, e.g. "bin_limit=4000".
    static TTagId BinIndexToUpperLimit(int binIndex);

    //! Account single value.
    /*!
     * \note: Thread affinity: any.
     */
    void Account(ui64 value);

    //! Iterate over all present bins and forcefully flush their values.
    //! This method is intended for periodical invocation in case when events
    //! happen rarely enough to prevent plot gaps.
    /*!
     * \note: Thread affinity: any.
     */
    void FlushBins();

public:
    // Last bin limit is 16e18.
    // NB: if value > 16e18 (which is slightly lower than 2^64), we still consider
    // it to be belonging to the last bin. This does not look like a big problem.
    static constexpr int BinCount = 65;

    static int GetBinIndex(ui64 value);

private:
    const NProfiling::TProfiler& Profiler_;
    NYPath::TYPath CounterPath_;
    TTagIdList CounterTagIds_;
    TDuration CounterInterval_;

    std::array<TShardedMonotonicCounter, TExponentialBins::BinCount> BinCounters_;
    //! We do not pollute metric list with bins that were never accessed.
    std::array<std::atomic_bool, TExponentialBins::BinCount> BinPresent_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
