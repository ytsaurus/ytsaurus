#include "exponential_bins.h"

#include "profile_manager.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

auto BinUpperLimits = [] {
    std::array<ui64, TExponentialBins::BinCount> result;
    for (int index = 0; index <= 6; ++index) {
        result[index] = 1ull << index;
    }
    for (int index = 7; index < 10; ++index) {
        result[index] = 1000ull >> (10 - index);
    }
    for (int index = 10; index < TExponentialBins::BinCount; ++index) {
        result[index] = 1000 * result[index - 10];
    }
    return result;
}();

TExponentialBins::TExponentialBins(
    const TLegacyProfiler& profiler,
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    std::function<TTagId(int)> binIndexToTagId,
    TDuration interval)
    : Profiler_(profiler)
    , CounterPath_(path)
    , CounterTagIds_(tagIds)
    , CounterInterval_(interval)
{
    for (int binIndex = 0; binIndex < BinCount; ++binIndex) {
        auto tagIds = CounterTagIds_;
        tagIds.emplace_back(binIndexToTagId(binIndex));
        BinCounters_[binIndex] = TShardedMonotonicCounter(CounterPath_, std::move(tagIds), CounterInterval_);
    }
}

void TExponentialBins::Account(ui64 value)
{
    auto binIndex = GetBinIndex(value);
    BinPresent_[binIndex].store(true);
    Profiler_.Increment(BinCounters_[binIndex]);
}

void TExponentialBins::FlushBins()
{
    for (int binIndex = 0; binIndex < BinCount; ++binIndex) {
        if (BinPresent_[binIndex]) {
            // No-op increment which still causes profiler to export the value.
            Profiler_.Increment(BinCounters_[binIndex], 0);
        }
    }
}

int TExponentialBins::GetBinIndex(ui64 value)
{
    return std::lower_bound(BinUpperLimits.begin(), BinUpperLimits.end() - 1, value) - BinUpperLimits.begin();
}

TTagId TExponentialBins::BinIndexToHumanReadableDuration(int binIndex)
{
    TString value;
    if (binIndex < 10) {
        value = Format("%vus", BinUpperLimits[binIndex]);
    } else if (binIndex < 20) {
        value = Format("%vms", BinUpperLimits[binIndex - 10]);
    } else {
        value = Format("%vs", BinUpperLimits[binIndex - 20]);
    }

    // Unfortunately there is currently no easy way to make Solomon properly sort bucket
    // labels containing us, ms and s, so we still need to include bin %02d-represented index
    // to make bins follow in alphabetical order.
    value = Format("%02d_%v", binIndex, value);

    return TProfileManager::Get()->RegisterTag("bin_time", value);
}

TTagId TExponentialBins::BinIndexToString(int binIndex)
{
    return TProfileManager::Get()->RegisterTag("bin_index", ToString(binIndex));
}

TTagId TExponentialBins::BinIndexToUpperLimit(int binIndex)
{
    auto binLimit = BinUpperLimits[binIndex];
    return TProfileManager::Get()->RegisterTag("bin_limit", ToString(binLimit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
