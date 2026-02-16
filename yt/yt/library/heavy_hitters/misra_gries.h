#pragma once

#include "config.h"
#include "public.h"

#include <util/datetime/base.h>

#include <set>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
class TMisraGriesHeavyHitters
    : public TRefCounted
{
public:
    struct TStatistics
    {
        double Total = 0;
        THashMap<TKey, double> Fractions;
    };

    TMisraGriesHeavyHitters(double threshold, TDuration window, i64 defaultLimit);
    explicit TMisraGriesHeavyHitters(const TMisraGriesHeavyHittersConfigPtr& config);

    void Register(const std::vector<TKey>& keys, TInstant now);
    void RegisterWeighted(const std::vector<std::pair<TKey, double>>& weightedKeys, TInstant now);
    void Reconfigure(const TMisraGriesHeavyHittersConfigPtr& newConfig);
    TStatistics GetStatistics(TInstant now, std::optional<i64> limit = {}) const;

private:
    struct TKeyState;

    using TSummaryMap = THashMap<TKey, TKeyState>;
    using TSummaryMapIterator = TSummaryMap::iterator;

    struct TCounterSetComparator
    {
        bool operator()(const std::pair<double, TSummaryMapIterator>& lhs, const std::pair<double, TSummaryMapIterator>& rhs) const;
    };

    using TCounterSet = std::set<std::pair<double, TSummaryMapIterator>, TCounterSetComparator>;

    struct TKeyState
    {
        // These counters are being changed lazily, actual counters can be evaluated as:
        // * ActualMisraGriesCounter = decay(MisraGriesCounter - MisraGriesDelta_, now - SummaryTimestamp_).
        // * ActualStatisticsCounter = decay(StatisticsCounter, now - SummaryTimestamp_).
        double MisraGriesCounter = 0;
        double StatisticsCounter = 0;

        TCounterSet::iterator SortedByMisraGriesCounterIterator = {};
        TCounterSet::iterator SortedByStatisticsCounterIterator = {};
    };

    double Threshold_;
    TDuration Window_;
    i64 DefaultLimit_;
    i64 SummarySizeLimit_;

    static constexpr int MaxWindowCount = 1000;
    static constexpr int WindowCountToUpdateTimestamp = 300;
    static constexpr i64 MaxSummarySize = 10000000;

    TSummaryMap Summary_;

    TCounterSet SortedByMisraGriesCounter_;
    TCounterSet SortedByStatisticsCounter_;
    std::vector<typename TCounterSet::node_type> CounterSetNodePool_;

    TInstant SummaryTimestamp_ = TInstant::Zero();
    double MisraGriesDelta_ = 0;
    double TotalCounter_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void DoRegister(const TKey& key, double increment);
    void CleanUpSummary();
    static void UpdateState(
        TCounterSet* set,
        TCounterSet::iterator* iterator,
        double newValue);
    double GetNormalizationFactor(TInstant now) const;
    void EnsureSummaryTimestampFreshness(TInstant now, bool force = false);
    void Clear();
    void UpdateWindow(TDuration newWindow, TInstant now);
    void UpdateThreshold(double newThreshold);
    void UpdateDefaultLimit(i64 newDefaultLimit);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MISRA_GRIES_INL_H_
#include "misra_gries-inl.h"
#undef MISRA_GRIES_INL_H_
