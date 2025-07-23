#pragma once

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

    void Register(const std::vector<TKey>& keys, TInstant now);
    void RegisterWeighted(const std::vector<std::pair<TKey, double>>& weightedKeys, TInstant now);
    TStatistics GetStatistics(TInstant now, std::optional<i64> limit = {}) const;

private:
    struct TCounters
    {
        // These counters are being changed lazily, actual counters can be evaluated as:
        // * ActualMisraGriesCounter = decay(MisraGriesCounter - MisraGriesDelta_, now - SummaryTimestamp_).
        // * ActualStatisticsCounter = decay(StatisticsCounter, now - SummaryTimestamp_).
        double MisraGriesCounter = 0;
        double StatisticsCounter = 0;
    };

    const double Threshold_;
    const TDuration Window_;
    i64 DefaultLimit_;
    i64 SummarySizeLimit_;

    static constexpr int MaxWindowCount = 1000;
    static constexpr int WindowCountToUpdateTimestamp = 300;
    static constexpr i64 MaxSummarySize = 10000000;

    THashMap<TKey, TCounters> Summary_;
    std::set<std::pair<double, TKey>> SortedByMisraGriesCounter_;
    std::set<std::pair<double, TKey>> SortedByStatisticsCounter_;

    TInstant SummaryTimestamp_ = TInstant::Zero();
    double MisraGriesDelta_ = 0;
    double TotalCounter_ = 0;

private:
    void DoRegister(const TKey& key, double increment);
    void CleanUpSummary();
    static void UpdateState(std::set<std::pair<double, TKey>>& set, const TKey& key, double oldValue, double newValue);
    double GetNormalizationFactor(TInstant now) const;
    void EnsureSummaryTimestampFreshness(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MISRA_GRIES_INL_H_
#include "misra_gries-inl.h"
#undef MISRA_GRIES_INL_H_
