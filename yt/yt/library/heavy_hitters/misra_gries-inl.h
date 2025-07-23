#ifndef MISRA_GRIES_INL_H_
#error "Direct inclusion of this file is not allowed, include misra_gries.h"
// For the sake of sane code completion.
#include "misra_gries.h"
#endif

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
TMisraGriesHeavyHitters<TKey>::TMisraGriesHeavyHitters(double threshold, TDuration window, i64 defaultLimit)
    : Threshold_(threshold)
    , Window_(window)
    , DefaultLimit_(defaultLimit)
    , SummarySizeLimit_(std::min(MaxSummarySize, static_cast<i64>(1.0 / threshold)))
{ }

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::DoRegister(const TKey& key, double increment)
{
    TotalCounter_ += increment;
    // The difference between misra-gries and statistics counters is here.
    auto [iter, emplaced] = Summary_.emplace(
        key, TCounters{
                    .MisraGriesCounter = increment + MisraGriesDelta_,
                    .StatisticsCounter = increment,
            });
    if (!emplaced) {
        auto oldValue = iter->second;

        iter->second.MisraGriesCounter += increment;
        iter->second.StatisticsCounter += increment;

        UpdateState(SortedByMisraGriesCounter_, iter->first, oldValue.MisraGriesCounter, iter->second.MisraGriesCounter);
        UpdateState(SortedByStatisticsCounter_, iter->first, oldValue.StatisticsCounter, iter->second.StatisticsCounter);
    } else {
        SortedByMisraGriesCounter_.insert(std::pair(iter->second.MisraGriesCounter, iter->first));
        SortedByStatisticsCounter_.insert(std::pair(iter->second.StatisticsCounter, iter->first));
        CleanUpSummary();
    }
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::Register(const std::vector<TKey>& keys, TInstant now)
{
    EnsureSummaryTimestampFreshness(now);
    double increment = 1.0 / GetNormalizationFactor(now);

    for (const auto& key : keys) {
        DoRegister(key, increment);
    }
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::RegisterWeighted(
    const std::vector<std::pair<TKey, double>>& weightedKeys,
    TInstant now)
{
    EnsureSummaryTimestampFreshness(now);
    double increment = 1.0 / GetNormalizationFactor(now);

    for (const auto& [key, weight] : weightedKeys) {
        DoRegister(key, increment * weight);
    }
}

template <class TKey>
TMisraGriesHeavyHitters<TKey>::TStatistics TMisraGriesHeavyHitters<TKey>::GetStatistics(TInstant now, std::optional<i64> limit) const
{
    double normalizationFactor = GetNormalizationFactor(now);
    auto total = TotalCounter_ * normalizationFactor;
    TStatistics statistics;
    statistics.Total = total / Window_.Seconds();

    auto rit = SortedByStatisticsCounter_.rbegin();
    i64 count = DefaultLimit_;
    if (limit.has_value()) {
        count = *limit;
    }
    while (count != 0 && rit != SortedByStatisticsCounter_.rend()) {
        auto hits = rit->first * normalizationFactor;
        auto ratio = hits / total;
        if (ratio >= Threshold_) {
            statistics.Fractions[rit->second] = ratio;
        }
        ++rit;
        --count;
    }

    return statistics;
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::CleanUpSummary()
{
    if (std::ssize(Summary_) < SummarySizeLimit_ || Summary_.empty()) {
        return;
    }

    auto minIter = SortedByMisraGriesCounter_.begin();
    double minValue = minIter->first;

    MisraGriesDelta_ = minValue;
    auto it = SortedByMisraGriesCounter_.begin();
    while (it != SortedByMisraGriesCounter_.end() && it->first <= MisraGriesDelta_) {
        auto next = std::next(it);
        auto summaryIter = Summary_.find(it->second);
        SortedByStatisticsCounter_.erase(std::pair(summaryIter->second.StatisticsCounter, it->second));
        Summary_.erase(summaryIter);
        SortedByMisraGriesCounter_.erase(it);
        it = next;
    }
}

template <class TKey>
double TMisraGriesHeavyHitters<TKey>::GetNormalizationFactor(TInstant now) const
{
    return std::exp2(-((now - SummaryTimestamp_) / Window_));
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::UpdateState(std::set<std::pair<double, TKey>>& set, const TKey& key, double oldValue, double newValue)
{
    auto setElement = set.extract(std::pair(oldValue, key));
    YT_VERIFY(!setElement.empty());
    setElement.value().first = newValue;
    set.insert(std::move(setElement));
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::EnsureSummaryTimestampFreshness(TInstant now)
{
    if ((now - SummaryTimestamp_) / Window_ > MaxWindowCount) {
        SortedByMisraGriesCounter_.clear();
        SortedByStatisticsCounter_.clear();
        Summary_.clear();
        MisraGriesDelta_ = 0;
        TotalCounter_ = 0;
        SummaryTimestamp_ = now;
    }

    if ((now - SummaryTimestamp_) / Window_ >= WindowCountToUpdateTimestamp) {
        double normalizationFactor = GetNormalizationFactor(now);
        for (auto iter = Summary_.begin(); iter != Summary_.end(); ++iter) {
            auto oldValue = iter->second;

            iter->second.MisraGriesCounter *= normalizationFactor;
            iter->second.StatisticsCounter *= normalizationFactor;

            UpdateState(SortedByMisraGriesCounter_, iter->first, oldValue.MisraGriesCounter, iter->second.MisraGriesCounter);
            UpdateState(SortedByStatisticsCounter_, iter->first, oldValue.StatisticsCounter, iter->second.StatisticsCounter);
        }
        MisraGriesDelta_ *= normalizationFactor;
        TotalCounter_ *= normalizationFactor;
        SummaryTimestamp_ = now;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
