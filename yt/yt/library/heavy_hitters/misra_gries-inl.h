#ifndef MISRA_GRIES_INL_H_
#error "Direct inclusion of this file is not allowed, include misra_gries.h"
// For the sake of sane code completion.
#include "misra_gries.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
bool TMisraGriesHeavyHitters<TKey>::TCounterSetComparator::operator()(
    const std::pair<double, TSummaryMapIterator>& lhs,
    const std::pair<double, TSummaryMapIterator>& rhs) const
{
    if (lhs.first != rhs.first) {
        return lhs.first < rhs.first;
    }
    return std::addressof(*lhs.second) < std::addressof(*rhs.second);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
TMisraGriesHeavyHitters<TKey>::TMisraGriesHeavyHitters(double threshold, TDuration window, i64 defaultLimit)
    : Threshold_(threshold)
    , Window_(window)
    , DefaultLimit_(defaultLimit)
    , SummarySizeLimit_(std::min(MaxSummarySize, static_cast<i64>(1.0 / threshold)))
{ }

template <class TKey>
TMisraGriesHeavyHitters<TKey>::TMisraGriesHeavyHitters(const TMisraGriesHeavyHittersConfigPtr& config)
    : Threshold_(config->Threshold)
    , Window_(config->Window)
    , DefaultLimit_(config->DefaultLimit)
    , SummarySizeLimit_(std::min(MaxSummarySize, static_cast<i64>(1.0 / Threshold_)))
{ }

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::DoRegister(const TKey& key, double increment)
{
    TotalCounter_ += increment;
    // The difference between misra-gries and statistics counters is here.
    auto [iter, emplaced] = Summary_.try_emplace(
        key,
        TKeyState{
            .MisraGriesCounter = increment + MisraGriesDelta_,
            .StatisticsCounter = increment,
        });

    if (!emplaced) {
        iter->second.MisraGriesCounter += increment;
        iter->second.StatisticsCounter += increment;

        UpdateState(&SortedByMisraGriesCounter_, &iter->second.SortedByMisraGriesCounterIterator, iter->second.MisraGriesCounter);
        UpdateState(&SortedByStatisticsCounter_, &iter->second.SortedByStatisticsCounterIterator, iter->second.StatisticsCounter);
    } else {
        auto insertCounter = [&] (auto* container, auto* targetIterator, double counter) {
            if (!CounterSetNodePool_.empty()) {
                CounterSetNodePool_.back().value().first = counter;
                CounterSetNodePool_.back().value().second = iter;
                auto insertionResult = container->insert(std::move(CounterSetNodePool_.back()));
                CounterSetNodePool_.pop_back();
                YT_VERIFY(insertionResult.inserted);
                *targetIterator = insertionResult.position;
            } else {
                *targetIterator = EmplaceOrCrash(*container, counter, iter);
            }
        };
        insertCounter(&SortedByMisraGriesCounter_, &iter->second.SortedByMisraGriesCounterIterator, iter->second.MisraGriesCounter);
        insertCounter(&SortedByStatisticsCounter_, &iter->second.SortedByStatisticsCounterIterator, iter->second.StatisticsCounter);

        CleanUpSummary();
    }
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::Register(const std::vector<TKey>& keys, TInstant now)
{
    auto guard = Guard(SpinLock_);

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
    auto guard = Guard(SpinLock_);

    EnsureSummaryTimestampFreshness(now);
    double increment = 1.0 / GetNormalizationFactor(now);

    for (const auto& [key, weight] : weightedKeys) {
        DoRegister(key, increment * weight);
    }
}

template <class TKey>
TMisraGriesHeavyHitters<TKey>::TStatistics TMisraGriesHeavyHitters<TKey>::GetStatistics(TInstant now, std::optional<i64> limit) const
{
    auto guard = Guard(SpinLock_);

    if (SortedByMisraGriesCounter_.empty()) {
        return {};
    }

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
            const auto& summaryIt = rit->second;
            statistics.Fractions[summaryIt->first] = ratio;
        }
        ++rit;
        --count;
    }

    return statistics;
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::CleanUpSummary()
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

    while (std::ssize(Summary_) >= SummarySizeLimit_ && !Summary_.empty()) {
        auto minIter = SortedByMisraGriesCounter_.begin();
        double minValue = minIter->first;

        MisraGriesDelta_ = minValue;
        auto it = SortedByMisraGriesCounter_.begin();
        while (it != SortedByMisraGriesCounter_.end() && it->first <= MisraGriesDelta_) {
            auto next = std::next(it);
            auto summaryIter = it->second;

            auto eraseCounter = [&] (auto* container, const auto& iterator) {
                auto element = container->extract(iterator);
                YT_ASSERT(!element.empty());
                CounterSetNodePool_.emplace_back(std::move(element));
            };

            eraseCounter(&SortedByStatisticsCounter_, summaryIter->second.SortedByStatisticsCounterIterator);
            eraseCounter(&SortedByMisraGriesCounter_, it);
            Summary_.erase(summaryIter);
            it = next;
        }
    }
}

template <class TKey>
double TMisraGriesHeavyHitters<TKey>::GetNormalizationFactor(TInstant now) const
{
    return std::exp2(-((now - SummaryTimestamp_) / Window_));
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::UpdateState(
    TCounterSet* set,
    TCounterSet::iterator* iterator,
    double newValue)
{
    auto setElement = set->extract(*iterator);
    YT_VERIFY(!setElement.empty());
    setElement.value().first = newValue;
    auto insertionResult = set->insert(std::move(setElement));
    YT_VERIFY(insertionResult.inserted);
    *iterator = insertionResult.position;
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::EnsureSummaryTimestampFreshness(TInstant now, bool force)
{
    if ((now - SummaryTimestamp_) / Window_ > MaxWindowCount) {
        SortedByMisraGriesCounter_.clear();
        SortedByStatisticsCounter_.clear();
        Summary_.clear();
        MisraGriesDelta_ = 0;
        TotalCounter_ = 0;
        SummaryTimestamp_ = now;
    }

    if ((now - SummaryTimestamp_) / Window_ >= WindowCountToUpdateTimestamp || force) {
        double normalizationFactor = GetNormalizationFactor(now);
        for (auto iter = Summary_.begin(); iter != Summary_.end(); ++iter) {
            iter->second.MisraGriesCounter *= normalizationFactor;
            iter->second.StatisticsCounter *= normalizationFactor;

            UpdateState(&SortedByMisraGriesCounter_, &iter->second.SortedByMisraGriesCounterIterator, iter->second.MisraGriesCounter);
            UpdateState(&SortedByStatisticsCounter_, &iter->second.SortedByStatisticsCounterIterator, iter->second.StatisticsCounter);
        }
        MisraGriesDelta_ *= normalizationFactor;
        TotalCounter_ *= normalizationFactor;
        SummaryTimestamp_ = now;
    }
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::Clear()
{
    SortedByMisraGriesCounter_.clear();
    SortedByStatisticsCounter_.clear();
    CounterSetNodePool_.clear();
    Summary_.clear();
    MisraGriesDelta_ = 0;
    TotalCounter_ = 0;
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::UpdateWindow(TDuration newWindow, TInstant now)
{
    Window_ = newWindow;
    EnsureSummaryTimestampFreshness(now, /*force*/ true);
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::UpdateThreshold(double newThreshold)
{
    SummarySizeLimit_ = std::min(MaxSummarySize, static_cast<i64>(1.0 / newThreshold));
    CleanUpSummary();
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::UpdateDefaultLimit(i64 newDefaultLimit)
{
    DefaultLimit_ = newDefaultLimit;
}

template <class TKey>
void TMisraGriesHeavyHitters<TKey>::Reconfigure(const TMisraGriesHeavyHittersConfigPtr& newConfig)
{
    auto guard = Guard(SpinLock_);

    if (!newConfig->Enable) {
        Clear();
    }

    if (Window_ != newConfig->Window) {
        UpdateWindow(newConfig->Window, TInstant::Now());
    }

    if (Threshold_ != newConfig->Threshold) {
        UpdateThreshold(newConfig->Threshold);
    }

    if (DefaultLimit_ != newConfig->DefaultLimit) {
        UpdateDefaultLimit(newConfig->DefaultLimit);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
