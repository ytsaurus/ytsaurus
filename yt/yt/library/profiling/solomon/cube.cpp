#include "cube.h"
#include "histogram_snapshot.h"

#include <yt/yt/library/profiling/summary.h>
#include <yt/yt/library/profiling/tag.h>

#include <yt/core/misc/assert.h>
#include <yt/core/misc/error.h>

#include <type_traits>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TCube<T>::TProjection::IsZero(int index) const
{
    T zero{};
    return Values[index] == zero;
}

template <class T>
bool TCube<T>::TProjection::IsLingering(i64 iteration) const
{
    return LastNonZeroIteration >= iteration;
}

template <class T>
TCube<T>::TCube(int windowSize, i64 nextIteration)
    : WindowSize_(windowSize)
    , NextIteration_(nextIteration)
    , BaseIteration_(nextIteration - (nextIteration % windowSize))
{ }

template <class T>
void TCube<T>::StartIteration()
{
    Index_ = GetIndex(NextIteration_);
    NextIteration_++;
    BaseIteration_ = NextIteration_ - (NextIteration_ % WindowSize_);

    for (auto& [tagIds, projection] : Projections_) {
        projection.Rollup += projection.Values[Index_];
        projection.Values[Index_] = {};
        projection.HasValue[Index_] = false;
    }
}

template <class T>
void TCube<T>::FinishIteration()
{ }

template <class T>
void TCube<T>::Add(const TTagIdList& tagIds)
{
    if (auto it = Projections_.find(tagIds); it != Projections_.end()) {
        it->second.UsageCount++;
    } else {
        TProjection projection;
        projection.UsageCount = 1;
        projection.Values.resize(WindowSize_);
        projection.HasValue.resize(WindowSize_);
        Projections_[tagIds] = std::move(projection);
    }
}

template <class T>
void TCube<T>::AddAll(const TTagIdList& tagIds, const TProjectionSet& projections)
{
    projections.Range(tagIds, [this] (auto tagIds) mutable {
        Add(tagIds);
    });
}

template <class T>
void TCube<T>::Remove(const TTagIdList& tagIds)
{
    auto it = Projections_.find(tagIds);
    if (it == Projections_.end()) {
        THROW_ERROR_EXCEPTION("Broken cube");
    }

    it->second.UsageCount--;
    if (it->second.UsageCount == 0) {
        Projections_.erase(it);
    }
}

template <class T>
void TCube<T>::RemoveAll(const TTagIdList& tagIds, const TProjectionSet& projections)
{
    projections.Range(tagIds, [this] (auto tagIds) mutable {
        Remove(tagIds);
    });
}

template <class T>
void TCube<T>::Update(const TTagIdList& tagIds, T value)
{
    auto it = Projections_.find(tagIds);
    if (it == Projections_.end()) {
        THROW_ERROR_EXCEPTION("Broken cube");
    }

    if constexpr (std::is_same_v<T, double>) {
        if (std::isnan(value)) {
            return;
        }
    }

    it->second.Values[Index_] += value;
    it->second.HasValue[Index_] = true;
    if (!it->second.IsZero(Index_)) {
        it->second.LastNonZeroIteration = NextIteration_ - 1;
    }
}

template <class T>
const THashMap<TTagIdList, typename TCube<T>::TProjection>& TCube<T>::GetProjections() const
{
    return Projections_;
}

template <class T>
int TCube<T>::GetSize() const
{
    return Projections_.size();
}

template <class T>
int TCube<T>::GetIndex(i64 iteration) const
{
    return iteration % WindowSize_;
}

template <class T>
i64 TCube<T>::GetIteration(int index) const
{
    auto iteration =  BaseIteration_ + index;
    if (iteration >= NextIteration_) {
        iteration -= WindowSize_;
    }
    return iteration;
}

template <class T>
T TCube<T>::Rollup(const TProjection& window, int index) const
{
    auto sum = window.Rollup;

    for (auto i = Index_ + 1; true; i++) {
        if (i == WindowSize_) {
            i = 0;
        }

        sum += window.Values[i];
        if (i == index) {
            break;
        }
    }

    return sum;
}

template <class T>
int TCube<T>::ReadSensors(
    const TString& name,
    const TReadOptions& options,
    const TTagRegistry& tagsRegistry,
    NMonitoring::IMetricConsumer* consumer) const
{
    int sensorsEmitted = 0;

    auto writeLabels = [&] (const auto& tagIds, std::optional<TStringBuf> suffix, bool allowAggregate) {
        consumer->OnLabelsBegin();

        TString sensorName;
        sensorName.reserve(name.size() + (suffix ? suffix->size() : 0));
        if (options.DisableSensorsRename) {
            sensorName += name;
        } else {
            if (name[0] != '/') {
                sensorName.push_back(name[0]);
            }
            for (size_t i = 1; i < name.size(); ++i) {
                if (name[i] == '/') {
                    sensorName.push_back('.');
                } else {
                    sensorName.push_back(name[i]);
                }
            }
            if (sensorName.back() == '.') {
                sensorName.pop_back();
            }
        }

        if (suffix) {
            sensorName += *suffix;
        }

        consumer->OnLabel("sensor", sensorName);

        if (options.Global) {
            consumer->OnLabel("host", "");
        } else if (options.Host) {
            consumer->OnLabel("host", *options.Host);
        }

        SmallVector<bool, 8> replacedInstanceTags(options.InstanceTags.size());

        if (allowAggregate && options.MarkAggregates && !options.Global) {
            consumer->OnLabel("yt_aggr", "1");
        }

        for (auto tagId : tagIds) {
            const auto& tag = tagsRegistry.Decode(tagId);

            for (size_t i = 0; i < options.InstanceTags.size(); i++) {
                if (options.InstanceTags[i].first == tag.first) {
                    replacedInstanceTags[i] = true;
                }
            }

            consumer->OnLabel(tag.first, tag.second);
        }

        if (!options.Global) {
            for (size_t i = 0; i < options.InstanceTags.size(); i++) {
                if (replacedInstanceTags[i]) {
                    continue;
                }

                const auto& tag = options.InstanceTags[i];
                consumer->OnLabel(tag.first, tag.second);
            }
        }

        consumer->OnLabelsEnd();
    };

    auto skipByHack = [&] (const auto& window) {
        if (!options.Sparse) {
            return false;
        }

        for (const auto& readBatch : options.Times) {
            for (auto index : readBatch.first) {
                if (window.IsLingering(GetIteration(index) - options.LingerWindowSize)) {
                    return false;
                }
            }
        }

        return true;
    };

    auto skipSparse = [&] (auto window, const std::vector<int>& indices) {
        if (!options.Sparse) {
            return false;
        }

        for (auto index : indices) {
            if (window.IsLingering(GetIteration(index) - options.LingerWindowSize)) {
                return false;
            }
        }

        return true;
    };

    for (const auto& [tagIds, window] : Projections_) {
        if (options.EnableSolomonAggregationWorkaround && skipByHack(window)) {
            continue;
        }

        int sensorCount = 0;
        auto writeSummary = [&, tagIds=tagIds] (auto time, auto snapshot) {
            if (options.ExportSummary) {
                consumer->OnMetricBegin(NMonitoring::EMetricType::DSUMMARY);
                writeLabels(tagIds, {}, true);
                sensorCount += 5;
                consumer->OnSummaryDouble(time, snapshot);
                consumer->OnMetricEnd();
            }

            if (options.ExportSummaryAsMax) {
                consumer->OnMetricBegin(NMonitoring::EMetricType::GAUGE);
                writeLabels(tagIds, ".max", false);
                sensorCount += 1;
                consumer->OnDouble(time, snapshot->GetMax());
                consumer->OnMetricEnd();
            }

            if (options.ExportSummaryAsAvg && snapshot->GetCount() > 0) {
                consumer->OnMetricBegin(NMonitoring::EMetricType::GAUGE);
                writeLabels(tagIds, ".avg", false);
                sensorCount += 1;
                consumer->OnDouble(time, snapshot->GetSum() / snapshot->GetCount());
                consumer->OnMetricEnd();
            }
        };

        for (const auto& [indices, time] : options.Times) {
            if (!options.EnableSolomonAggregationWorkaround && skipSparse(window, indices)) {
                continue;
            }

            T value{};
            for (auto index : indices) {
                if (index < 0 || static_cast<size_t>(index) >= window.Values.size()) {
                    THROW_ERROR_EXCEPTION("Read index is invalid")
                        << TErrorAttribute("index", index)
                        << TErrorAttribute("window_size", window.Values.size());
                }

                value += window.Values[index];
            }

            if constexpr (std::is_same_v<T, i64> || std::is_same_v<T, TDuration>) {
                if (options.ConvertCountersToRateGauge) {
                    consumer->OnMetricBegin(NMonitoring::EMetricType::GAUGE);
                } else {
                    consumer->OnMetricBegin(NMonitoring::EMetricType::RATE);
                }

                writeLabels(tagIds, options.ConvertCountersToRateGauge ? std::optional(".rate") : std::nullopt, true);

                sensorCount = 1;
                if (options.ConvertCountersToRateGauge) {
                    if (options.RateDenominator < 0.1) {
                        THROW_ERROR_EXCEPTION("Invalid rate denominator");
                    }

                    if constexpr (std::is_same_v<T, i64>) {
                        consumer->OnDouble(time, value / options.RateDenominator);
                    } else {
                        consumer->OnDouble(time, value.SecondsFloat() / options.RateDenominator);
                    }
                } else {
                    // TODO(prime@): RATE is incompatible with windowed read. 
                    if constexpr (std::is_same_v<T, i64>) {
                        consumer->OnInt64(time, Rollup(window, indices.back()));
                    } else {
                        consumer->OnDouble(time, Rollup(window, indices.back()).SecondsFloat());
                    }
                }

                consumer->OnMetricEnd();
            } else if constexpr (std::is_same_v<T, double>) {
                if (options.DisableDefault && !window.HasValue[indices.back()]) {
                    continue;
                }

                consumer->OnMetricBegin(NMonitoring::EMetricType::GAUGE);

                writeLabels(tagIds, {}, true);

                sensorCount = 1;
                consumer->OnDouble(time, window.Values[indices.back()]);
                consumer->OnMetricEnd();
            } else if constexpr (std::is_same_v<T, TSummarySnapshot<double>>) {
                auto snapshot = MakeIntrusive<NMonitoring::TSummaryDoubleSnapshot>(
                    value.Sum(),
                    value.Min(),
                    value.Max(),
                    value.Last(),
                    static_cast<ui64>(value.Count())
                );

                writeSummary(time, snapshot);
            } else if constexpr (std::is_same_v<T, TSummarySnapshot<TDuration>>) {
                auto snapshot = MakeIntrusive<NMonitoring::TSummaryDoubleSnapshot>(
                    value.Sum().SecondsFloat(),
                    value.Min().SecondsFloat(),
                    value.Max().SecondsFloat(),
                    value.Last().SecondsFloat(),
                    static_cast<ui64>(value.Count())
                );

                writeSummary(time, snapshot);
            } else if constexpr (std::is_same_v<T, THistogramSnapshot>) {
                consumer->OnMetricBegin(NMonitoring::EMetricType::HIST);

                writeLabels(tagIds, {}, true);

                size_t n = value.Times.size();
                auto hist = NMonitoring::TExplicitHistogramSnapshot::New(n + 1);
                for (size_t i = 0; i != n; ++i) {
                    int bucketValue = i < value.Values.size() ? value.Values[i] : 0u;
                    (*hist)[i] = {value.Times[i].SecondsFloat(), bucketValue};
                }
                // add inf
                (*hist)[n] = {Max<NMonitoring::TBucketBound>(), n < value.Values.size() ? value.Values[n] : 0u};
                sensorCount = n + 1;
                consumer->OnHistogram(time, hist);
                consumer->OnMetricEnd();
            } else {
                THROW_ERROR_EXCEPTION("Unexpected cube type");
            }
        }

        sensorsEmitted += sensorCount;
    }

    return sensorsEmitted;
}

template <class T>
int TCube<T>::ReadSensorValues(
    const TTagIdList& tagIds,
    int index,
    const TReadOptions& options,
    TFluentAny fluent) const
{
    auto it = Projections_.find(tagIds);
    if (it == Projections_.end()) {
        return 0;
    }

    const auto& projection = it->second;
    const auto& value = projection.Values[index];

    int valuesRead = 0;
    if constexpr (std::is_same_v<T, i64>) {
        // NB(eshcherbin): Not much sense in returning rate here.
        fluent.Value(Rollup(projection, index));
        ++valuesRead;
    } else if constexpr (std::is_same_v<T, double>) {
        fluent.Value(value);
        ++valuesRead;
    } else if constexpr (std::is_same_v<T, TSummarySnapshot<double>>) {
        if (options.ExportSummaryAsMax) {
            fluent.Value(value.Max());
        } else {
            fluent
                .BeginMap()
                    .Item("sum").Value(value.Sum())
                    .Item("min").Value(value.Min())
                    .Item("max").Value(value.Max())
                    .Item("last").Value(value.Last())
                    .Item("count").Value(static_cast<ui64>(value.Count()))
                .EndMap();
        }
        ++valuesRead;
    } else if constexpr (std::is_same_v<T, TSummarySnapshot<TDuration>>) {
        if (options.ExportSummaryAsMax) {
            fluent.Value(value.Max().SecondsFloat());
        } else {
            fluent
                .BeginMap()
                    .Item("sum").Value(value.Sum().SecondsFloat())
                    .Item("min").Value(value.Min().SecondsFloat())
                    .Item("max").Value(value.Max().SecondsFloat())
                    .Item("last").Value(value.Last().SecondsFloat())
                    .Item("count").Value(static_cast<ui64>(value.Count()))
                .EndMap();
        }
        ++valuesRead;
    } else if constexpr (std::is_same_v<T, THistogramSnapshot>) {
        std::vector<std::pair<double, int>> hist;
        size_t n = value.Times.size();
        hist.reserve(n + 1);
        for (size_t i = 0; i != n; ++i) {
            int bucketValue = i < value.Values.size() ? value.Values[i] : 0;
            hist.emplace_back(value.Times[i].SecondsFloat(), bucketValue);
        }
        hist.emplace_back(Max<double>(), n < value.Values.size() ? value.Values[n] : 0u);

        fluent.DoMapFor(hist, [] (TFluentMap fluent, const auto& bar) {
            fluent
                .Item("bound").Value(bar.first)
                .Item("count").Value(bar.second);
        });
        ++valuesRead;
    } else {
        THROW_ERROR_EXCEPTION("Unexpected cube type");
    }

    return valuesRead;
}

////////////////////////////////////////////////////////////////////////////////

template class TCube<double>;
template class TCube<i64>;
template class TCube<TDuration>;
template class TCube<TSummarySnapshot<double>>;
template class TCube<TSummarySnapshot<TDuration>>;
template class TCube<THistogramSnapshot>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
