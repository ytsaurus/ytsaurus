#include "cube.h"
#include "remote.h"
#include "config.h"

#include <util/string/split.h>
#include <yt/yt/library/profiling/summary.h>
#include <yt/yt/library/profiling/tag.h>
#include <yt/yt/library/profiling/histogram_snapshot.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <type_traits>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TCube<T>::TProjection::IsZero(int index) const
{
    return IsZeroValue(Values[index]);
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
void TCube<T>::Add(TTagIdList tagIds)
{
    std::sort(tagIds.begin(), tagIds.end());
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
        Add(std::move(tagIds));
    });
}

template <class T>
void TCube<T>::Remove(TTagIdList tagIds)
{
    std::sort(tagIds.begin(), tagIds.end());
    auto it = Projections_.find(tagIds);
    if (it == Projections_.end()) {
        THROW_ERROR_EXCEPTION("Can't remove tags from cube")
            << TErrorAttribute("tag_ids", tagIds);
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
        Remove(std::move(tagIds));
    });
}

template <class T>
void TCube<T>::Update(TTagIdList tagIds, T value)
{
    std::sort(tagIds.begin(), tagIds.end());
    auto it = Projections_.find(tagIds);
    if (it == Projections_.end()) {
        THROW_ERROR_EXCEPTION("Can't update tags in cube")
            << TErrorAttribute("tag_ids", tagIds);
    }

    if constexpr (std::is_same_v<T, double>) {
        // Special value for gauges with DisableDefault option enabled.
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
    const std::string& name,
    const TReadOptions& options,
    TTagWriter* tagWriter,
    ::NMonitoring::IMetricConsumer* consumer) const
{
    const auto& scrapeOptions = options.ScrapeOptions;

    YT_VERIFY(CheckSummaryPolicy(options.SummaryPolicy));

    int sensorsEmitted = 0;

    auto getNameLabel = [&] (std::optional<TStringBuf> suffix) {
        static constexpr char TokenDelimiter = '/';

        // Let's not change anything if rename is disabled.
        // XXX(achulkov2): Does anyone actually use this? I can't find any usages.
        if (options.DisableSensorsRename) {
            return name;
        }

        std::string_view potentiallyStrippedName = name;

        // First, we strip the prefix (everything before last /), if requested.
        if (scrapeOptions->StripSensorsNamePrefix) {
            auto delimiterPos = potentiallyStrippedName.find_last_of(TokenDelimiter);
            if (delimiterPos != std::string::npos) {
                potentiallyStrippedName = potentiallyStrippedName.substr(delimiterPos + 1);
            }
        }

        // Now we normalize the sensor name in two ways: by replacing / with the delimiter
        // provided in options and by converting metric path tokens to camel case if requested.
        std::string normalizedSensorName;
        normalizedSensorName.reserve(name.size() + (suffix ? suffix->size() : 0));

        std::vector<std::string_view> tokens;
        StringSplitter(potentiallyStrippedName).Split(TokenDelimiter).SkipEmpty().Collect(&tokens);

        // The suffix should not contain a leading delimiter.
        if (suffix) {
            tokens.push_back(*suffix);
        }

        bool firstToken = true;
        for (auto token : tokens) {
            if (firstToken) {
                firstToken = false;
            } else {
                normalizedSensorName += scrapeOptions->SensorComponentDelimiter;
            }

            if (scrapeOptions->ConvertSensorComponentNamesToCamelCase) {
                normalizedSensorName += UnderscoreCaseToCamelCase(token);
            } else {
                normalizedSensorName += token;
            }
        }

        return normalizedSensorName;
    };

    auto prepareNameLabel = [&] (std::optional<TStringBuf> suffix) {
        return consumer->PrepareLabel("sensor", getNameLabel(suffix));
    };

    auto nameLabel = prepareNameLabel({});
    auto sumNameLabel = prepareNameLabel("sum");
    auto minNameLabel = prepareNameLabel("min");
    auto maxNameLabel = prepareNameLabel("max");
    auto avgNameLabel = prepareNameLabel("avg");
    auto rateNameLabel = prepareNameLabel("rate");
    auto deltaNameLabel = prepareNameLabel("delta");

    auto globalHostLabel = consumer->PrepareLabel("host", "");
    auto hostLabel = consumer->PrepareLabel("host", options.Host.value_or(""));
    auto ytAggrLabel = consumer->PrepareLabel("yt_aggr", "1");

    // Set allowAggregate to true to aggregate by host.
    // Currently Monitoring supports only sum aggregation.
    auto writeLabels = [&] (const auto& tagIds, std::pair<ui32, ui32> nameLabel, ::NMonitoring::EMetricType metricType, bool allowAggregate) {
        consumer->OnLabelsBegin();

        if (scrapeOptions->AddMetricTypeLabel) {
            // This should almost always be a cache hit.
            auto metricTypeLabel = consumer->PrepareLabel("metric_type", ToString(metricType));
            consumer->OnLabel(metricTypeLabel.first, metricTypeLabel.second);
        }

        consumer->OnLabel(nameLabel.first, nameLabel.second);

        if (options.Global) {
            consumer->OnLabel(globalHostLabel.first, globalHostLabel.second);
        } else if (options.Host) {
            consumer->OnLabel(hostLabel.first, hostLabel.second);
        }

        TCompactVector<bool, 8> replacedInstanceTags(options.InstanceTags.size());

        if (allowAggregate && scrapeOptions->MarkAggregates && !options.Global) {
            consumer->OnLabel(ytAggrLabel.first, ytAggrLabel.second);
        }

        for (auto tagId : tagIds) {
            const auto& tag = tagWriter->Decode(tagId);

            for (size_t i = 0; i < options.InstanceTags.size(); i++) {
                if (options.InstanceTags[i].first == tag.first) {
                    replacedInstanceTags[i] = true;
                }
            }

            tagWriter->WriteLabel(tagId);
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
        if (scrapeOptions->EnableAggregationWorkaround && skipByHack(window)) {
            continue;
        }

        int sensorCount = 0;

        bool empty = true;
        for (const auto& [indices, time] : options.Times) {
            if (!scrapeOptions->EnableAggregationWorkaround && skipSparse(window, indices)) {
                continue;
            }

            empty = false;
        }
        if (empty) {
            continue;
        }

        auto rangeValues = [&, window=&window] (auto cb) {
            for (const auto& [indices, time] : options.Times) {
                if (!scrapeOptions->EnableAggregationWorkaround && skipSparse(*window, indices)) {
                    continue;
                }

                // Indices form a sub-window, which is typically either corresponds to readGridStep seconds
                // in terms of the solomon exporter, or contains just a single index of the latest timestamp
                // to read.

                T value{};
                for (auto index : indices) {
                    if (index < 0 || static_cast<size_t>(index) >= window->Values.size()) {
                        THROW_ERROR_EXCEPTION("Read index is invalid")
                            << TErrorAttribute("index", index)
                            << TErrorAttribute("window_size", window->Values.size());
                    }

                    value += window->Values[index];
                }

                cb(value, time, indices);
            }
        };

        auto writeSummary = [&, tagIds=tagIds] (auto makeSummary) {
            bool omitSuffix = Any(options.SummaryPolicy & ESummaryPolicy::OmitNameLabelSuffix);

            auto writeMetric = [&] (
                ESummaryPolicy policyBit,
                std::pair<ui32, ui32> specificNameLabel,
                bool aggregate,
                NMonitoring::EMetricType type,
                auto cb)
            {
                if (Any(options.SummaryPolicy & policyBit)) {
                    consumer->OnMetricBegin(type);
                    writeLabels(tagIds, omitSuffix ? nameLabel : specificNameLabel, type, aggregate);

                    rangeValues(cb);

                    consumer->OnMetricEnd();
                }
            };

            auto writeGaugeSummaryMetric = [&] (
                ESummaryPolicy policyBit,
                std::pair<ui32, ui32> specificNameLabel,
                bool aggregate,
                double (NMonitoring::TSummaryDoubleSnapshot::*valueGetter)() const)
            {
                writeMetric(
                    policyBit,
                    specificNameLabel,
                    aggregate,
                    NMonitoring::EMetricType::GAUGE,
                    [&] (auto value, auto time, const auto& /*indices*/) {
                        sensorCount += 1;
                        consumer->OnDouble(time, (makeSummary(value).Get()->*valueGetter)());
                    });
            };

            writeMetric(
                ESummaryPolicy::All,
                nameLabel,
                /*aggregate*/ true,
                NMonitoring::EMetricType::DSUMMARY,
                [&] (auto value, auto time, const auto& /*indices*/) {
                    sensorCount += 5;
                    consumer->OnSummaryDouble(time, makeSummary(value));
                });

            writeGaugeSummaryMetric(
                ESummaryPolicy::Sum,
                sumNameLabel,
                /*aggregate*/ true,
                &NMonitoring::TSummaryDoubleSnapshot::GetSum);

            writeGaugeSummaryMetric(
                ESummaryPolicy::Min,
                minNameLabel,
                /*aggregate*/ false,
                &NMonitoring::TSummaryDoubleSnapshot::GetMin);

            writeGaugeSummaryMetric(
                ESummaryPolicy::Max,
                maxNameLabel,
                /*aggregate*/ false,
                &NMonitoring::TSummaryDoubleSnapshot::GetMax);

            if (Any(options.SummaryPolicy & ESummaryPolicy::Avg)) {
                bool empty = true;

                rangeValues([&] (auto value, auto time, const auto& /*indices*/) {
                    auto snapshot = makeSummary(value);
                    if (snapshot->GetCount() == 0) {
                        return;
                    }

                    if (empty) {
                        empty = false;
                        auto metricType = NMonitoring::EMetricType::GAUGE;
                        consumer->OnMetricBegin(metricType);
                        writeLabels(tagIds, omitSuffix ? nameLabel : avgNameLabel, metricType, false);
                    }

                    sensorCount += 1;

                    consumer->OnDouble(time, snapshot->GetSum() / snapshot->GetCount());
                });

                if (!empty) {
                    consumer->OnMetricEnd();
                }
            }
        };

        if constexpr (std::is_same_v<T, i64> || std::is_same_v<T, TDuration>) {
            auto metricType = scrapeOptions->ConvertCountersToRateGauge || scrapeOptions->ConvertCountersToDeltaGauge
                ? NMonitoring::EMetricType::GAUGE
                // Underlying encoding options for each format might be different, e.g. this becomes a COUNTER in prometheus.
                : NMonitoring::EMetricType::RATE;

            consumer->OnMetricBegin(metricType);

            auto counterNameLabel = nameLabel;
            if (scrapeOptions->RenameConvertedCounters && scrapeOptions->ConvertCountersToRateGauge) {
                counterNameLabel = rateNameLabel;
            }
            if (scrapeOptions->RenameConvertedCounters && scrapeOptions->ConvertCountersToDeltaGauge) {
                counterNameLabel = deltaNameLabel;
            }

            writeLabels(
                tagIds,
                counterNameLabel,
                metricType,
                /*allowAggregate*/ true);

            rangeValues([&, window=&window] (auto value, auto time, const auto& indices) {
                sensorCount += 1;
                if (scrapeOptions->ConvertCountersToRateGauge) {
                    if (options.RateDenominator < 0.1) {
                        THROW_ERROR_EXCEPTION("Invalid rate denominator");
                    }

                    // Rate denominator corresponds to the size of the indices sub-window in seconds.
                    if constexpr (std::is_same_v<T, i64>) {
                        consumer->OnDouble(time, value / options.RateDenominator);
                    } else {
                        consumer->OnDouble(time, value.SecondsFloat() / options.RateDenominator);
                    }
                } else if (scrapeOptions->ConvertCountersToDeltaGauge) {
                    if constexpr (std::is_same_v<T, i64>) {
                        consumer->OnDouble(time, value);
                    } else {
                        consumer->OnDouble(time, value.SecondsFloat());
                    }
                } else {
                    // TODO(prime@): RATE is incompatible with windowed read.
                    if constexpr (std::is_same_v<T, i64>) {
                        // This is effectively the sum of all values up until the specified index, which
                        // is just the value of the counter for the specified index. Same below.
                        consumer->OnInt64(time, Rollup(*window, indices.back()));
                    } else {
                        consumer->OnDouble(time, Rollup(*window, indices.back()).SecondsFloat());
                    }
                }
            });

            consumer->OnMetricEnd();
        } else if constexpr (std::is_same_v<T, double>) {
            auto metricType = NMonitoring::EMetricType::GAUGE;
            consumer->OnMetricBegin(metricType);

            writeLabels(tagIds, nameLabel, metricType, /*allowAggregate*/ true);

            rangeValues([&, window=&window] (auto /* value */, auto time, const auto& indices) {
                if (options.DisableDefault && !window->HasValue[indices.back()]) {
                    return;
                }

                sensorCount += 1;
                consumer->OnDouble(time, window->Values[indices.back()]);
            });

            consumer->OnMetricEnd();
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<double>>) {
            writeSummary([] (auto value) {
                return MakeIntrusive<NMonitoring::TSummaryDoubleSnapshot>(
                    value.Sum(),
                    value.Min(),
                    value.Max(),
                    value.Last(),
                    static_cast<ui64>(value.Count()));
            });
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<TDuration>>) {
            writeSummary([] (auto value) {
                return MakeIntrusive<NMonitoring::TSummaryDoubleSnapshot>(
                    value.Sum().SecondsFloat(),
                    value.Min().SecondsFloat(),
                    value.Max().SecondsFloat(),
                    value.Last().SecondsFloat(),
                    static_cast<ui64>(value.Count()));
            });
        } else if constexpr (std::is_same_v<T, TTimeHistogramSnapshot>) {
            auto metricType = NMonitoring::EMetricType::HIST;
            consumer->OnMetricBegin(metricType);

            writeLabels(tagIds, nameLabel, metricType, /*allowAggregate*/ true);

            rangeValues([&, window=&window] (auto value, auto time, const auto& indices) {
                size_t n = value.Bounds.size();
                auto hist = NMonitoring::TExplicitHistogramSnapshot::New(n + 1);

                if (scrapeOptions->ConvertCountersToRateGauge || options.EnableHistogramCompat) {
                    if (options.RateDenominator < 0.1) {
                        THROW_ERROR_EXCEPTION("Invalid rate denominator");
                    }

                    for (size_t i = 0; i < n; ++i) {
                        auto bucketValue = i < value.Values.size() ? value.Values[i] : 0u;

                        (*hist)[i] = {value.Bounds[i], bucketValue / options.RateDenominator};
                    }

                    // Add inf.
                    (*hist)[n] = {Max<NMonitoring::TBucketBound>(), n < value.Values.size() ? (value.Values[n] / options.RateDenominator) : 0u};
                } else {
                    auto rollup = Rollup(*window, indices.back());

                    for (size_t i = 0; i < n; ++i) {
                        auto bucketValue = i < rollup.Values.size() ? rollup.Values[i] : 0u;
                        (*hist)[i] = {rollup.Bounds[i], bucketValue};
                    }

                    // Add inf.
                    (*hist)[n] = {Max<NMonitoring::TBucketBound>(), n < rollup.Values.size() ? (rollup.Values[n]) : 0u};
                }

                sensorCount = n + 1;

                consumer->OnHistogram(time, hist);
            });

            consumer->OnMetricEnd();
        } else if constexpr (std::is_same_v<T, TGaugeHistogramSnapshot>) {
            auto metricType = NMonitoring::EMetricType::HIST;
            consumer->OnMetricBegin(metricType);

            writeLabels(tagIds, nameLabel, metricType, /*allowAggregate*/ true);

            rangeValues([&] (auto value, auto time, const auto& /*indices*/) {
                size_t n = value.Bounds.size();
                auto hist = NMonitoring::TExplicitHistogramSnapshot::New(n + 1);

                for (size_t i = 0; i < n; ++i) {
                    auto bucketValue = i < value.Values.size() ? value.Values[i] : 0u;
                    (*hist)[i] = {value.Bounds[i], bucketValue};
                }

                // Add inf.
                (*hist)[n] = {Max<NMonitoring::TBucketBound>(), n < value.Values.size() ? value.Values[n] : 0u};

                sensorCount = n + 1;

                consumer->OnHistogram(time, hist);
            });

            consumer->OnMetricEnd();
        } else if constexpr (std::is_same_v<T, TRateHistogramSnapshot>) {
            auto metricType = NMonitoring::EMetricType::HIST;
            consumer->OnMetricBegin(metricType);

            writeLabels(tagIds, nameLabel, metricType, /*allowAggregate*/ true);

            rangeValues([&] (auto value, auto time, const auto& /*indices*/) {
                size_t n = value.Bounds.size();
                auto hist = NMonitoring::TExplicitHistogramSnapshot::New(n + 1);

                if (options.RateDenominator < 0.1) {
                    THROW_ERROR_EXCEPTION("Invalid rate denominator");
                }

                for (size_t i = 0; i < n; ++i) {
                    auto bucketValue = i < value.Values.size() ? value.Values[i] : 0u;
                    (*hist)[i] = {value.Bounds[i], bucketValue / options.RateDenominator};
                }

                // Add inf.
                (*hist)[n] = {Max<NMonitoring::TBucketBound>(), n < value.Values.size() ? (value.Values[n] / options.RateDenominator) : 0u};

                sensorCount = n + 1;

                consumer->OnHistogram(time, hist);
            });

            consumer->OnMetricEnd();
        } else {
            THROW_ERROR_EXCEPTION("Unexpected cube type");
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
    const TTagRegistry& tagRegistry,
    TFluentAny fluent) const
{
    int valuesRead = 0;
    auto doReadValueForProjection = [&] (TFluentAny fluent, const TProjection& projection, const T& value) {
        if constexpr (std::is_same_v<T, i64> || std::is_same_v<T, TDuration>) {
            // NB(eshcherbin): Not much sense in returning rate here.
            if constexpr (std::is_same_v<T, i64>) {
                fluent.Value(Rollup(projection, index));
            } else {
                fluent.Value(Rollup(projection, index).SecondsFloat());
            }
            ++valuesRead;
        } else if constexpr (std::is_same_v<T, double>) {
            fluent.Value(value);
            ++valuesRead;
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<double>>) {
            if (Any(options.SummaryPolicy & ESummaryPolicy::All)) {
                fluent
                    .BeginMap()
                        .Item("sum").Value(value.Sum())
                        .Item("min").Value(value.Min())
                        .Item("max").Value(value.Max())
                        .Item("last").Value(value.Last())
                        .Item("count").Value(static_cast<ui64>(value.Count()))
                    .EndMap();
            } else if (Any(options.SummaryPolicy & ESummaryPolicy::Sum)) {
                fluent.Value(value.Sum());
            } else if (Any(options.SummaryPolicy & ESummaryPolicy::Min)) {
                fluent.Value(value.Min());
            } else if (Any(options.SummaryPolicy & ESummaryPolicy::Max)) {
                if (options.SummaryAsMaxForAllTime) {
                    fluent
                        .BeginMap()
                            .Item("max").Value(value.Max())
                            .Item("all_time_max").Value(Rollup(projection, index).Max())
                        .EndMap();
                } else {
                    fluent.Value(value.Max());
                }
            } else if (Any(options.SummaryPolicy & ESummaryPolicy::Avg)) {
                fluent.Value(value.Count() > 0 ? value.Sum() / value.Count() : NAN);
            }
            ++valuesRead;
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<TDuration>>) {
            if (Any(options.SummaryPolicy & ESummaryPolicy::Max) && options.SummaryAsMaxForAllTime) {
                fluent
                    .BeginMap()
                        .Item("max").Value(value.Max().SecondsFloat())
                        .Item("all_time_max").Value(Rollup(projection, index).Max().SecondsFloat())
                    .EndMap();
            } else if (Any(options.SummaryPolicy & ESummaryPolicy::Max)) {
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
        } else if constexpr (std::is_same_v<T, TTimeHistogramSnapshot> || std::is_same_v<T, TGaugeHistogramSnapshot> || std::is_same_v<T, TRateHistogramSnapshot>) {
            std::vector<std::pair<double, i64>> hist;
            size_t n = value.Bounds.size();
            hist.reserve(n + 1);
            for (size_t i = 0; i != n; ++i) {
                auto bucketValue = i < value.Values.size() ? value.Values[i] : 0;
                hist.emplace_back(value.Bounds[i], bucketValue);
            }
            hist.emplace_back(Max<double>(), n < value.Values.size() ? value.Values[n] : 0u);

            fluent.DoListFor(hist, [] (TFluentList fluent, const auto& bar) {
                fluent
                    .Item().BeginMap()
                        .Item("bound").Value(bar.first)
                        .Item("count").Value(bar.second)
                    .EndMap();
            });
            ++valuesRead;
        } else {
            THROW_ERROR_EXCEPTION("Unexpected cube type");
        }
    };

    // NB(eshcherbin): ReadAllProjections is intended only for debugging purposes.
    if (options.ReadAllProjections) {
        std::vector<TTagIdList> filteredProjectionTagIds;
        for (const auto& [projectionTagIds, _] : Projections_) {
            // NB(eshcherbin): All tagIds vector are guaranteed to be sorted.
            if (std::includes(projectionTagIds.begin(), projectionTagIds.end(), tagIds.begin(), tagIds.end())) {
                filteredProjectionTagIds.push_back(projectionTagIds);
            }
        }

        if (!filteredProjectionTagIds.empty()) {
            fluent.DoListFor(filteredProjectionTagIds, [&] (TFluentList fluent, const auto& projectionTagIds) {
                const auto& projection = GetOrCrash(Projections_, projectionTagIds);

                fluent
                    .Item().BeginMap()
                        .Item("tags").DoMapFor(projectionTagIds, [&] (TFluentMap fluent, auto tagId) {
                            const auto& [key, value] = tagRegistry.Decode(tagId);
                            fluent.Item(key).Value(value);
                        })
                        .Item("value").Do([&] (TFluentAny fluent) {
                            doReadValueForProjection(fluent, projection, projection.Values[index]);
                        })
                    .EndMap();
            });
        }
    } else  {
        auto it = Projections_.find(tagIds);
        if (it == Projections_.end()) {
            return valuesRead;
        }

        const auto& projection = it->second;
        doReadValueForProjection(fluent, projection, projection.Values[index]);
    }

    return valuesRead;
}

template <class T>
void TCube<T>::DumpCube(NProto::TCube *cube, const std::vector<TTagId>& extraTags) const
{
    for (const auto& [tagIds, window] : Projections_) {
        auto projection = cube->add_projections();
        for (auto tagId : tagIds) {
            projection->add_tag_ids(tagId);
        }
        for (auto tagId : extraTags) {
            projection->add_tag_ids(tagId);
        }

        projection->set_has_value(window.HasValue[Index_]);
        if constexpr (std::is_same_v<T, i64>) {
            projection->set_counter(window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TDuration>) {
            projection->set_duration(window.Values[Index_].GetValue());
        } else if constexpr (std::is_same_v<T, double>) {
            projection->set_gauge(window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<double>>) {
            ToProto(projection->mutable_summary(), window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TSummarySnapshot<TDuration>>) {
            ToProto(projection->mutable_timer(), window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TTimeHistogramSnapshot>) {
            ToProto(projection->mutable_time_histogram(), window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TGaugeHistogramSnapshot>) {
            ToProto(projection->mutable_gauge_histogram(), window.Values[Index_]);
        } else if constexpr (std::is_same_v<T, TRateHistogramSnapshot>) {
            ToProto(projection->mutable_rate_histogram(), window.Values[Index_]);
        } else {
            THROW_ERROR_EXCEPTION("Unexpected cube type");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TCube<double>;
template class TCube<i64>;
template class TCube<TDuration>;
template class TCube<TSummarySnapshot<double>>;
template class TCube<TSummarySnapshot<TDuration>>;
template class TCube<TTimeHistogramSnapshot>;
template class TCube<TGaugeHistogramSnapshot>;
template class TCube<TRateHistogramSnapshot>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
