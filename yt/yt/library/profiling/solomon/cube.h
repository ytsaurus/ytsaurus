#pragma once

#include "private.h"
#include "tag_registry.h"

#include <limits>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/summary.h>

#include <yt/core/ytree/fluent.h>

#include <library/cpp/monlib/metrics/metric_consumer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TReadOptions
{
    std::vector<std::pair<std::vector<int>, TInstant>> Times;

    std::function<bool(const TString&)> SensorFilter;

    bool ConvertCountersToRateGauge = false;
    double RateDenominator = 1.0;

    bool EnableSolomonAggregationWorkaround = false;

    // Direct summary export is not supported by solomon, yet.
    bool ExportSummary = false;
    bool ExportSummaryAsMax = false;
    bool ExportSummaryAsAvg = false;

    bool MarkAggregates = false;

    std::optional<TString> Host;

    std::vector<TTag> InstanceTags;

    bool Sparse = false;
    bool Global = false;
    bool DisableSensorsRename = false;

    int LingerWindowSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TCube
{
public:
    explicit TCube(int windowSize, i64 nextIteration);

    void Add(const TTagIdList& tagIds);
    void AddAll(const TTagIdList& tagIds, const TProjectionSet& projections);
    void Remove(const TTagIdList& tagIds);
    void RemoveAll(const TTagIdList& tagIds, const TProjectionSet& projections);

    void Update(const TTagIdList& tagIds, T value);

    void StartIteration();
    void FinishIteration();

    struct TProjection
    {
        T Rollup{};
        std::vector<T> Values;

        bool IsZero(int index) const;
        bool IsLingering(i64 iteration) const;

        i64 LastNonZeroIteration = std::numeric_limits<i64>::min();
        int UsageCount = 0;
    };

    const THashMap<TTagIdList, TCube::TProjection>& GetProjections() const;
    int GetSize() const;

    int GetIndex(i64 iteration) const;
    i64 GetIteration(int index) const;
    T Rollup(const TProjection& window, int index) const;

    int ReadSensors(
        const TString& name,
        const TReadOptions& options,
        const TTagRegistry& tagsRegistry,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TTagIdList& tagIds,
        int index,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

private:
    int WindowSize_;
    i64 NextIteration_;
    i64 BaseIteration_;
    int Index_ = 0;

    THashMap<TTagIdList, TProjection> Projections_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
