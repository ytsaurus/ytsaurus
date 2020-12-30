#pragma once

#include "cube.h"
#include "tag_registry.h"
#include "sensor.h"

#include <yt/yt/library/profiling/tag.h>

#include <yt/core/profiling/public.h>

#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/error.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCounterState)

struct TCounterState final
{
    TCounterState(
        TWeakPtr<TRefCounted> owner,
        std::function<i64()> reader,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<i64()> Reader;
    i64 LastValue = 0;

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimeCounterState)

struct TTimeCounterState final
{
    TTimeCounterState(
        TWeakPtr<ITimeCounterImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ITimeCounterImpl> Owner;
    TDuration LastValue = TDuration::Zero();

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TTimeCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TGaugeState)

struct TGaugeState final
{
    TGaugeState(
        TWeakPtr<TRefCounted> owner,
        std::function<double()> reader,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<double()> Reader;

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TGaugeState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSummaryState)

struct TSummaryState final
{
    TSummaryState(
        TWeakPtr<ISummaryImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(std::move(owner))
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ISummaryImpl> Owner;

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TSummaryState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimerSummaryState)

struct TTimerSummaryState final
{
    TTimerSummaryState(
        TWeakPtr<ITimerImpl> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(owner)
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<ITimerImpl> Owner;

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(TTimerSummaryState)

////////////////////////////////////////////////////////////////////////////////


DECLARE_REFCOUNTED_STRUCT(THistogramState)

struct THistogramState final
{
    THistogramState(
        TWeakPtr<THistogram> owner,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Owner(owner)
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TWeakPtr<THistogram> Owner;

    const TTagIdList TagIds;
    const TProjectionSet Projections;
};

DEFINE_REFCOUNTED_TYPE(THistogramState)

////////////////////////////////////////////////////////////////////////////////

class TSensorSet
{
public:
    TSensorSet(TSensorOptions options, i64 iteration, int windowSize);

    bool IsEmpty() const;

    void Profile(const TRegistry& profiler);
    void ValidateOptions(TSensorOptions options);

    void AddCounter(TCounterStatePtr counter);
    void AddGauge(TGaugeStatePtr gauge);
    void AddSummary(TSummaryStatePtr summary);
    void AddTimerSummary(TTimerSummaryStatePtr timer);
    void AddTimeCounter(TTimeCounterStatePtr counter);
    void AddHistogram(THistogramStatePtr counter);

    int Collect();

    void ReadSensors(
        const TString& name,
        const TReadOptions& options,
        const TTagRegistry& tagsRegistry,
        ::NMonitoring::IMetricConsumer* consumer) const;

    void LegacyReadSensors(const TString& name, TTagRegistry* tagRegistry);

    int GetObjectCount() const;
    int GetCubeSize() const;
    const TError& GetError() const;

private:
    const TSensorOptions Options_;

    TError Error_;

    THashSet<TCounterStatePtr> Counters_;
    TCube<i64> CountersCube_;

    THashSet<TTimeCounterStatePtr> TimeCounters_;
    TCube<i64> TimeCountersCube_;

    THashSet<TGaugeStatePtr> Gauges_;
    TCube<double> GaugesCube_;

    THashSet<TSummaryStatePtr> Summaries_;
    TCube<TSummarySnapshot<double>> SummariesCube_;

    THashSet<TTimerSummaryStatePtr> Timers_;
    TCube<TSummarySnapshot<TDuration>> TimersCube_;

    THashSet<THistogramStatePtr> Histograms_;
    TCube<THistogramSnapshot> HistogramsCube_;

    std::optional<int> Type_;
    TGauge CubeSize_;
    TGauge SensorsEmitted_;

    void OnError(TError error);

    void InitializeType(int index);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
