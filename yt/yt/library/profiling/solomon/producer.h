#pragma once

#include "cube.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/impl.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProducerState)

struct TProducerState final
{
    TProducerState(
        TString prefix,
        TWeakPtr<ISensorProducer> producer,
        const TSensorOptions& options,
        const TTagIdList& tagIds,
        const TProjectionSet& projections)
        : Prefix(std::move(prefix))
        , Producer(std::move(producer))
        , Options(options)
        , TagIds(tagIds)
        , Projections(projections)
    { }

    const TString Prefix;
    const TWeakPtr<ISensorProducer> Producer;
    const TSensorOptions Options;
    const TTagIdList TagIds;
    const TProjectionSet Projections;

    THashMap<TString, THashMap<TTagIdList, i64>> Counters;
    THashMap<TString, THashSet<TTagIdList>> Gauges;
};

////////////////////////////////////////////////////////////////////////////////

struct TProducerBuffer
{
    TProducerBuffer(TSensorOptions options, i64 iteration, int windowSize);

    const TSensorOptions Options;

    TError Error;

    void Validate(const TSensorOptions& options);
    void OnError(const TError& error);
    bool IsEmpty() const;

    TCube<i64> CountersCube;
    TCube<double> GaugesCube;

    TGauge CubeSize;
    TGauge SensorEmitted;
};

////////////////////////////////////////////////////////////////////////////////

struct TSensorInfo
{
    TString Name;
    int ObjectCount;
    int CubeSize;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TProducerSet
{
public:
    TProducerSet(TTagRegistry* tagRegistry, i64 iteration);

    void AddProducer(TProducerStatePtr state);
    void SetWindowSize(int size);

    int Collect();

    void ReadSensors(
        const TReadOptions& options,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TString& name,
        const TTagIdList& tagIds,
        int index,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

    std::vector<TSensorInfo> ListSensors() const;
    void LegacyReadSensors();

    void Profile(const TProfiler& profiler);

private:
    TTagRegistry* const TagRegistry_;
    i64 Iteration_ = 0;
    std::optional<int> WindowSize_;
    THashSet<TProducerStatePtr> Producers_;
    THashMap<TString, TProducerBuffer> Buffers_;

    TProfiler SelfProfiler_;
    TEventTimer ProducerCollectDuration_;

    TProducerBuffer* Find(const TString& name, const TSensorOptions& options);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
