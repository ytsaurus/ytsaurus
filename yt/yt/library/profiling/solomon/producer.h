#pragma once

#include "cube.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProducerCounters)

struct TProducerCounters final
{
    TString Prefix;
    TTagSet ProducerTags;
    TSensorOptions Options;

    THashMap<TString, TGauge> Gauges;
    THashMap<TString, std::pair<i64, TCounter>> Counters;
    THashMap<TTag, TProducerCountersPtr> Tags;
};

class TCounterWriter final : public ISensorWriter
{
public:
    TCounterWriter(IRegistryImplPtr registry, TProducerCountersPtr counters);

    virtual void PushTag(const TTag& tag) override;
    virtual void PopTag() override;
    virtual void AddGauge(const TString& name, double value) override;
    virtual void AddCounter(const TString& name, i64 value) override;

private:
    IRegistryImplPtr Registry_;
    std::vector<TProducerCountersPtr> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProducerState)

struct TProducerState final
{
    TProducerState(
        const TString& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        TWeakPtr<ISensorProducer> producer)
        : Producer(std::move(producer))
        , Counters(New<TProducerCounters>())
    {
        Counters->Prefix = prefix;
        Counters->ProducerTags = tags;
        Counters->Options = options;
    }

    const TWeakPtr<ISensorProducer> Producer;

    TProducerCountersPtr Counters;
};

////////////////////////////////////////////////////////////////////////////////

class TProducerSet
{
public:
    void AddProducer(TProducerStatePtr state);

    void Collect(IRegistryImplPtr profiler, IInvokerPtr invoker);

    void Profile(const TProfiler& profiler);

private:
    THashSet<TProducerStatePtr> Producers_;

    TProfiler SelfProfiler_;
    TEventTimer ProducerCollectDuration_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
