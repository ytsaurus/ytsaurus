#include "producer.h"
#include "private.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TProducerState)
DEFINE_REFCOUNTED_TYPE(TProducerCounters)

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

TCounterWriter::TCounterWriter(IRegistryImplPtr registry, TProducerCountersPtr counters)
    : Registry_(registry)
    , Counters_{{counters}}
{ }

void TCounterWriter::PushTag(const TTag& tag)
{
    auto& nested = Counters_.back()->Tags[tag];
    if (!nested) {
        nested = New<TProducerCounters>();
        nested->Prefix = Counters_.back()->Prefix;
        nested->ProducerTags = Counters_.back()->ProducerTags;
        nested->Options = Counters_.back()->Options;
        nested->ProducerTags.AddTag(tag);
    }
    Counters_.push_back(nested);
}

void TCounterWriter::PopTag()
{
    Counters_.pop_back();
}

void TCounterWriter::AddGauge(const TString& name, double value)
{
    auto& gauge = Counters_.back()->Gauges[name];
    if (!gauge) {
        TProfiler profiler{
            Counters_.back()->Prefix,
            "",
            Counters_.back()->ProducerTags,
            Registry_,
            Counters_.back()->Options,
        };

        gauge = profiler.Gauge(name);
    }
    gauge.Update(value);
}

void TCounterWriter::AddCounter(const TString& name, i64 value)
{
    auto& counter = Counters_.back()->Counters[name];
    if (!counter.second) {
        TProfiler profiler{
            Counters_.back()->Prefix,
            "",
            Counters_.back()->ProducerTags,
            Registry_,
            Counters_.back()->Options,
        };

        counter.second = profiler.Counter(name);
    }

    if (value >= counter.first) {
        auto delta = value - counter.first;
        counter.second.Increment(delta);
        counter.first = value;
    } else {
        // Some producers use counter incorrectly.
        counter.first = value;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TProducerSet::AddProducer(TProducerStatePtr state)
{
    Producers_.insert(std::move(state));
}

void TProducerSet::Collect(IRegistryImplPtr profiler, IInvokerPtr invoker)
{
    std::vector<TFuture<void>> offload;
    std::deque<TProducerStatePtr> toRemove;
    for (const auto& producer : Producers_) {
        auto owner = producer->Producer.Lock();
        if (!owner) {
            toRemove.push_back(producer);
            continue;
        }

        auto future = BIND([profiler, owner, producer, collectDuration=ProducerCollectDuration_] () {
            auto startTime = TInstant::Now();
            auto reportTime = Finally([&] {
                collectDuration.Record(TInstant::Now() - startTime);
            });

            TCounterWriter writer(profiler, producer->Counters);
            try {
                owner->CollectSensors(&writer);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Producer read failed");
                return;
            }
        })
            .AsyncVia(invoker)
            .Run();

        offload.push_back(future);
    }

    for (auto& future : offload) {
        future.Get();
    }

    for (const auto& producer : toRemove) {
        Producers_.erase(producer);
    }
}

void TProducerSet::Profile(const TProfiler& profiler)
{
    SelfProfiler_ = profiler;
    ProducerCollectDuration_ = profiler.Timer("/producer_collect_duration");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
