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

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

TProducerBuffer::TProducerBuffer(
    TSensorOptions options,
    i64 iteration,
    int windowSize)
    : Options(options)
    , CountersCube(windowSize, iteration)
    , GaugesCube(windowSize, iteration)
{ }

bool TProducerBuffer::IsEmpty() const
{
    return CountersCube.GetSize() == 0 && GaugesCube.GetSize() == 0;
}

void TProducerBuffer::Validate(const TSensorOptions& options)
{
    if (options != Options) {
        OnError(TError("Conflicting sensor options")
            << TErrorAttribute("current_options", ToString(Options))
            << TErrorAttribute("new_options", ToString(Options)));
    }
}

void TProducerBuffer::OnError(const TError& error)
{
    if (!Error.IsOK()) {
        Error = error;
    }
}

////////////////////////////////////////////////////////////////////////////////

TProducerSet::TProducerSet(TTagRegistry* tagRegistry, i64 iteration)
    : TagRegistry_(tagRegistry)
    , Iteration_(iteration)
{ }

void TProducerSet::SetWindowSize(int size)
{
    if (WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is already set");
    }

    WindowSize_ = size;
}

void TProducerSet::AddProducer(TProducerStatePtr state)
{
    if (!WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is not configured");
    }

    Producers_.insert(std::move(state));
}

int TProducerSet::Collect()
{
    int projectionCount = 0;

    for (auto& [name, buffer] : Buffers_) {
        buffer.CountersCube.StartIteration();
        buffer.GaugesCube.StartIteration();
    }

    std::deque<TProducerStatePtr> toRemove;
    for (const auto& producer : Producers_) {
        auto owner = producer->Producer.Lock();
        if (!owner) {
            toRemove.push_back(producer);
            continue;
        }

        auto startTime = TInstant::Now();
        auto reportTime = Finally([this, startTime] {
            ProducerCollectDuration_.Record(TInstant::Now() - startTime);
        });

        TSensorBuffer buffer;
        try {
            owner->CollectSensors(&buffer);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Producer read failed");
            continue;
        }

        projectionCount += buffer.GetCounters().size();
        projectionCount += buffer.GetGauges().size();

        auto commonTags = producer->TagIds;
        auto commonTagsSize = commonTags.size();

        auto projections = producer->Projections;
        for (const auto& [name, tags, value] : buffer.GetCounters()) {
            auto tagIdList = TagRegistry_->Encode(tags);

            commonTags.resize(commonTagsSize);
            commonTags.append(tagIdList.begin(), tagIdList.end());
            projections.Resize(commonTags.size());

            auto fullName = producer->Prefix + name;
            auto& counter = producer->Counters[fullName];
            auto& buffer = *Find(fullName, producer->Options);

            auto it = counter.find(commonTags);
            if (it == counter.end()) {
                buffer.CountersCube.AddAll(commonTags, projections);

                it = counter.emplace(commonTags, 0).first;
            }

            auto delta = value - it->second;
            it->second = value;
            projections.Range(commonTags, [&] (auto tags) {
                buffer.CountersCube.Update(tags, delta);
            });
        }

        for (const auto& [name, tags, value] : buffer.GetGauges()) {
            auto tagIdList = TagRegistry_->Encode(tags);

            commonTags.resize(commonTagsSize);
            commonTags.append(tagIdList.begin(), tagIdList.end());
            projections.Resize(commonTags.size());

            auto fullName = producer->Prefix + name;
            auto& gauge = producer->Gauges[fullName];
            auto& buffer = *Find(fullName, producer->Options);

            auto it = gauge.find(commonTags);
            if (it == gauge.end()) {
                buffer.GaugesCube.AddAll(commonTags, projections);

                bool ok = false;
                std::tie(it, ok) = gauge.emplace(commonTags);
            }

            projections.Range(commonTags, [&, value=value] (auto tags) {
                buffer.GaugesCube.Update(tags, value);
            });
        }
    }

    Iteration_++;
    for (auto& [name, buffer] : Buffers_) {
        buffer.CountersCube.FinishIteration();
        buffer.GaugesCube.FinishIteration();
        buffer.CubeSize.Update(buffer.CountersCube.GetSize() + buffer.GaugesCube.GetSize());
    }

    for (const auto& producer : toRemove) {
        Producers_.erase(producer);

        for (const auto& [name, tags] : producer->Counters) {
            auto it = Buffers_.find(name);
            if (it == Buffers_.end()) {
                THROW_ERROR_EXCEPTION("Broken producer")
                    << TErrorAttribute("name", name);
            }

            auto projections = producer->Projections;
            for (const auto& tag : tags) {
                projections.Resize(tag.first.size());
                it->second.CountersCube.RemoveAll(tag.first, projections);
            }

            if (it->second.IsEmpty()) {
                Buffers_.erase(it);
            }
        }

        for (const auto& [name, tags] : producer->Gauges) {
            auto it = Buffers_.find(name);
            if (it == Buffers_.end()) {
                THROW_ERROR_EXCEPTION("Broken producer")
                    << TErrorAttribute("name", name);
            }

            auto projections = producer->Projections;
            for (const auto& tag : tags) {
                projections.Resize(tag.size());
                it->second.GaugesCube.RemoveAll(tag, projections);
            }

            if (it->second.IsEmpty()) {
                Buffers_.erase(it);
            }
        }
    }

    return projectionCount;
}

void TProducerSet::ReadSensors(
    const TReadOptions& options,
    NMonitoring::IMetricConsumer* consumer) const
{
    for (const auto& [name, buffer] : Buffers_) {
        if (options.SensorFilter && !options.SensorFilter(name)) {
            continue;
        }

        if (!buffer.Error.IsOK()) {
            continue;
        }

        auto readOptions = options;
        readOptions.Sparse = buffer.Options.Sparse;
        readOptions.Global = buffer.Options.Global;
        readOptions.DisableSensorsRename = buffer.Options.DisableSensorsRename;
        readOptions.DisableDefault = buffer.Options.DisableDefault;

        int sensorsEmitted = 0;
        sensorsEmitted += buffer.CountersCube.ReadSensors(name, readOptions, *TagRegistry_, consumer);
        sensorsEmitted += buffer.GaugesCube.ReadSensors(name, readOptions, *TagRegistry_, consumer);
        buffer.SensorEmitted.Update(sensorsEmitted);
    }
}

int TProducerSet::ReadSensorValues(
    const TString& name,
    const TTagIdList& tagIds,
    int index,
    const TReadOptions& options,
    TFluentAny fluent) const
{
    auto it = Buffers_.find(name);
    if (it == Buffers_.end()) {
        return 0;
    }

    const auto& buffer = it->second;
    if (!buffer.Error.IsOK()) {
        THROW_ERROR_EXCEPTION("Broken sensor")
            << buffer.Error;
    }

    int valuesRead = 0;
    valuesRead += buffer.CountersCube.ReadSensorValues(tagIds, index, options, fluent);
    valuesRead += buffer.GaugesCube.ReadSensorValues(tagIds, index, options, fluent);

    return valuesRead;
}

void TProducerSet::LegacyReadSensors()
{
    for (const auto& producer : Producers_) {
        auto owner = producer->Producer.Lock();
        if (!owner) {
            continue;
        }

        auto commonTags = TagRegistry_->EncodeLegacy(producer->TagIds);

        TSensorBuffer buffer;
        owner->CollectSensors(&buffer);

        for (const auto& [name, tags, value] : buffer.GetCounters()) {
            auto prefix = TStringBuf{producer->Prefix};
            prefix.SkipPrefix("yt");
            prefix.SkipPrefix("yp");

            auto fullName = TString{prefix};
            fullName += name;

            TQueuedSample sample;
            sample.Time = GetCpuInstant();
            sample.Path = fullName;
            sample.Value = value;
            sample.TagIds = commonTags + TagRegistry_->EncodeLegacy(TagRegistry_->Encode(tags));
            sample.MetricType = EMetricType::Counter;

            TProfileManager::Get()->Enqueue(sample, false);
        }

        for (const auto& [name, tags, value] : buffer.GetGauges()) {
            auto prefix = TStringBuf{producer->Prefix};
            prefix.SkipPrefix("yt");
            prefix.SkipPrefix("yp");

            auto fullName = TString{prefix};
            fullName += name;

            TQueuedSample sample;
            sample.Time = GetCpuInstant();
            sample.Path = fullName;
            sample.Value = value;
            sample.TagIds = commonTags + TagRegistry_->EncodeLegacy(TagRegistry_->Encode(tags));
            sample.MetricType = EMetricType::Gauge;

            TProfileManager::Get()->Enqueue(sample, false);
        }
    }
}

std::vector<TSensorInfo> TProducerSet::ListSensors() const
{
    THashMap<TString, int> objectCount;
    for (const auto& producer : Producers_) {
        for (const auto& [name, tags] : producer->Counters) {
            objectCount[name] += tags.size();
        }
        for (const auto& [name, tags] : producer->Gauges) {
            objectCount[name] += tags.size();
        }
    }

    std::vector<TSensorInfo> list;
    for (const auto& [name, set] : Buffers_) {
        auto cubeSize = set.CountersCube.GetSize() + set.GaugesCube.GetSize();
        list.push_back(TSensorInfo{name, objectCount[name], cubeSize, set.Error});
    }
    return list;
}

void TProducerSet::Profile(const TProfiler& profiler)
{
    SelfProfiler_ = profiler;
    ProducerCollectDuration_ = profiler.Timer("/producer_collect_duration");
}

TProducerBuffer* TProducerSet::Find(const TString& name, const TSensorOptions& options)
{
    auto it = Buffers_.find(name);
    if (it != Buffers_.end()) {
        it->second.Validate(options);
        return &it->second;
    }

    it = Buffers_.emplace(name, TProducerBuffer{options, Iteration_, *WindowSize_}).first;
    it->second.CountersCube.StartIteration();
    it->second.GaugesCube.StartIteration();

    auto profiler = SelfProfiler_.WithTag("metric_name", name);
    it->second.CubeSize = profiler.Gauge("/cube_size");
    it->second.SensorEmitted = profiler.Gauge("/sensors_emitted");
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
