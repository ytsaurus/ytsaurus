#include "registry.h"

#include "sensor.h"
#include "percpu.h"

#include <type_traits>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/assert.h>

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TSolomonRegistry::TSolomonRegistry()
    : Producers_(&Tags_, Iteration_)
{ }

template <class TBase, class TSimple, class TPerCpu, class TFn>
TIntrusivePtr<TBase> selectImpl(bool hot, const TFn& fn)
{
    if (!hot) {
        auto counter = New<TSimple>();
        fn(counter);
        return counter;
    } else {
        auto counter = New<TPerCpu>();
        fn(counter);
        return counter;
    }
}

ICounterImplPtr TSolomonRegistry::RegisterCounter(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return selectImpl<ICounterImpl, TSimpleCounter, TPerCpuCounter>(options.Hot, [&, this] (const auto& counter) {
        DoRegister([this, name, tags, options, counter] () {
            auto reader = [ptr = counter.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddCounter(New<TCounterState>(counter, reader, Tags_.Encode(tags), tags));
        });
    });
}

ITimeCounterImplPtr TSolomonRegistry::RegisterTimeCounter(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return selectImpl<ITimeCounterImpl, TSimpleTimeCounter, TPerCpuTimeCounter>(
        options.Hot,
        [&, this] (const auto& counter) {
            DoRegister([this, name, tags, options, counter] () {
                auto set = FindSet(name, options);
                set->AddTimeCounter(New<TTimeCounterState>(counter, Tags_.Encode(tags), tags));
            });
        });
}

IGaugeImplPtr TSolomonRegistry::RegisterGauge(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return selectImpl<IGaugeImpl, TSimpleGauge, TPerCpuGauge>(options.Hot, [&, this] (const auto& gauge) {
        DoRegister([this, name, tags, options, gauge] () {
            auto reader = [ptr = gauge.Get()] {
                return ptr->GetValue();
            };

            auto set = FindSet(name, options);
            set->AddGauge(New<TGaugeState>(gauge, reader, Tags_.Encode(tags), tags));
        });
    });
}

ITimeGaugeImplPtr TSolomonRegistry::RegisterTimeGauge(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto gauge = New<TSimpleTimeGauge>();

    DoRegister([this, name, tags, options, gauge] () {
        auto reader = [ptr = gauge.Get()] {
            return ptr->GetValue().SecondsFloat();
        };

        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(gauge, reader, Tags_.Encode(tags), tags));
    });

    return gauge;
}

ISummaryImplPtr TSolomonRegistry::RegisterSummary(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return selectImpl<ISummaryImpl, TSimpleSummary<double>, TPerCpuSummary<double>>(options.Hot, [&, this] (const auto& summary) {
        DoRegister([this, name, tags, options, summary] () {
            auto set = FindSet(name, options);
            set->AddSummary(New<TSummaryState>(summary, Tags_.Encode(tags), tags));
        });
    });
}

ITimerImplPtr TSolomonRegistry::RegisterTimerSummary(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    return selectImpl<ITimerImpl, TSimpleSummary<TDuration>, TPerCpuSummary<TDuration>>(
        options.Hot,
        [&, this] (const auto& timer) {
            DoRegister([this, name, tags, options, timer] () {
                auto set = FindSet(name, options);
                set->AddTimerSummary(New<TTimerSummaryState>(timer, Tags_.Encode(tags), tags));
            });
        });
}

ITimerImplPtr TSolomonRegistry::RegisterTimerHistogram(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options)
{
    auto hist = New<THistogram>(options);
    DoRegister([this, name, tags, options, hist] () {
        auto set = FindSet(name, options);
        set->AddHistogram(New<THistogramState>(hist, Tags_.Encode(tags), tags));
    });
    return hist;
}

void TSolomonRegistry::RegisterFuncCounter(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<i64()> reader)
{
    DoRegister([this, name, tags, options, owner, reader] () {
        auto set = FindSet(name, options);
        set->AddCounter(New<TCounterState>(owner, reader, Tags_.Encode(tags), tags));
    });
}

void TSolomonRegistry::RegisterFuncGauge(
    const TString& name,
    const TTagSet& tags,
    TSensorOptions options,
    const TRefCountedPtr& owner,
    std::function<double()> reader)
{
    DoRegister([this, name, tags, options, owner, reader] () {
        auto set = FindSet(name, options);
        set->AddGauge(New<TGaugeState>(owner, reader, Tags_.Encode(tags), tags));
    });
}

void TSolomonRegistry::RegisterProducer(
    const TString& prefix,
    const TTagSet& tags,
    TSensorOptions options,
    const ISensorProducerPtr& producer)
{
    DoRegister([this, prefix, tags, options, producer] () {
        Producers_.AddProducer(New<TProducerState>(prefix, producer, options, Tags_.Encode(tags), tags));
    });
}

TSolomonRegistryPtr TSolomonRegistry::Get()
{
    struct TPtrLeaker
    {
        TSolomonRegistryPtr Ptr = New<TSolomonRegistry>();
    };

    return LeakySingleton<TPtrLeaker>()->Ptr;
}

i64 TSolomonRegistry::GetNextIteration() const
{
    return Iteration_;
}

void TSolomonRegistry::SetWindowSize(int windowSize)
{
    if (WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is already set");
    }

    WindowSize_ = windowSize;
    Producers_.SetWindowSize(windowSize);
}

int TSolomonRegistry::GetWindowSize() const
{
    if (!WindowSize_) {
        THROW_ERROR_EXCEPTION("Window size is not configured");
    }

    return *WindowSize_;
}

int TSolomonRegistry::IndexOf(i64 iteration) const
{
    return iteration % GetWindowSize();
}

void TSolomonRegistry::Profile(const TRegistry& profiler)
{
    SelfProfiler_ = profiler.WithPrefix("/solomon_registry");

    Producers_.Profile(SelfProfiler_);

    SensorCollectDuration_ = SelfProfiler_.Timer("/sensor_collect_duration");
    ReadDuration_ = SelfProfiler_.Timer("/read_duration");
    SensorCount_ = SelfProfiler_.Gauge("/sensor_count");
    ProjectionCount_ = SelfProfiler_.Gauge("/projection_count");
    TagCount_ = SelfProfiler_.Gauge("/tag_count");
    RegistrationCount_ = SelfProfiler_.Counter("/registration_count");
}

const TRegistry& TSolomonRegistry::GetSelfProfiler() const
{
    return SelfProfiler_;
}

template <class TFn>
void TSolomonRegistry::DoRegister(TFn fn)
{
    if (Disabled_) {
        return;
    }

    RegistrationQueue_.Enqueue(std::move(fn));
}

void TSolomonRegistry::SetDynamicTags(std::vector<TTag> dynamicTags)
{
    auto guard = Guard(DynamicTagsLock_);
    std::swap(DynamicTags_, dynamicTags);
}

std::vector<TTag> TSolomonRegistry::GetDynamicTags()
{
    auto guard = Guard(DynamicTagsLock_);
    return DynamicTags_;
}

void TSolomonRegistry::Disable()
{
    Disabled_ = true;
    RegistrationQueue_.DequeueAll();
}

void TSolomonRegistry::ProcessRegistrations()
{
    GetWindowSize();
    RegistrationCount_.Increment();

    RegistrationQueue_.DequeueAll(false, [this] (const std::function<void()>& fn) {
        fn();

        TagCount_.Update(Tags_.GetSize());
    });
}

void TSolomonRegistry::Collect()
{
    i64 projectionCount = 0;
    for (auto& [name, set] : Sensors_) {
        auto start = TInstant::Now();
        projectionCount += set.Collect();
        SensorCollectDuration_.Record(TInstant::Now() - start);
    }

    projectionCount += Producers_.Collect();

    ProjectionCount_.Update(projectionCount);
    Iteration_++;
}

void TSolomonRegistry::ReadSensors(
    const TReadOptions& options,
    NMonitoring::IMetricConsumer* consumer) const
{
    auto readOptions = options;
    {
        auto guard = Guard(DynamicTagsLock_);
        readOptions.InstanceTags.insert(
            readOptions.InstanceTags.end(),
            DynamicTags_.begin(),
            DynamicTags_.end());
    }

    for (const auto& [name, set] : Sensors_) {
        if (readOptions.SensorFilter && !readOptions.SensorFilter(name)) {
            continue;
        }

        auto start = TInstant::Now();
        set.ReadSensors(name, readOptions, Tags_, consumer);
        ReadDuration_.Record(TInstant::Now() - start);
    }

    Producers_.ReadSensors(readOptions, consumer);
}

std::vector<TSensorInfo> TSolomonRegistry::ListSensors() const
{
    auto list = Producers_.ListSensors();
    for (const auto& [name, set] : Sensors_) {
        list.push_back(TSensorInfo{name, set.GetObjectCount(), set.GetCubeSize(), set.GetError()});
    }
    return list;
}

const TTagRegistry& TSolomonRegistry::GetTags() const
{
    return Tags_;
}

TSensorSet* TSolomonRegistry::FindSet(const TString& name, const TSensorOptions& options)
{
    if (auto it = Sensors_.find(name); it != Sensors_.end()) {
        it->second.ValidateOptions(options);
        return &it->second;
    } else {
        it = Sensors_.emplace(name, TSensorSet{options, Iteration_, GetWindowSize()}).first;
        it->second.Profile(SelfProfiler_.WithTag("metric_name", name));
        SensorCount_.Update(Sensors_.size());
        return &it->second;
    }
}

void TSolomonRegistry::LegacyReadSensors()
{
    for (auto [name, set] : Sensors_) {
        set.LegacyReadSensors(name, &Tags_);
    }

    Producers_.LegacyReadSensors();
}

////////////////////////////////////////////////////////////////////////////////

// This function overrides weak symbol defined in impl.cpp
IRegistryImplPtr GetGlobalRegistry()
{
    return TSolomonRegistry::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
