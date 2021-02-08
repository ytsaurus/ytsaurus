#include "sensor.h"
#include "impl.h"

#include <yt/core/misc/assert.h>

#include <yt/core/profiling/timing.h>

#include <util/system/compiler.h>

#include <atomic>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TCounter::Increment(i64 delta) const
{
    if (!Counter_) {
        return;
    }

    Counter_->Increment(delta);
}

TCounter::operator bool() const
{
    return Counter_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TTimeCounter::Add(TDuration delta) const
{
    if (!Counter_) {
        return;
    }

    Counter_->Add(delta);
}

TTimeCounter::operator bool() const
{
    return Counter_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TGauge::Update(double value) const
{
    if (!Gauge_) {
        return;
    }

    Gauge_->Update(value);
}

TGauge::operator bool() const
{
    return Gauge_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TTimeGauge::Update(TDuration value) const
{
    if (!Gauge_) {
        return;
    }

    Gauge_->Update(value);
}

TTimeGauge::operator bool() const
{
    return Gauge_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TSummary::Record(double value) const
{
    if (!Summary_) {
        return;
    }

    Summary_->Record(value);
}

TSummary::operator bool() const
{
    return Summary_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TEventTimer::Record(TDuration value) const
{
    if (!Timer_) {
        return;
    }

    Timer_->Record(value);
}

TEventTimer::operator bool() const
{
    return Timer_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

TEventTimerGuard::TEventTimerGuard(TEventTimer timer)
    : Timer_(std::move(timer))
    , StartTime_(GetCpuInstant())
{ }

TEventTimerGuard::~TEventTimerGuard()
{
    if (!Timer_) {
        return;
    }

    auto now = GetCpuInstant();
    Timer_.Record(CpuDurationToDuration(now - StartTime_));
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TSensorOptions& options)
{
    return "{sparse=" + ToString(options.Sparse) +
        ";global=" + ToString(options.Global) +
        ";hot=" + ToString(options.Hot) +
        ":histogram_min=" + ToString(options.HistogramMin) +
        ":histogram_max=" + ToString(options.HistogramMax) +
        "}";
}

bool TSensorOptions::operator == (const TSensorOptions& other) const
{
    return Sparse == other.Sparse &&
        Global == other.Global &&
        Hot == other.Hot &&
        HistogramMin == other.HistogramMin &&
        HistogramMax == other.HistogramMax;
}

bool TSensorOptions::operator != (const TSensorOptions& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TRegistry::TRegistry(
    const IRegistryImplPtr& impl,
    const TString& prefix,
    const TString& _namespace)
    : Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
    , Impl_(impl)
{ }

TRegistry::TRegistry(
    const TString& prefix,
    const TString& _namespace,
    const TTagSet& tags,
    const IRegistryImplPtr& impl,
    TSensorOptions options)
    : Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
    , Tags_(tags)
    , Options_(options)
    , Impl_(impl ? impl : GetGlobalRegistry())
{ }

TRegistry TRegistry::WithPrefix(const TString& prefix) const
{
    if (!Enabled_) {
        return {};
    }

    return TRegistry(Prefix_ + prefix, Namespace_, Tags_, Impl_);
}

TRegistry TRegistry::WithTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddTag(std::pair(name, value), parent);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithRequiredTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddRequiredTag(std::pair(name, value), parent);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithExcludedTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddExcludedTag(std::pair(name, value), parent);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithAlternativeTag(const TString& name, const TString& value, int alternativeTo, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;

    allTags.AddAlternativeTag(std::pair(name, value), alternativeTo, parent);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithTags(const TTagSet& tags) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.Append(tags);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithSparse() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Sparse = true;
    return TRegistry(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TRegistry TRegistry::WithGlobal() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Global = true;
    return TRegistry(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TRegistry TRegistry::WithProjectionsDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.SetEnabled(false);
    return TRegistry(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TRegistry TRegistry::WithHot() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Hot = true;
    return TRegistry(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TCounter TRegistry::Counter(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TCounter counter;
    counter.Counter_ = Impl_->RegisterCounter(Namespace_ + Prefix_ + name, Tags_, Options_);;
    return counter;
}

TTimeCounter TRegistry::TimeCounter(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TTimeCounter counter;
    counter.Counter_ = Impl_->RegisterTimeCounter(Namespace_ + Prefix_ + name, Tags_, Options_);;
    return counter;
}

TGauge TRegistry::Gauge(const TString& name) const
{
    if (!Impl_) {
        return TGauge();
    }

    TGauge gauge;
    gauge.Gauge_ = Impl_->RegisterGauge(Namespace_ + Prefix_ + name, Tags_, Options_);;
    return gauge;
}

TTimeGauge TRegistry::TimeGauge(const TString& name) const
{
    if (!Impl_) {
        return TTimeGauge();
    }

    TTimeGauge gauge;
    gauge.Gauge_ = Impl_->RegisterTimeGauge(Namespace_ + Prefix_ + name, Tags_, Options_);;
    return gauge;
}

TSummary TRegistry::Summary(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TSummary summary;
    summary.Summary_ = Impl_->RegisterSummary(Namespace_ + Prefix_ + name, Tags_, Options_);;
    return summary;
}

TEventTimer TRegistry::Timer(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TEventTimer timer;
    timer.Timer_ = Impl_->RegisterTimerSummary(Namespace_ + Prefix_ + name, Tags_, Options_);
    return timer;
}

TEventTimer TRegistry::Histogram(const TString& name, TDuration min, TDuration max) const
{
    if (!Impl_) {
        return {};
    }

    auto options = Options_;
    options.HistogramMin = min;
    options.HistogramMax = max;

    TEventTimer timer;
    timer.Timer_ = Impl_->RegisterExponentialTimerHistogram(Namespace_ + Prefix_ + name, Tags_, options);;
    return timer;
}

void TRegistry::AddFuncCounter(
    const TString& name,
    const TIntrusivePtr<TRefCounted>& owner,
    std::function<i64()> reader) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterFuncCounter(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
}

void TRegistry::AddFuncGauge(
    const TString& name,
    const TIntrusivePtr<TRefCounted>& owner,
    std::function<double()> reader) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterFuncGauge(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
}

void TRegistry::AddProducer(
    const TString& prefix,
    const ISensorProducerPtr& producer) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterProducer(Namespace_ + Prefix_ + prefix, Tags_, Options_, producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
