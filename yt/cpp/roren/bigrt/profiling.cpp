#include "profiling.h"

#include <library/cpp/yt/memory/new.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TCounterImpl
    : public ICounterImpl
{
public:
    using TImpl = NSFStats::TSumMetric<ui64>::TProxy<>;

    explicit TCounterImpl(TImpl impl)
        : Counter_(std::move(impl))
    { }

    void Increment(i64 delta) override
    {
        if (delta >= 0) {
            Counter_.Inc(delta);
        } else {
            Counter_.Dec(-delta);
        }
    }

    i64 GetValue() override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TImpl Counter_;
};

////////////////////////////////////////////////////////////////////////////////

class TSolomonContextRegistryImpl
    : public IRegistryImpl
{
public:
    explicit TSolomonContextRegistryImpl(NSFStats::TSolomonContext solomonCtx)
        : Context_(std::move(solomonCtx))
    { }

    NSFStats::TSolomonContext CloneContext() const
    {
        return Context_.Detached();
    }

    static TVector<NSFStats::TSolomonContext::TLabel> GetLabelsFromTags(const TTagSet& tags) {
        const auto& list = tags.Tags();
        TVector<NSFStats::TSolomonContext::TLabel> labels(Reserve(list.size()));
        for (const auto& [name, value] : list) {
            labels.emplace_back(name, value);
        }
        return labels;
    }

    ICounterImplPtr RegisterCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions /*options*/) override
    {
        auto ctx = NSFStats::TSolomonContext(Context_, GetLabelsFromTags(tags));
        auto metric = ctx.Get<NSFStats::TSumMetric<ui64>>(name);
        return NYT::New<TCounterImpl>(std::move(metric));
    }

    ITimeCounterImplPtr RegisterTimeCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    IGaugeImplPtr RegisterGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    ITimeGaugeImplPtr RegisterTimeGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    ISummaryImplPtr RegisterSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    IGaugeImplPtr RegisterGaugeSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    ITimeGaugeImplPtr RegisterTimeGaugeSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    ITimerImplPtr RegisterTimerSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    ITimerImplPtr RegisterTimerHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    IGaugeHistogramImplPtr RegisterGaugeHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override
    {
        Y_UNUSED(name, tags, options);
        YT_UNIMPLEMENTED();
    }

    void RegisterFuncCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const NYT::TRefCountedPtr& owner,
        std::function<i64()> reader) override
    {
        Y_UNUSED(name, tags, options, owner, reader);
        YT_UNIMPLEMENTED();
    }

    void RegisterFuncGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const NYT::TRefCountedPtr& owner,
        std::function<double()> reader) override
    {
        Y_UNUSED(name, tags, options, owner, reader);
        YT_UNIMPLEMENTED();
    }

    void RegisterProducer(
        const TString& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) override
    {
        Y_UNUSED(prefix, tags, options, owner);
        YT_UNIMPLEMENTED();
    }

    void Flush()
    {
        Context_.Flush();
    }

    void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        const TString& name,
        const TString& value) override
    {
        Y_UNUSED(tag, name, value);
        YT_UNIMPLEMENTED();
    }

private:
    NSFStats::TSolomonContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

TProfiler CreateSolomonContextProfiler(NSFStats::TSolomonContext solomonCtx)
{
    auto registry = NYT::New<TSolomonContextRegistryImpl>(std::move(solomonCtx));
    return TProfiler(std::move(registry), "", "");
}

////////////////////////////////////////////////////////////////////////////////

NSFStats::TSolomonContext UnwrapSolomonContextProfiler(const TProfiler& profiler)
{
    auto registry = profiler.GetRegistry();
    auto solomonContextRegistry = dynamic_cast<TSolomonContextRegistryImpl*>(registry.Get());
    Y_VERIFY(solomonContextRegistry);
    return solomonContextRegistry->CloneContext();
}

////////////////////////////////////////////////////////////////////////////////

void FlushProfiler(const TProfiler& profiler)
{
    auto registry = profiler.GetRegistry();
    auto solomonContextRegistry = dynamic_cast<TSolomonContextRegistryImpl*>(registry.Get());
    if (solomonContextRegistry) {
        solomonContextRegistry->Flush();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
