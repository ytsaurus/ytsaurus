#include "profiling.h"

#include "job_registry.h"
#include "monitoring.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <iterator>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Request type tag shared with the Java companion schema.
constexpr TStringBuf ProcessBatchRequestType = "process_batch";

//! |registry|, or the process-wide registry when null.
NProfiling::IRegistryPtr GetRegistryOrGlobal(const NProfiling::TSolomonRegistryPtr& registry)
{
    return registry ? NProfiling::IRegistryPtr(registry) : NProfiling::GetGlobalRegistry();
}

////////////////////////////////////////////////////////////////////////////////

//! Filters a producer-emitted origin tag.
class TOriginDroppingWriter
    : public NProfiling::ISensorWriter
{
public:
    explicit TOriginDroppingWriter(NProfiling::ISensorWriter* writer)
        : Writer_(writer)
    { }

    void PushTag(NProfiling::TTag tag) override
    {
        Forwarded_.push_back(tag.first != CompanionProcessTag);
        if (Forwarded_.back()) {
            Writer_->PushTag(std::move(tag));
        }
    }

    void PopTag() override
    {
        YT_VERIFY(!Forwarded_.empty());
        if (Forwarded_.back()) {
            Writer_->PopTag();
        }
        Forwarded_.pop_back();
    }

    void AddGauge(TStringBuf name, double value) override
    {
        Writer_->AddGauge(name, value);
    }

    void AddCounter(TStringBuf name, i64 value) override
    {
        Writer_->AddCounter(name, value);
    }

private:
    NProfiling::ISensorWriter* const Writer_;
    //! Whether each pushed tag was forwarded.
    std::vector<bool> Forwarded_;
};

//! Filters producer tags while preserving weak ownership and buffer semantics.
class TOriginDroppingProducer
    : public NProfiling::ISensorProducer
{
public:
    using TExpiredCallback = std::function<void(TOriginDroppingProducer*)>;

    TOriginDroppingProducer(
        const NProfiling::ISensorProducerPtr& producer,
        TExpiredCallback expiredCallback)
        : Producer_(producer)
        , ExpiredCallback_(std::move(expiredCallback))
    { }

    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        if (auto buffer = GetBuffer()) {
            buffer->WriteTo(writer);
        }
    }

    TIntrusivePtr<NProfiling::TSensorBuffer> GetBuffer() override
    {
        auto producer = Producer_.Lock();
        if (!producer) {
            ClearCachedBuffers();
            ExpiredCallback_(this);
            return nullptr;
        }
        auto source = producer->GetBuffer();
        if (!source) {
            ClearCachedBuffers();
            return nullptr;
        }

        auto guard = Guard(Lock_);
        // Preserve buffer identity so the registry can skip unchanged samples.
        if (source != LastSource_) {
            auto filtered = New<NProfiling::TSensorBuffer>();
            TOriginDroppingWriter droppingWriter(filtered.Get());
            source->WriteTo(&droppingWriter);
            LastSource_ = std::move(source);
            LastFiltered_ = std::move(filtered);
        }
        return LastFiltered_;
    }

private:
    const TWeakPtr<NProfiling::ISensorProducer> Producer_;
    const TExpiredCallback ExpiredCallback_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TIntrusivePtr<NProfiling::TSensorBuffer> LastSource_;
    TIntrusivePtr<NProfiling::TSensorBuffer> LastFiltered_;

    void ClearCachedBuffers()
    {
        auto guard = Guard(Lock_);
        LastSource_.Reset();
        LastFiltered_.Reset();
    }
};

//! Forwards registry calls to the underlying registry.
class TForwardingRegistry
    : public NProfiling::IRegistry
{
public:
    explicit TForwardingRegistry(NProfiling::IRegistryPtr underlying)
        : UnderlyingRegistry_(std::move(underlying))
    { }

    NProfiling::ICounterPtr RegisterCounter(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterCounter(name, tags, std::move(options));
    }

    NProfiling::ITimeCounterPtr RegisterTimeCounter(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterTimeCounter(name, tags, std::move(options));
    }

    NProfiling::IGaugePtr RegisterGauge(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterGauge(name, tags, std::move(options));
    }

    NProfiling::ITimeGaugePtr RegisterTimeGauge(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterTimeGauge(name, tags, std::move(options));
    }

    NProfiling::ISummaryPtr RegisterSummary(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterSummary(name, tags, std::move(options));
    }

    NProfiling::IGaugePtr RegisterGaugeSummary(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterGaugeSummary(name, tags, std::move(options));
    }

    NProfiling::ITimeGaugePtr RegisterTimeGaugeSummary(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterTimeGaugeSummary(name, tags, std::move(options));
    }

    NProfiling::ITimerPtr RegisterTimerSummary(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterTimerSummary(name, tags, std::move(options));
    }

    NProfiling::ITimerPtr RegisterTimeHistogram(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterTimeHistogram(name, tags, std::move(options));
    }

    NProfiling::IHistogramPtr RegisterGaugeHistogram(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterGaugeHistogram(name, tags, std::move(options));
    }

    NProfiling::IHistogramPtr RegisterRateHistogram(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options) override
    {
        return UnderlyingRegistry_->RegisterRateHistogram(name, tags, std::move(options));
    }

    void RegisterFuncCounter(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) override
    {
        UnderlyingRegistry_->RegisterFuncCounter(name, tags, std::move(options), owner, std::move(reader));
    }

    void RegisterFuncGauge(
        TStringBuf name,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) override
    {
        UnderlyingRegistry_->RegisterFuncGauge(name, tags, std::move(options), owner, std::move(reader));
    }

    void RegisterProducer(
        TStringBuf prefix,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options,
        const NProfiling::ISensorProducerPtr& producer) override
    {
        UnderlyingRegistry_->RegisterProducer(prefix, tags, std::move(options), producer);
    }

    void RenameDynamicTag(
        const NProfiling::TDynamicTagPtr& tag,
        TStringBuf name,
        TStringBuf value) override
    {
        UnderlyingRegistry_->RenameDynamicTag(tag, name, value);
    }

protected:
    const NProfiling::IRegistryPtr& GetUnderlyingRegistry() const
    {
        return UnderlyingRegistry_;
    }

private:
    const NProfiling::IRegistryPtr UnderlyingRegistry_;
};

//! Filters origin tags emitted by producers at collection time.
class TOriginFilteringRegistry
    : public TForwardingRegistry
{
public:
    explicit TOriginFilteringRegistry(NProfiling::IRegistryPtr underlying)
        : TForwardingRegistry(std::move(underlying))
    { }

    void RegisterProducer(
        TStringBuf prefix,
        const NProfiling::TTagSet& tags,
        NProfiling::TSensorOptions options,
        const NProfiling::ISensorProducerPtr& producer) override
    {
        auto droppingProducer = New<TOriginDroppingProducer>(
            producer,
            [weakThis = MakeWeak(this)] (TOriginDroppingProducer* expiredProducer) {
                if (auto this_ = weakThis.Lock()) {
                    auto guard = Guard(this_->ProducersLock_);
                    this_->Producers_.erase(expiredProducer);
                }
            });
        {
            auto guard = Guard(ProducersLock_);
            Producers_.emplace(droppingProducer.Get(), droppingProducer);
        }
        GetUnderlyingRegistry()->RegisterProducer(prefix, tags, std::move(options), droppingProducer);
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ProducersLock_);
    //! Owns stand-ins because the registry stores producers weakly.
    THashMap<TOriginDroppingProducer*, TIntrusivePtr<TOriginDroppingProducer>> Producers_;
};

NProfiling::IRegistryPtr CreateComputationRegistry(const NProfiling::TSolomonRegistryPtr& registry)
{
    auto underlying = GetRegistryOrGlobal(registry);
    return underlying
        ? NProfiling::IRegistryPtr(New<TOriginFilteringRegistry>(std::move(underlying)))
        : nullptr;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TComputationCounters::TComputationCounters(NProfiling::TProfiler profiler)
    : RequestCount(profiler.Counter("/request/count"))
    , RequestSize(profiler.Summary("/request/size"))
    , RequestDuration(profiler.Timer("/request/duration"))
    , RequestCpuTime(profiler.TimeCounter("/request/cpu_time"))
    , JobRecreationCount(profiler.Counter("/job/recreation/count"))
    , Profiler_(std::move(profiler))
{
    for (auto status : {
            NProto::NCompanion::RS_OK,
            NProto::NCompanion::RS_ERROR,
            NProto::NCompanion::RS_JOB_NOT_FOUND,
            NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED})
    {
        ResponseCounts_[status] = Profiler_
            .WithDefaultDisabled()
            .WithTag("status", NProto::NCompanion::EResponseStatus_Name(status))
            .Counter("/request/response/count");
    }
}

void TComputationCounters::ProfileResponse(NProto::NCompanion::EResponseStatus status)
{
    YT_VERIFY(status >= 0 && status < std::ssize(ResponseCounts_) && ResponseCounts_[status]);
    ResponseCounts_[status].Increment();
}

NProfiling::TSummary TComputationCounters::GetStateSizeSummary(
    TStringBuf direction,
    TStringBuf stateType,
    const std::string& stateName)
{
    auto key = Format("%v/%v/%v", direction, stateType, stateName);

    auto guard = Guard(StateSizesLock_);
    auto it = StateSizes_.find(key);
    if (it == StateSizes_.end()) {
        it = StateSizes_.emplace(
            key,
            Profiler_
                .WithTag("direction", std::string(direction))
                .WithTag("state_type", std::string(stateType))
                .WithTag("state_name", stateName)
                .Summary("/state/size"))
            .first;
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

TCompanionProfiler::TCompanionProfiler(
    const TJobRegistryPtr& jobRegistry,
    const NProfiling::TSolomonRegistryPtr& registry)
    : Profiler_(GetRegistryOrGlobal(registry), /*prefix*/ "", "yt.flow.companion")
    , ComputationProfiler_(
        NProfiling::TProfiler(
            CreateComputationRegistry(registry),
            /*prefix*/ "",
            "yt.flow.worker")
            .WithRequiredTag(CompanionProcessTag, CompanionProcessTagValue))
{
    Profiler_.AddFuncGauge(
        "/job/count",
        jobRegistry,
        [jobRegistry = jobRegistry.Get()] {
            return jobRegistry->GetJobCount();
        });
}

TComputationCountersPtr TCompanionProfiler::GetComputationCounters(
    const TComputationId& computationId)
{
    auto guard = Guard(Lock_);
    auto it = ComputationCounters_.find(computationId);
    if (it == ComputationCounters_.end()) {
        it = ComputationCounters_.emplace(
            computationId,
            New<TComputationCounters>(Profiler_
                    .WithTag("request_type", std::string(ProcessBatchRequestType))
                    .WithTag("computation_id", computationId.Underlying())))
            .first;
    }
    return it->second;
}

NProfiling::TProfiler TCompanionProfiler::GetComputationProfiler(
    const TComputationId& computationId) const
{
    return ComputationProfiler_
        .WithTag("computation_id", computationId.Underlying())
        .WithPrefix("/computation");
}

void TCompanionProfiler::ProfileResourceExecute(
    NCompanion::ECompanionResourceCommand command,
    NCompanion::ECompanionResourceExecuteStatus status)
{
    auto commandName = FormatEnum(command);
    auto statusName = FormatEnum(status);
    auto key = Format("%v/%v", commandName, statusName);

    auto guard = Guard(Lock_);
    auto it = ResourceExecuteCounters_.find(key);
    if (it == ResourceExecuteCounters_.end()) {
        it = ResourceExecuteCounters_.emplace(
            key,
            Profiler_
                .WithTag("command", commandName)
                .WithTag("status", statusName)
                .Counter("/resource/execute/count"))
            .first;
    }
    it->second.Increment();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
