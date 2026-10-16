#include "watermark_generator.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/client/api/transaction.h>

#include <list>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TWatermarkGeneratorState::Register(TRegistrar registrar)
{
    registrar.Parameter("max", &TThis::Max)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("min_ahead", &TThis::MinAhead)
        .Default();
    registrar.Parameter("max_ahead", &TThis::MaxAhead)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

class TWatermarkGenerator
    : public IWatermarkGenerator
{
public:
    TWatermarkGenerator(TWatermarkGeneratorSpecPtr spec, NProfiling::TProfiler profiler, NLogging::TLogger logger)
        : Spec_(std::move(spec))
        , Profiler_(profiler)
        , Logger(std::move(logger))
    { }

    TWatermarkGeneratorCookie RegisterRead(const std::vector<TMessage>& messages) override
    {
        if (messages.empty()) {
            return TWatermarkGeneratorCookie(std::any{});
        }

        TBatchInfo info;
        for (const auto& message : messages) {
            ReadState_->Max = std::max(ReadState_->Max, message.EventTimestamp);
            info.MinEventTimestamp = std::min(info.MinEventTimestamp, message.EventTimestamp);
            info.MaxEventTimestamp = std::max(info.MaxEventTimestamp, message.EventTimestamp);
        }
        info.CollapsedMax = info.MaxEventTimestamp;
        auto it = Inflight_.insert(Inflight_.end(), info);

        if (Inflight_.size() <= 1) {
            UpdateAhead(*PersistedState_);
        }

        return TWatermarkGeneratorCookie(std::any(it));
    }

    void MarkPersisted(TWatermarkGeneratorCookie cookie) override
    {
        if (!cookie.Underlying().has_value()) {
            return;
        }

        auto it = std::any_cast<TInflightList::iterator>(cookie.Underlying());

        auto& persistedState = *PersistedState_;
        if (it == Inflight_.begin()) {
            persistedState.Max = std::max(persistedState.Max, it->CollapsedMax);
        } else {
            auto prevIt = std::prev(it);
            prevIt->CollapsedMax = std::max(prevIt->CollapsedMax, it->CollapsedMax);
        }
        Inflight_.erase(it);

        UpdateAhead(persistedState);
    }

    TSystemTimestamp GetPartitionReadWatermark(std::optional<TSystemTimestamp> sourceReadWatermark) const override
    {
        return GetPartitionWatermark(sourceReadWatermark, ReadState_.Get());
    }

    TSystemTimestamp GetPartitionPersistedWatermark(std::optional<TSystemTimestamp> sourcePersistedWatermark) const override
    {
        return GetPartitionWatermark(sourcePersistedWatermark, &*PersistedState_);
    }

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> Apply(THashMap<TStreamId, TInflightStreamTraverseDataPtr>&& inflights, const THashSet<TStreamId>& streamIds) override
    {
        for (const auto& streamId : streamIds) {
            auto inflight = GetOrCrash(inflights, streamId);
            inflight->MinEventTimestamp = GetPartitionPersistedWatermark(inflight->MinEventTimestamp);
        }
        return std::move(inflights);
    }

    void Init(IInitContextPtr initContext) override
    {
        initContext->InitClient<TWatermarkGeneratorState>(PersistedState_, "v0");
        ReadState_ = NYTree::CloneYsonStruct(MakeStrong(&*PersistedState_));
        ReadState_->MinAhead = {};
        ReadState_->MaxAhead = {};
    }

private:
    struct TBatchInfo
    {
        TSystemTimestamp MinEventTimestamp = InfinitySystemTimestamp;
        TSystemTimestamp MaxEventTimestamp = ZeroSystemTimestamp;
        // Accumulates MaxEventTimestamp from out-of-order (non-front) MarkPersisted calls
        // on later entries that were collapsed into this entry. Applied to PersistedState_->Max
        // when this entry reaches the front and is persisted.
        TSystemTimestamp CollapsedMax = ZeroSystemTimestamp;
    };

    using TInflightList = std::list<TBatchInfo>;

    const TWatermarkGeneratorSpecPtr Spec_;
    const NProfiling::TProfiler Profiler_;
    const NLogging::TLogger Logger;

    TInflightList Inflight_;

    TMutableStateClient<TWatermarkGeneratorState> PersistedState_;
    TWatermarkGeneratorStatePtr ReadState_;

private:
    TSystemTimestamp GetPartitionWatermark(const std::optional<TSystemTimestamp>& sourceWatermark, const TWatermarkGeneratorState* state) const
    {
        if (Spec_ && Spec_->UseSourceWatermark) {
            return sourceWatermark.value_or(ZeroSystemTimestamp);
        }

        const ui64 delay = Spec_ ? Spec_->OutOfOrdernessBound.Seconds() : 0ull;
        auto watermark = TSystemTimestamp(std::max<ui64>(state->Max.Underlying(), delay) - delay);
        if (state->MinAhead.has_value() && state->MaxAhead.has_value()) {
            auto aheadMaxWatermark = TSystemTimestamp(std::max<ui64>(state->MaxAhead->Underlying(), delay) - delay);
            return std::max(watermark, std::min(*state->MinAhead, aheadMaxWatermark));
        }
        if (sourceWatermark) {
            return std::min(*sourceWatermark, watermark);
        }
        return watermark;
    }

    void UpdateAhead(TWatermarkGeneratorState& persistedState)
    {
        if (Inflight_.empty()) {
            persistedState.MinAhead = {};
            persistedState.MaxAhead = {};
        } else {
            const auto& front = Inflight_.front();
            persistedState.MinAhead = front.MinEventTimestamp;
            persistedState.MaxAhead = front.MaxEventTimestamp;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IWatermarkGeneratorPtr CreateWatermarkGenerator(
    TWatermarkGeneratorSpecPtr spec,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
{
    return New<TWatermarkGenerator>(std::move(spec), std::move(profiler), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
