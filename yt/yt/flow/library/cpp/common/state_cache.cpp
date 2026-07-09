#include "state_cache.h"

#include "spec.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TStateCache::TStateCache(TDynamicStateCacheSpecPtr dynamicSpec, NProfiling::TProfiler profiler)
    : NCache::TTwoLevelCache<TStateCacheKey, IStateCacheValue>(profiler)
{
    Reconfigure(std::move(dynamicSpec));
}

void TStateCache::Reconfigure(TDynamicStateCacheSpecPtr dynamicSpec)
{
    Reconfigure(dynamicSpec->UncompressedCacheWeight, dynamicSpec->CompressedCacheWeight);
}

TJobStateCachePtr TStateCache::WithJob(TJobId jobId, NProfiling::TProfiler jobProfiler)
{
    return New<TJobStateCache>(jobId, MakeStrong(this), jobProfiler);
}

i64 TStateCache::GetKeyWeight(const TStateCacheKey& key) const
{
    const auto& [jobId, maybeKey, name] = key;
    Y_UNUSED(jobId);
    return sizeof(TJobId) + (maybeKey ? maybeKey->Underlying().GetSpaceUsed() : 0) + name.size();
}

////////////////////////////////////////////////////////////////////////////////

TJobStateCache::TMetrics::TMetrics(NProfiling::TProfiler profiler)
    : InsertCounter(profiler.Counter("/insert"))
    , InsertBytesCounter(profiler.Counter("/insert_bytes"))
    , ExtractHitCounter(profiler.Counter("/extract_hit"))
    , ExtractHitBytesCounter(profiler.Counter("/extract_hit_bytes"))
    , ExtractMissCounter(profiler.Counter("/extract_miss"))
    , CountGauge(profiler.Gauge("/count"))
    , SizeGauge(profiler.Gauge("/size"))
    , TimeToCompress(profiler.WithSparse().Timer("/time_to_compress"))
    , TimeToExpire(profiler.WithSparse().Timer("/time_to_expire"))
{ }

TJobStateCache::TTrackedValue::TTrackedValue(TMetricsPtr metrics, i64 size, IStateCacheValuePtr value)
    : Metrics_(std::move(metrics))
    , Size_(size)
    , InsertTimestamp_(TInstant::Now())
    , Value_(std::move(value))
{ }

void TJobStateCache::TTrackedValue::Compress()
{
    Value_->Compress();
    if (Metrics_ && Value_ && InsertTimestamp_) {
        Metrics_->TimeToCompress.Record(TInstant::Now() - *InsertTimestamp_);
    }
}

void TJobStateCache::TTrackedValue::Decompress()
{
    Value_->Decompress();
}

i64 TJobStateCache::TTrackedValue::GetWeight()
{
    return Value_->GetWeight();
}

IStateCacheValuePtr TJobStateCache::TTrackedValue::Get()
{
    return Value_;
}

void TJobStateCache::TTrackedValue::MarkExtracted()
{
    InsertTimestamp_ = {};
}

TJobStateCache::TTrackedValue::~TTrackedValue()
{
    if (Metrics_ && Value_) {
        Metrics_->CountGauge.Update(Metrics_->Count.fetch_sub(1));
        Metrics_->SizeGauge.Update(Metrics_->Size.fetch_sub(Size_));
        if (InsertTimestamp_) {
            Metrics_->TimeToExpire.Record(TInstant::Now() - *InsertTimestamp_);
        }
    }
}

TJobStateCache::TJobStateCache(TJobId jobId, TStateCachePtr stateCache, NProfiling::TProfiler profiler)
    : JobId_(jobId)
    , StateCache_(std::move(stateCache))
    , Profiler_(std::move(profiler))
{ }

void TJobStateCache::Insert(const TJobStateCacheKey& key, IStateCacheValuePtr state)
{
    auto metrics = GetMetrics(key.second);
    const i64 size = state->GetWeight();
    metrics->InsertCounter.Increment();
    metrics->InsertBytesCounter.Increment(size);
    metrics->CountGauge.Update(metrics->Count.fetch_add(1));
    metrics->SizeGauge.Update(metrics->Size.fetch_add(size));
    StateCache_->Insert(TStateCacheKey(JobId_, key.first, key.second), New<TTrackedValue>(std::move(metrics), size, std::move(state)));
}

IStateCacheValuePtr TJobStateCache::Extract(const TJobStateCacheKey& key)
{
    auto state = StateCache_->Extract(TStateCacheKey(JobId_, key.first, key.second));
    auto metrics = GetMetrics(key.second);
    if (state) {
        metrics->ExtractHitCounter.Increment();
        metrics->ExtractHitBytesCounter.Increment(state->GetWeight());
        auto trackedState = DynamicPointerCast<TTrackedValue>(state);
        YT_VERIFY(trackedState);
        trackedState->MarkExtracted();
        return trackedState->Get();
    }
    metrics->ExtractMissCounter.Increment();
    return nullptr;
}

TJobNamedStateCachePtr TJobStateCache::WithName(const std::string& name)
{
    return New<TJobNamedStateCache>(MakeStrong(this), name);
}

TJobStateCache::TMetricsPtr TJobStateCache::GetMetrics(const std::string& name)
{
    return GetOrInsert(Metrics_, name, [&] {
        return New<TMetrics>(Profiler_.WithTag("name", name));
    });
}

NProfiling::TProfiler TJobStateCache::GetProfiler() const
{
    return Profiler_;
}

////////////////////////////////////////////////////////////////////////////////

TJobNamedStateCache::TJobNamedStateCache(TJobStateCachePtr jobStateCache, std::string name)
    : StateCache_(std::move(jobStateCache))
    , Name_(std::move(name))
{ }

void TJobNamedStateCache::Insert(const std::optional<TKey>& key, IStateCacheValuePtr value)
{
    StateCache_->Insert({key, Name_}, std::move(value));
}

IStateCacheValuePtr TJobNamedStateCache::Extract(const std::optional<TKey>& key)
{
    return StateCache_->Extract({key, Name_});
}

NProfiling::TProfiler TJobNamedStateCache::GetProfiler() const
{
    return StateCache_->GetProfiler().WithTag("name", Name_);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicExpiringJobNamedStateCacheSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("ttl", &TThis::Ttl)
        .Default(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

TExpiringJobNamedStateCache::TExpiringCacheValue::TExpiringCacheValue(IStateCacheValuePtr value, TInstant invalidationTime, TExpirationMetricsPtr metrics)
    : Value(value)
    , InvalidationTime(invalidationTime)
    , Metrics(metrics)
{ }

void TExpiringJobNamedStateCache::TExpiringCacheValue::Compress()
{
    Value->Compress();
}

void TExpiringJobNamedStateCache::TExpiringCacheValue::Decompress()
{
    Value->Decompress();
}

i64 TExpiringJobNamedStateCache::TExpiringCacheValue::GetWeight()
{
    return Value->GetWeight();
}

TExpiringJobNamedStateCache::TExpiringCacheValue::~TExpiringCacheValue()
{
    if (Metrics) {
        Metrics->InCacheAfterExpired.Record(TInstant::Now() - InvalidationTime);
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpiringJobNamedStateCache::TExpirationMetrics::TExpirationMetrics(NProfiling::TProfiler profiler)
    : ExtractedNotExpiredCounter(profiler.Counter("/extract_hit_not_expired"))
    , ExtractedNotExpiredBytesCounter(profiler.Counter("/extract_hit_not_expired_bytes"))
    , ExtractedExpiredCounter(profiler.Counter("/extract_hit_expired"))
    , ExtractedExpiredBytesCounter(profiler.Counter("/extract_hit_expired_bytes"))
    , InCacheAfterExpired(profiler.WithSparse().Timer("/in_cache_after_expired"))
{ }

TExpiringJobNamedStateCache::TExpiringJobNamedStateCache(TJobNamedStateCachePtr stateCache, TDynamicExpiringJobNamedStateCacheSpecPtr dynamicSpec)
    : StateCache_(std::move(stateCache))
    , DynamicSpec_(std::move(dynamicSpec))
    , Metrics_(New<TExpirationMetrics>(StateCache_->GetProfiler()))
    , SerializedInvoker_(GetCurrentInvoker())
{
    YT_VERIFY(SerializedInvoker_->IsSerialized());
}

void TExpiringJobNamedStateCache::Insert(const std::optional<TKey>& key, IStateCacheValuePtr value, const std::optional<TCookie>& cookie)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    if (DynamicSpec_->Ttl != TDuration::Zero()) {
        TExpiringCacheValuePtr expiringValue = New<TExpiringCacheValue>(std::move(value), cookie.value_or(TInstant::Now() + DynamicSpec_->Ttl), Metrics_);
        if (TInstant::Now() < expiringValue->InvalidationTime) {
            StateCache_->Insert(key, std::move(expiringValue));
        }
    }
}

std::optional<std::pair<IStateCacheValuePtr, TExpiringJobNamedStateCache::TCookie>> TExpiringJobNamedStateCache::Extract(const std::optional<TKey>& key)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    auto result = DynamicPointerCast<TExpiringCacheValue>(StateCache_->Extract(key));
    if (!result) {
        return std::nullopt;
    }
    if (TInstant::Now() >= result->InvalidationTime) {
        Metrics_->ExtractedExpiredCounter.Increment();
        Metrics_->ExtractedExpiredBytesCounter.Increment(result->GetWeight());
        return std::nullopt;
    }
    Metrics_->ExtractedNotExpiredCounter.Increment();
    Metrics_->ExtractedNotExpiredBytesCounter.Increment(result->GetWeight());
    return std::pair<IStateCacheValuePtr, TCookie>{result->Value, result->InvalidationTime};
}

void TExpiringJobNamedStateCache::Reconfigure(TDynamicExpiringJobNamedStateCacheSpecPtr dynamicSpec)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

    DynamicSpec_ = std::move(dynamicSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
