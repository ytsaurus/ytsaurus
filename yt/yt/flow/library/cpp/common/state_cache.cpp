#include "state_cache.h"

#include "spec.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TStateCacheNamespace::TStateCacheNamespace(TJobId jobId, std::string name)
    : JobId(jobId)
    , Name(std::move(name))
    , Hash(CombineHashes(THash<TJobId>()(JobId), THash<std::string>()(Name)))
{ }

TStateCacheKey::TStateCacheKey()
    : TStateCacheKey(TJobId(), std::nullopt, std::string())
{ }

TStateCacheKey::TStateCacheKey(TJobId jobId, std::optional<TKey> key, std::string name)
    : TStateCacheKey(New<TStateCacheNamespace>(jobId, std::move(name)), std::move(key))
{ }

TStateCacheKey::TStateCacheKey(TStateCacheNamespacePtr cacheNamespace, std::optional<TKey> key)
    : Namespace(std::move(cacheNamespace))
    , Key(std::move(key))
{
    YT_VERIFY(Namespace);
}

bool TStateCacheKey::operator==(const TStateCacheKey& other) const
{
    // Independently created job wrappers must still address the same entries.
    return (Namespace == other.Namespace ||
        (Namespace && other.Namespace && Namespace->Hash == other.Namespace->Hash &&
            Namespace->JobId == other.Namespace->JobId && Namespace->Name == other.Namespace->Name)) &&
        Key == other.Key;
}

void FormatValue(TStringBuilderBase* builder, const TStateCacheKey& key, TStringBuf /*spec*/)
{
    Format(builder, "{%v, %v, %v}", key.Namespace->JobId, key.Key, key.Namespace->Name);
}

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
    // Keep the logical admission charge independent of the physical key layout.
    return sizeof(TJobId) + (key.Key ? key.Key->Underlying().GetSpaceUsed() : 0) + key.Namespace->Name.size();
}

////////////////////////////////////////////////////////////////////////////////

TJobStateCache::TMetrics::TMetrics(NProfiling::TProfiler profiler)
    : InsertCounter(profiler.Counter("/insert"))
    , InsertBytesCounter(profiler.Counter("/insert_bytes"))
    , ExtractHitCounter(profiler.Counter("/extract_hit"))
    , ExtractHitBytesCounter(profiler.Counter("/extract_hit_bytes"))
    , ExtractMissCounter(profiler.Counter("/extract_miss"))
    , TimeToCompress(profiler.WithSparse().Timer("/time_to_compress"))
    , TimeToExpire(profiler.WithSparse().Timer("/time_to_expire"))
{
    profiler.AddFuncGauge("/count", MakeStrong(this), [this] {
        return Count.load();
    });
    profiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return Size.load();
    });
}

TJobStateCache::TTrackedItem::TTrackedItem(TStateCacheKey key, TMetricsPtr metrics, i64 size, IStateCacheValuePtr value)
    : TItem(std::move(key), std::move(value))
    , Metrics_(std::move(metrics))
    , Size_(size)
{
    Metrics_->Count.fetch_add(1);
    Metrics_->Size.fetch_add(Size_);
}

void TJobStateCache::TTrackedItem::Compress()
{
    TItem::Compress();
    Metrics_->TimeToCompress.Record(TInstant::Now() - InsertTimestamp);
}

void TJobStateCache::TTrackedItem::MarkExtracted() noexcept
{
    Extracted_ = true;
}

TJobStateCache::TTrackedItem::~TTrackedItem()
{
    Metrics_->Count.fetch_sub(1);
    Metrics_->Size.fetch_sub(Size_);
    if (!Extracted_) {
        Metrics_->TimeToExpire.Record(TInstant::Now() - InsertTimestamp);
    }
}

TJobStateCache::TJobStateCache(TJobId jobId, TStateCachePtr stateCache, NProfiling::TProfiler profiler)
    : JobId_(jobId)
    , StateCache_(std::move(stateCache))
    , Profiler_(std::move(profiler))
{ }

void TJobStateCache::Insert(const TJobStateCacheKey& key, IStateCacheValuePtr state)
{
    Insert(key.first, GetContext(key.second), std::move(state));
}

void TJobStateCache::Insert(const std::optional<TKey>& key, const TNamedContext& context, IStateCacheValuePtr state)
{
    const auto& metrics = context.Metrics;
    const i64 size = state->GetWeight();
    metrics->InsertCounter.Increment();
    metrics->InsertBytesCounter.Increment(size);
    StateCache_->InsertItem(New<TTrackedItem>(
        TStateCacheKey(context.Namespace, key),
        metrics,
        size,
        std::move(state)));
}

IStateCacheValuePtr TJobStateCache::Extract(const TJobStateCacheKey& key)
{
    return Extract(key.first, GetContext(key.second));
}

IStateCacheValuePtr TJobStateCache::Extract(const std::optional<TKey>& key, const TNamedContext& context)
{
    auto item = StateCache_->ExtractItem(TStateCacheKey(context.Namespace, key));
    const auto& metrics = context.Metrics;
    if (item) {
        metrics->ExtractHitCounter.Increment();
        metrics->ExtractHitBytesCounter.Increment(item->Value->GetWeight());
        item->MarkExtracted();
        return item->Value;
    }
    metrics->ExtractMissCounter.Increment();
    return nullptr;
}

TJobNamedStateCachePtr TJobStateCache::WithName(const std::string& name)
{
    return New<TJobNamedStateCache>(MakeStrong(this), name);
}

const TJobStateCache::TNamedContext& TJobStateCache::GetContext(const std::string& name)
{
    return GetOrInsert(Contexts_, name, [&] {
        return TNamedContext{
            New<TStateCacheNamespace>(JobId_, name),
            New<TMetrics>(Profiler_.WithTag("name", name)),
        };
    });
}

NProfiling::TProfiler TJobStateCache::GetProfiler() const
{
    return Profiler_;
}

////////////////////////////////////////////////////////////////////////////////

TJobNamedStateCache::TJobNamedStateCache(TJobStateCachePtr jobStateCache, std::string name)
    : StateCache_(std::move(jobStateCache))
    , Context_(StateCache_->GetContext(name))
{ }

void TJobNamedStateCache::Insert(const std::optional<TKey>& key, IStateCacheValuePtr value)
{
    StateCache_->Insert(key, Context_, std::move(value));
}

IStateCacheValuePtr TJobNamedStateCache::Extract(const std::optional<TKey>& key)
{
    return StateCache_->Extract(key, Context_);
}

NProfiling::TProfiler TJobNamedStateCache::GetProfiler() const
{
    return StateCache_->GetProfiler().WithTag("name", Context_.Namespace->Name);
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

    if (DynamicSpec_->Ttl == TDuration::Zero()) {
        return std::nullopt;
    }

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
