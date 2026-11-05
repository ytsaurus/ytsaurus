#pragma once

#include "public.h"

#include "key.h"

#include <yt/yt/flow/library/cpp/misc/two_level_cache.h>

#include <library/cpp/yt/system/cache_line_size.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Keep one namespace's immutable hash off another namespace's refcount cache line.
struct alignas(CacheLineSize) TStateCacheNamespace
    : public TRefCounted
{
    const TJobId JobId;
    const std::string Name;
    const size_t Hash;

    TStateCacheNamespace(TJobId jobId, std::string name);
};

DEFINE_REFCOUNTED_TYPE(TStateCacheNamespace);

struct TStateCacheKey
{
    TStateCacheNamespacePtr Namespace;
    std::optional<TKey> Key;

    TStateCacheKey();
    TStateCacheKey(TJobId jobId, std::optional<TKey> key, std::string name);
    TStateCacheKey(TStateCacheNamespacePtr cacheNamespace, std::optional<TKey> key);

    bool operator==(const TStateCacheKey& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TStateCacheKey& key, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

template <>
struct THash<NYT::NFlow::TStateCacheKey>
{
    size_t operator()(const NYT::NFlow::TStateCacheKey& key) const;
};

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IStateCacheValue
    : public TRefCounted
{
    // Compresses the value; heavy storage detached during compression must be handed
    // over to the current destruction context (see #NYT::NFlow::TDestructionContextGuard).
    virtual void Compress() = 0;
    virtual void Decompress() = 0;
    virtual i64 GetWeight() = 0;
};

using IStateCacheValuePtr = TIntrusivePtr<IStateCacheValue>;

////////////////////////////////////////////////////////////////////////////////

class TStateCache
    : public NCache::TTwoLevelCache<TStateCacheKey, IStateCacheValue>
{
public:
    TStateCache(TDynamicStateCacheSpecPtr dynamicSpec, NProfiling::TProfiler profiler);
    using NCache::TTwoLevelCache<TStateCacheKey, IStateCacheValue>::Reconfigure;

    void Reconfigure(TDynamicStateCacheSpecPtr dynamicSpec);

    TJobStateCachePtr WithJob(TJobId jobId, NProfiling::TProfiler jobProfiler);

protected:
    i64 GetKeyWeight(const TStateCacheKey& key) const override;

private:
    friend class TJobStateCache;

    using NCache::TTwoLevelCache<TStateCacheKey, IStateCacheValue>::InsertItem;
    using NCache::TTwoLevelCache<TStateCacheKey, IStateCacheValue>::ExtractItem;
};

DEFINE_REFCOUNTED_TYPE(TStateCache);

////////////////////////////////////////////////////////////////////////////////

class TJobStateCache
    : public TRefCounted
{
public:
    struct TMetrics
        : public TRefCounted
    {
        explicit TMetrics(NProfiling::TProfiler profiler);

        NProfiling::TCounter InsertCounter;
        NProfiling::TCounter InsertBytesCounter;
        NProfiling::TCounter ExtractHitCounter;
        NProfiling::TCounter ExtractHitBytesCounter;
        NProfiling::TCounter ExtractMissCounter;
        std::atomic<i64> Count = 0;
        std::atomic<i64> Size = 0;
        NProfiling::TEventTimer TimeToCompress;
        NProfiling::TEventTimer TimeToExpire;
    };

    using TMetricsPtr = TIntrusivePtr<TMetrics>;

    class TTrackedItem
        : public TStateCache::TItem
    {
    public:
        TTrackedItem(TStateCacheKey key, TMetricsPtr metrics, i64 size, IStateCacheValuePtr value);

        void Compress() override;
        void MarkExtracted() noexcept override;

        ~TTrackedItem() override;

    private:
        TMetricsPtr Metrics_;
        i64 Size_;
        bool Extracted_ = false;
    };

    using TJobStateCacheKey = std::pair<std::optional<TKey>, std::string>;

    TJobStateCache(TJobId jobId, TStateCachePtr stateCache, NProfiling::TProfiler profiler);

    void Insert(const TJobStateCacheKey& key, IStateCacheValuePtr value);
    IStateCacheValuePtr Extract(const TJobStateCacheKey& key);
    TJobNamedStateCachePtr WithName(const std::string& name);
    NProfiling::TProfiler GetProfiler() const;

private:
    const TJobId JobId_;
    const TStateCachePtr StateCache_;
    const NProfiling::TProfiler Profiler_;

    friend class TJobNamedStateCache;

    struct TNamedContext
    {
        TStateCacheNamespacePtr Namespace;
        TMetricsPtr Metrics;
    };

    THashMap<std::string, TNamedContext> Contexts_;

private:
    const TNamedContext& GetContext(const std::string& name);
    void Insert(const std::optional<TKey>& key, const TNamedContext& context, IStateCacheValuePtr value);
    IStateCacheValuePtr Extract(const std::optional<TKey>& key, const TNamedContext& context);
};

DEFINE_REFCOUNTED_TYPE(TJobStateCache);

////////////////////////////////////////////////////////////////////////////////

class TJobNamedStateCache
    : public TRefCounted
{
public:
    TJobNamedStateCache(TJobStateCachePtr jobStateCache, std::string name);

    void Insert(const std::optional<TKey>& key, IStateCacheValuePtr value);
    IStateCacheValuePtr Extract(const std::optional<TKey>& key);
    NProfiling::TProfiler GetProfiler() const;

private:
    const TJobStateCachePtr StateCache_;
    const TJobStateCache::TNamedContext Context_;
};

DEFINE_REFCOUNTED_TYPE(TJobNamedStateCache);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicExpiringJobNamedStateCacheSpec
    : public NYTree::TYsonStruct
{
    TDuration Ttl;

    REGISTER_YSON_STRUCT(TDynamicExpiringJobNamedStateCacheSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicExpiringJobNamedStateCacheSpec);

class TExpiringJobNamedStateCache
    : public TRefCounted
{
public:
    struct TExpirationMetrics
        : public TRefCounted
    {
        explicit TExpirationMetrics(NProfiling::TProfiler profiler);

        NProfiling::TCounter ExtractedNotExpiredCounter;
        NProfiling::TCounter ExtractedNotExpiredBytesCounter;
        NProfiling::TCounter ExtractedExpiredCounter;
        NProfiling::TCounter ExtractedExpiredBytesCounter;
        NProfiling::TEventTimer InCacheAfterExpired;
    };

    using TExpirationMetricsPtr = TIntrusivePtr<TExpirationMetrics>;

    using TCookie = TInstant;

    TExpiringJobNamedStateCache(TJobNamedStateCachePtr stateCache, TDynamicExpiringJobNamedStateCacheSpecPtr dynamicSpec);

    void Insert(const std::optional<TKey>& key, IStateCacheValuePtr value, const std::optional<TCookie>& cookie);

    std::optional<std::pair<IStateCacheValuePtr, TCookie>> Extract(const std::optional<TKey>& key);

    void Reconfigure(TDynamicExpiringJobNamedStateCacheSpecPtr dynamicSpec);

private:
    struct TExpiringCacheValue
        : public IStateCacheValue
    {
        TExpiringCacheValue(IStateCacheValuePtr value, TInstant invalidationTime, TExpirationMetricsPtr metrics);

        void Compress() override;

        void Decompress() override;

        i64 GetWeight() override;

        ~TExpiringCacheValue() override;

        IStateCacheValuePtr Value;
        TInstant InvalidationTime;
        TExpirationMetricsPtr Metrics;
    };

    using TExpiringCacheValuePtr = TIntrusivePtr<TExpiringCacheValue>;

private:
    const TJobNamedStateCachePtr StateCache_;
    TDynamicExpiringJobNamedStateCacheSpecPtr DynamicSpec_;
    TExpirationMetricsPtr Metrics_;

    // Used to verify the same serialized invoker is being used consistently.
    IInvokerPtr SerializedInvoker_;
};

DEFINE_REFCOUNTED_TYPE(TExpiringJobNamedStateCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define STATE_CACHE_INL_H_
#include "state_cache-inl.h"
#undef STATE_CACHE_INL_H_
