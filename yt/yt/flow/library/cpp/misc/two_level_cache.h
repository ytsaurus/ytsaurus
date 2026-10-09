#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NFlow::NCache {

////////////////////////////////////////////////////////////////////////////////

// Exceptions from eviction callbacks are fatal. This includes #TCompressibleValue::Compress(),
// #TCompressibleValue::GetWeight(), and hashing, copying or comparing #TKey.
// Other cache operations may propagate exceptions to the caller.
//
// #TCompressibleValue::Compress() runs under cache locks; heavy storage detached
// during compression must be handed over to the current destruction context
// (see #NYT::NFlow::TDestructionContextGuard) instead of being destroyed in place.
template <class TKey, class TCompressibleValue>
class TTwoLevelCache
    : public TRefCounted
{
public:
    using TCompressibleValuePtr = TIntrusivePtr<TCompressibleValue>;

    struct TItem
        : public TAsyncCacheValueBase<TKey, TItem>
    {
        TItem(TKey key, TCompressibleValuePtr value);

        TCompressibleValuePtr Value;
        std::atomic<bool> AllowCompression = true;
        TInstant InsertTimestamp;
        i64 CompressedWeight = 0;
    };

    using TItemPtr = TIntrusivePtr<TItem>;

    class TCompressedCache
        : public TSyncSlruCacheBase<TKey, TItem>
    {
    public:
        TCompressedCache(
            TSlruCacheConfigPtr config,
            TWeakPtr<TTwoLevelCache> owner,
            NProfiling::TProfiler profiler);

        void Insert(const TItemPtr& item);
        TItemPtr Find(const TKey& key);

        i64 GetWeight(const TItemPtr& item) const override;
        void OnRemoved(const TItemPtr& item) noexcept override;
        void OnTotalWeightUpdated(i64 weightDelta) override;

    private:
        const TWeakPtr<TTwoLevelCache> Owner_;
        NProfiling::TCounter HitCounter_;
        NProfiling::TCounter HitWeightCounter_;
        NProfiling::TCounter MissedCounter_;
        NProfiling::TCounter MissedWeightCounter_;
        NProfiling::TEventTimer TimeToExpire_;
        std::atomic<i64> Weight_ = 0;
    };

    using TCompressedCachePtr = TIntrusivePtr<TCompressedCache>;

    class TCache
        : public TAsyncSlruCacheBase<TKey, TItem>
    {
    public:
        TCache(
            TSlruCacheConfigPtr config,
            TCompressedCachePtr nextCache,
            TWeakPtr<TTwoLevelCache> owner,
            NProfiling::TProfiler profiler);

        i64 GetWeight(const TItemPtr& item) const override;

        void OnRemoved(const TItemPtr& item) noexcept override;

        bool IsResurrectionSupported() const override;

    private:
        const TCompressedCachePtr NextCache_;
        const TWeakPtr<TTwoLevelCache> Owner_;
        NProfiling::TEventTimer TimeToExpire_;
    };

    using TCachePtr = TIntrusivePtr<TCache>;

    TTwoLevelCache(NProfiling::TProfiler profiler);

    ~TTwoLevelCache() override = default;

    void Reconfigure(i64 capacity, i64 compressedCapacity);

    void Insert(const TKey& key, TCompressibleValuePtr value);

    // Removes value from cache and returns it.
    // Expects no concurrent access to one key.
    TCompressibleValuePtr Extract(const TKey& key);

protected:
    // Override to account for key memory in the cache budget.
    virtual i64 GetKeyWeight(const TKey& key) const = 0;

private:
    TCompressedCachePtr CompressedCache_;
    TCachePtr Cache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCache

#define TWO_LEVEL_CACHE_INL_H_
#include "two_level_cache-inl.h"
#undef TWO_LEVEL_CACHE_INL_H_
