#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NFlow::NCache {

////////////////////////////////////////////////////////////////////////////////

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
    };

    using TItemPtr = TIntrusivePtr<TItem>;

    class TCache
        : public TAsyncSlruCacheBase<TKey, TItem>
    {
    public:
        TCache(
            TSlruCacheConfigPtr config,
            TIntrusivePtr<TCache> nextCache,
            TWeakPtr<TTwoLevelCache> owner,
            NProfiling::TProfiler profiler);

        i64 GetWeight(const TItemPtr& item) const override;

        void OnRemoved(const TItemPtr& item) override;

        bool IsResurrectionSupported() const override;

    private:
        TIntrusivePtr<TCache> NextCache_;
        const TWeakPtr<TTwoLevelCache> Owner_;
        NProfiling::TEventTimer TimeToExpire_;
    };

    using TCachePtr = TIntrusivePtr<TCache>;

    TTwoLevelCache(NProfiling::TProfiler profiler);

    virtual ~TTwoLevelCache() = default;

    void Reconfigure(i64 capacity, i64 compressedCapacity);

    void Insert(const TKey& key, TCompressibleValuePtr value);

    // Removes value from cache and returns it.
    // Expects no concurrent access to one key.
    TCompressibleValuePtr Extract(const TKey& key);

protected:
    // Override to account for key memory in the cache budget.
    virtual i64 GetKeyWeight(const TKey& key) const = 0;

private:
    TCachePtr CompressedCache_;
    TCachePtr Cache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCache

#define TWO_LEVEL_CACHE_INL_H_
#include "two_level_cache-inl.h"
#undef TWO_LEVEL_CACHE_INL_H_
