#pragma once

#ifndef TWO_LEVEL_CACHE_INL_H_
    #error "Direct inclusion of this file is not allowed, include two_level_cache.h"
    // For the sake of sane code completion.
    #include "two_level_cache.h"
#endif

namespace NYT::NFlow::NCache {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TCompressibleValue>
TTwoLevelCache<TKey, TCompressibleValue>::TItem::TItem(TKey key, TCompressibleValuePtr value)
    : TAsyncCacheValueBase<TKey, TItem>(std::move(key))
    , Value(std::move(value))
    , InsertTimestamp(TInstant::Now())
{ }

template <class TKey, class TCompressibleValue>
TTwoLevelCache<TKey, TCompressibleValue>::TCache::TCache(
    TSlruCacheConfigPtr config,
    TIntrusivePtr<TCache> nextCache,
    TWeakPtr<TTwoLevelCache> owner,
    NProfiling::TProfiler profiler)
    : TAsyncSlruCacheBase<TKey, TItem>(std::move(config), profiler)
    , NextCache_(std::move(nextCache))
    , Owner_(std::move(owner))
    , TimeToExpire_(profiler.WithSparse().Timer("/time_to_expire"))
{ }

template <class TKey, class TCompressibleValue>
i64 TTwoLevelCache<TKey, TCompressibleValue>::TCache::GetWeight(const TItemPtr& item) const
{
    i64 keyWeight = 0;
    if (auto owner = Owner_.Lock()) {
        keyWeight = owner->GetKeyWeight(item->GetKey());
    }
    return item->Value->GetWeight() + keyWeight;
}

template <class TKey, class TCompressibleValue>
void TTwoLevelCache<TKey, TCompressibleValue>::TCache::OnRemoved(const TItemPtr& item)
{
    if (NextCache_ && item->AllowCompression.exchange(false) == true) {
        item->Value->Compress();
        auto cookie = NextCache_->BeginInsert(item->GetKey());
        cookie.EndInsert(item);
        NextCache_->Touch(item);
    }
    TimeToExpire_.Record(TInstant::Now() - item->InsertTimestamp);
}

template <class TKey, class TCompressibleValue>
bool TTwoLevelCache<TKey, TCompressibleValue>::TCache::IsResurrectionSupported() const
{
    return false;
}

template <class TKey, class TCompressibleValue>
TTwoLevelCache<TKey, TCompressibleValue>::TTwoLevelCache(NProfiling::TProfiler profiler)
    : CompressedCache_(New<TCache>(New<TSlruCacheConfig>(), nullptr, MakeWeak(this), profiler.WithTag("cache", "compressed")))
    , Cache_(New<TCache>(New<TSlruCacheConfig>(), CompressedCache_, MakeWeak(this), profiler.WithTag("cache", "uncompressed")))
{ }

template <class TKey, class TCompressibleValue>
void TTwoLevelCache<TKey, TCompressibleValue>::Reconfigure(i64 capacity, i64 compressedCapacity)
{
    {
        auto cacheConfig = New<TSlruCacheDynamicConfig>();
        cacheConfig->Capacity = capacity;
        Cache_->Reconfigure(cacheConfig);
    }
    {
        auto cacheConfig = New<TSlruCacheDynamicConfig>();
        cacheConfig->Capacity = compressedCapacity;
        CompressedCache_->Reconfigure(cacheConfig);
    }
}

template <class TKey, class TCompressibleValue>
void TTwoLevelCache<TKey, TCompressibleValue>::Insert(const TKey& key, TCompressibleValuePtr value)
{
    YT_VERIFY(value);
    auto cookie = Cache_->BeginInsert(key);
    auto item = New<TItem>(key, std::move(value));
    cookie.EndInsert(item);
    Cache_->Touch(item);
}

// Removes value from cache and returns it.
// Expects no concurrent access to one key.
template <class TKey, class TCompressibleValue>
TIntrusivePtr<TCompressibleValue> TTwoLevelCache<TKey, TCompressibleValue>::Extract(const TKey& key)
{
    if (auto item = Cache_->Find(key)) {
        auto value = item->Value;
        bool mayCompress = item->AllowCompression.exchange(false);
        Cache_->TryRemove(key);
        CompressedCache_->TryRemove(key);
        if (!mayCompress) {
            value->Decompress();
        }
        return value;
    } else if (auto item = CompressedCache_->Find(key)) {
        auto value = item->Value;
        bool mayCompress = item->AllowCompression.exchange(false);
        YT_VERIFY(!mayCompress);
        CompressedCache_->TryRemove(key);
        value->Decompress();
        return value;
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCache
