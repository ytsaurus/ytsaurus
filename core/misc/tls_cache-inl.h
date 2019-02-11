#pragma once
#ifndef TLS_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include tls_cache.h"
// For the sake of sane code completion.
#include "tls_cache.h"
#endif

#include <util/generic/hash.h>
#include <util/system/spinlock.h>
#include <util/thread/singleton.h>

#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename TTrait>
class TCache
{
public:
    template <typename... U>
    typename TTrait::TValue& Find(U&&... u)
    {
        auto&& key = TTrait::ToKey(std::forward<U>(u)...);
        auto it = Values_.find(key);
        if (it != Values_.end()) {
            return it->second;
        }
        return Values_.emplace(key, TTrait::ToValue(std::forward<U>(u)...)).first->second;
    }

private:
    THashMap<typename TTrait::TKey, typename TTrait::TValue> Values_;
};

template <typename TBaseTrait>
struct TSynchronizedTrait
    : public TBaseTrait
{
    using TBaseValue = typename TBaseTrait::TValue;
    using TValue = std::shared_ptr<TBaseValue>;

    template <typename... U>
    static TValue ToValue(U&&... u)
    {
        return std::make_shared<TBaseValue>(TBaseTrait::ToValue(std::forward<U>(u)...));
    }
};

template <typename TTrait>
class TSynchronizedCache
{
public:
    template <typename... U>
    typename TTrait::TValue& Find(U&&... u)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return *Cache_.Find(std::forward<U>(u)...);
    }

private:
    TCache<TSynchronizedTrait<TTrait>> Cache_;
    TSpinLock SpinLock_;
};

template <typename TBaseTrait>
struct TSingletonTrait
    : public TBaseTrait
{
    using TValue = typename TBaseTrait::TValue*;

    template <typename... U>
    static TValue ToValue(U&&... u)
    {
        return &GetGloballyCachedValue<TBaseTrait>(std::forward<U>(u)...);
    }
};

template <typename TTrait>
using TSingletonCache = TCache<TSingletonTrait<TTrait>>;

} // namespace NDetail

template <typename TTrait, typename... U>
typename TTrait::TValue& GetGloballyCachedValue(U&&... u)
{
    static NDetail::TSynchronizedCache<TTrait> cache;
    return cache.Find(std::forward<U>(u)...);
};

template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyCachedValue(U&&... u)
{
    return FastTlsSingleton<NDetail::TCache<TTrait>>()->Find(std::forward<U>(u)...);
}

template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyGloballyCachedValue(U&&... u)
{
    return *FastTlsSingleton<NDetail::TSingletonCache<TTrait>>()->Find(std::forward<U>(u)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
