#pragma once

#ifndef CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include cache.h"
#endif

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TExpiringCache<TKey, TValue>::TExpiringCache(
    TCallback<TValue(TKey)> calculateValueAction,
    TDuration expirationTimeout,
    IInvokerPtr invoker)
    : CalculateValueAction_(std::move(calculateValueAction))
    , ExpirationTimeout_(expirationTimeout)
    , PeriodicDeleter_(New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TExpiringCache::DeleteExpiredItems, MakeWeak(this)),
        expirationTimeout,
        NConcurrency::EPeriodicExecutorMode::Automatic))
{ }

template <class TKey, class TValue>
TValue TExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();

    {
        NConcurrency::TReaderGuard guard(StoreLock_);

        auto it = Store_.find(key);
        if (it != Store_.end()) {
            auto& entry = it->second;
            if (now <= entry.LastUpdateTime + NProfiling::DurationToCpuDuration(ExpirationTimeout_)) {
                entry.LastAccessTime = now;
                return entry.Value;
            }
        }
    }

    auto result = CalculateValueAction_.Run(key);

    {
        NConcurrency::TWriterGuard guard(StoreLock_);

        auto it = Store_.find(key);
        if (it != Store_.end()) {
            it->second = TEntry({now, now, std::move(result)});
        } else {
            auto emplaceResult = Store_.emplace(key, TEntry({now, now, std::move(result)}));
            YCHECK(emplaceResult.second);
            it = emplaceResult.first;
        }

        return it->second.Value;
    }
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::ForceUpdate()
{
    auto now = NProfiling::GetCpuInstant();

    std::vector<TKey> keys;
    {
        NConcurrency::TReaderGuard guard(StoreLock_);
        keys.reserve(Store_.size());
        for (const auto& pair : Store_) {
            keys.push_back(pair.first);
        }
    }

    std::vector<TValue> values;
    values.reserve(keys.size());
    for (const auto& key : keys) {
        values.emplace_back(CalculateValueAction_.Run(key));
    }

    {
        NConcurrency::TWriterGuard guard(StoreLock_);

        for (size_t index = 0; index < keys.size(); ++index) {
            const auto& key = keys[index];
            auto& value = values[index];

            auto it = Store_.find(key);
            if (it != Store_.end()) {
                it->second = TEntry({now, now, std::move(value)});
            } else {
                auto emplaceResult = Store_.emplace(key, TEntry({now, now, std::move(value)}));
                YCHECK(emplaceResult.second);
            }
        }
    }
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::Start()
{
    PeriodicDeleter_->Start();
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::Stop()
{
    PeriodicDeleter_->Stop();

    {
        NConcurrency::TWriterGuard guard(StoreLock_);
        Store_.clear();
    }
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::DeleteExpiredItems()
{
    auto deadline = NProfiling::GetCpuInstant() - NProfiling::DurationToCpuDuration(ExpirationTimeout_);
    std::vector<TKey> toRemove;

    {
        NConcurrency::TReaderGuard guard(StoreLock_);

        for (const auto& pair : Store_) {
            if (pair.second.LastAccessTime < deadline) {
                toRemove.push_back(pair.first);
            }
        }
    }
    if (!toRemove.empty()) {
        NConcurrency::TWriterGuard guard(StoreLock_);

        for (const auto& key : toRemove) {
            auto it = Store_.find(key);
            if (it->second.LastAccessTime < deadline) {
                Store_.erase(it);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
