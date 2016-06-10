#ifndef EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include expiring_cache.h"
#endif

#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TExpiringCache<TKey, TValue>::TExpiringCache(TExpiringCacheConfigPtr config)
    : Config_(std::move(config))
{ }

template <class TKey, class TValue>
TFuture<TValue> TExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    auto now = TInstant::Now();

    // Fast path.
    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            const auto& entry = it->second;
            if (now < entry.Deadline) {
                return entry.Promise;
            }
        }
    }

    // Slow path.
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
        auto it = Map_.find(key);
        if (it == Map_.end()) {
            TEntry entry;
            entry.Deadline = TInstant::Max();
            auto promise = entry.Promise = NewPromise<TValue>();
            YCHECK(Map_.insert(std::make_pair(key, entry)).second);
            guard.Release();
            InvokeGet(key);
            return promise;
        }

        auto& entry = it->second;
        const auto& promise = entry.Promise;

        if (promise.IsSet() && now > entry.Deadline) {
            // Evict and retry.
            NConcurrency::TDelayedExecutor::CancelAndClear(entry.ProbationCookie);
            entry.ProbationFuture.Cancel();
            entry.ProbationFuture.Reset();
            Map_.erase(it);
            guard.Release();
            return Get(key);
        }

        return promise;
    }
}

template <class TKey, class TValue>
TFuture<typename TExpiringCache<TKey, TValue>::TCombinedValue> TExpiringCache<TKey, TValue>::Get(const std::vector<TKey>& keys)
{
    auto now = TInstant::Now();

    std::vector<TFuture<TValue>> results(keys.size());
    std::vector<size_t> fetchIndexes;

    // Fast path.
    {
        NConcurrency::TReaderGuard guard(SpinLock_);

        for (size_t index = 0; index < keys.size(); ++index) {
            auto it = Map_.find(keys[index]);
            if (it != Map_.end()) {
                const auto& entry = it->second;
                if (now < entry.Deadline) {
                    results[index] = entry.Promise;
                    continue;
                }
            }
            fetchIndexes.push_back(index);
        }
    }

    // Slow path.
    if (!fetchIndexes.empty()) {
        std::vector<size_t> invokeIndexes;

        NConcurrency::TWriterGuard guard(SpinLock_);
        for (auto index : fetchIndexes) {
            const auto& key = keys[index];

            auto inserted = Map_.insert(std::make_pair(key, TEntry()));
            auto& entry = inserted.first->second;

            if (!inserted.second) {
                if (entry.Promise.IsSet() && now > entry.Deadline) {
                    NConcurrency::TDelayedExecutor::CancelAndClear(entry.ProbationCookie);
                } else {
                    results[index] = entry.Promise;
                    continue;
                }
            }

            entry.Deadline = TInstant::Max();
            entry.Promise = NewPromise<TValue>();
            invokeIndexes.push_back(index);

            results[index] = entry.Promise;
        }

        std::vector<TKey> invokeKeys;
        for (auto index : invokeIndexes) {
            invokeKeys.push_back(keys[index]);
        }

        InvokeGetMany(invokeKeys);
    }

    return Combine(results);
}

template <class TKey, class TValue>
bool TExpiringCache<TKey, TValue>::TryRemove(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    return Map_.erase(key) != 0;
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::Clear()
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    Map_.clear();
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::SetResult(const TKey& key, const TErrorOr<TValue>& valueOrError)
{
    auto it = Map_.find(key);
    if (it == Map_.end())
        return;

    auto& entry = it->second;

    auto expirationTime = valueOrError.IsOK() ? Config_->SuccessExpirationTime : Config_->FailureExpirationTime;
    entry.Deadline = TInstant::Now() + expirationTime;
    if (entry.Promise.IsSet()) {
        entry.Promise = MakePromise(valueOrError);
    } else {
        entry.Promise.Set(valueOrError);
    }

    if (valueOrError.IsOK()) {
        NTracing::TNullTraceContextGuard guard;
        entry.ProbationCookie = NConcurrency::TDelayedExecutor::Submit(
            BIND(&TExpiringCache::InvokeGet, MakeWeak(this), key),
            Config_->SuccessProbationTime);
    }
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::InvokeGet(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);

    auto it = Map_.find(key);
    if (it == Map_.end()) {
        return;
    }

    auto future = it->second.ProbationFuture = DoGet(key);

    guard.Release();

    future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TValue>& valueOrError) {
        NConcurrency::TWriterGuard guard(SpinLock_);
        SetResult(key, valueOrError);
    }));
}

template <class TKey, class TValue>
void TExpiringCache<TKey, TValue>::InvokeGetMany(const std::vector<TKey>& keys)
{
    DoGetMany(keys)
    .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TValue>>& valueOrError) {
        NConcurrency::TWriterGuard guard(SpinLock_);

        if (valueOrError.IsOK()) {
            for (size_t index = 0; index < keys.size(); ++index) {
                SetResult(keys[index], valueOrError.Value()[index]);
            }
        } else {
            for (const auto& key : keys) {
                SetResult(key, TError(valueOrError));
            }
        }
    }));
}

template <class TKey, class TValue>
TFuture<typename TExpiringCache<TKey, TValue>::TCombinedValue> TExpiringCache<TKey, TValue>::DoGetMany(const std::vector<TKey>& keys)
{
    std::vector<TFuture<TValue>> results;

    for (const auto& key : keys) {
        results.push_back(DoGet(key));
    }

    return Combine(results);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
