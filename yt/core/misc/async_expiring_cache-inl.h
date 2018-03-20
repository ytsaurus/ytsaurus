#pragma once
#ifndef EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_expiring_cache.h"
#endif

#include "config.h"

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TEntry::TEntry(NProfiling::TCpuInstant accessDeadline)
    : AccessDeadline(accessDeadline)
    , UpdateDeadline(std::numeric_limits<NProfiling::TCpuInstant>::max())
    , Promise(NewPromise<TValue>())
{ }

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TEntry::IsExpired(NProfiling::TCpuInstant now) const
{
    return now > AccessDeadline || now > UpdateDeadline;
}

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TAsyncExpiringCache(TExpiringCacheConfigPtr config)
    : Config_(std::move(config))
{ }

template <class TKey, class TValue>
TFuture<TValue> TAsyncExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();

    // Fast path.
    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now)) {
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                return entry->Promise;
            }
        }
    }

    // Slow path.
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
        auto it = Map_.find(key);

        if (it != Map_.end()) {
            auto& entry = it->second;
            if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
                Map_.erase(it);
            } else {
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                return entry->Promise;
            }
        }

        auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
        auto entry = New<TEntry>(accessDeadline);
        auto promise = entry->Promise;
        // NB: we don't want to hold a strong reference to entry after releasing the guard, so we make a weak reference here.
        auto weakEntry = MakeWeak(entry);
        YCHECK(Map_.insert(std::make_pair(key, std::move(entry))).second);
        guard.Release();
        InvokeGet(weakEntry, key, false);
        return promise;
    }
}

template <class TKey, class TValue>
TFuture<typename TAsyncExpiringCache<TKey, TValue>::TCombinedValue> TAsyncExpiringCache<TKey, TValue>::Get(const std::vector<TKey>& keys)
{
    auto now = NProfiling::GetCpuInstant();

    std::vector<TFuture<TValue>> results(keys.size());
    std::vector<size_t> fetchIndexes;

    // Fast path.
    {
        NConcurrency::TReaderGuard guard(SpinLock_);

        for (size_t index = 0; index < keys.size(); ++index) {
            auto it = Map_.find(keys[index]);
            if (it != Map_.end()) {
                const auto& entry = it->second;
                if (!entry->IsExpired(now)) {
                    results[index] = entry->Promise;
                    entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                    continue;
                }
            }
            fetchIndexes.push_back(index);
        }
    }

    // Slow path.
    if (!fetchIndexes.empty()) {
        std::vector<size_t> invokeIndexes;
        std::vector<TWeakPtr<TEntry>> invokeEntries;

        NConcurrency::TWriterGuard guard(SpinLock_);
        for (auto index : fetchIndexes) {
            const auto& key = keys[index];

            auto it = Map_.find(keys[index]);
            if (it != Map_.end()) {
                auto& entry = it->second;
                if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                    NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
                    Map_.erase(it);
                } else {
                    results[index] = entry->Promise;
                    entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                    continue;
                }
            }

            auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            auto entry = New<TEntry>(accessDeadline);

            invokeIndexes.push_back(index);
            invokeEntries.push_back(entry);
            results[index] = entry->Promise;

            YCHECK(Map_.insert(std::make_pair(key, std::move(entry))).second);
        }

        std::vector<TKey> invokeKeys;
        for (auto index : invokeIndexes) {
            invokeKeys.push_back(keys[index]);
        }

        guard.Release();
        InvokeGetMany(invokeEntries, invokeKeys);
    }

    return Combine(results);
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Invalidate(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    auto it = Map_.find(key);
    if (it != Map_.end() && it->second->Promise.IsSet()) {
        NConcurrency::TDelayedExecutor::CancelAndClear(it->second->ProbationCookie);
        Map_.erase(it);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Clear()
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    for (const auto& pair : Map_) {
        if (pair.second->Promise.IsSet()) {
            NConcurrency::TDelayedExecutor::CancelAndClear(pair.second->ProbationCookie);
        }
    }
    Map_.clear();
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::SetResult(const TWeakPtr<TEntry>& weakEntry, const TKey& key, const TErrorOr<TValue>& valueOrError)
{
    auto entry = weakEntry.Lock();
    if (!entry) {
        return;
    }

    auto it = Map_.find(key);
    Y_ASSERT(it != Map_.end() && it->second == entry);

    auto now = NProfiling::GetCpuInstant();
    auto expirationTime = valueOrError.IsOK() ? Config_->ExpireAfterSuccessfulUpdateTime : Config_->ExpireAfterFailedUpdateTime;
    entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
    if (entry->Promise.IsSet()) {
        entry->Promise = MakePromise(valueOrError);
    } else {
        entry->Promise.Set(valueOrError);
    }

    if (now > entry->AccessDeadline) {
        Map_.erase(key);
        return;
    }

    if (valueOrError.IsOK()) {
        NTracing::TNullTraceContextGuard guard;
        entry->ProbationCookie = NConcurrency::TDelayedExecutor::Submit(
            BIND(&TAsyncExpiringCache::InvokeGet, MakeWeak(this), MakeWeak(entry), key, true),
            Config_->RefreshTime);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGet(const TWeakPtr<TEntry>& weakEntry, const TKey& key, bool checkExpired)
{
    if (checkExpired && TryEraseExpired(weakEntry, key)) {
        return;
    }

    DoGet(key)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TValue>& valueOrError) {
            NConcurrency::TWriterGuard guard(SpinLock_);

            SetResult(weakEntry, key, valueOrError);
        }));
}

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TryEraseExpired(const TWeakPtr<TEntry>& weakEntry, const TKey& key)
{
    auto entry = weakEntry.Lock();
    if (!entry) {
        return true;
    }

    if (NProfiling::GetCpuInstant() > entry->AccessDeadline) {
        NConcurrency::TWriterGuard writerGuard(SpinLock_);
        Map_.erase(key);
        return true;
    }
    return false;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGetMany(const std::vector<TWeakPtr<TEntry>>& entries, const std::vector<TKey>& keys)
{
    DoGetMany(keys)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TValue>>& valueOrError) {
            NConcurrency::TWriterGuard guard(SpinLock_);

            if (valueOrError.IsOK()) {
                for (size_t index = 0; index < keys.size(); ++index) {
                    SetResult(entries[index], keys[index], valueOrError.Value()[index]);
                }
            } else {
                for (size_t index = 0; index < keys.size(); ++index) {
                    SetResult(entries[index], keys[index], TError(valueOrError));
                }
            }
        }));
}

template <class TKey, class TValue>
TFuture<typename TAsyncExpiringCache<TKey, TValue>::TCombinedValue> TAsyncExpiringCache<TKey, TValue>::DoGetMany(const std::vector<TKey>& keys)
{
    std::vector<TFuture<TValue>> results;

    for (const auto& key : keys) {
        results.push_back(DoGet(key));
    }

    return Combine(results);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
