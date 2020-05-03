#pragma once
#ifndef EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_expiring_cache.h"
// For the sake of sane code completion.
#include "async_expiring_cache.h"
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
    , Future(Promise.ToFuture().ToUncancelable())
{ }

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TEntry::IsExpired(NProfiling::TCpuInstant now) const
{
    return now > AccessDeadline || now > UpdateDeadline;
}

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TAsyncExpiringCache(
    TAsyncExpiringCacheConfigPtr config,
    NProfiling::TProfiler profiler)
    : Config_(std::move(config))
    , Profiler_(std::move(profiler))
{
    if (Config_->BatchUpdate && Config_->RefreshTime) {
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TAsyncExpiringCache::UpdateAll, MakeWeak(this)),
            *Config_->RefreshTime);
    }
}

template <class TKey, class TValue>
typename TAsyncExpiringCache<TKey, TValue>::TExtendedGetResult TAsyncExpiringCache<TKey, TValue>::GetExtended(
    const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();

    // Fast path.
    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now)) {
                Profiler_.Increment(HitCounter_);
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                return {entry->Future, false};
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
                Profiler_.Increment(HitCounter_);
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                return {entry->Future, false};
            }
        }

        Profiler_.Increment(MissedCounter_);
        auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
        auto entry = New<TEntry>(accessDeadline);
        auto future = entry->Future;
        // NB: we don't want to hold a strong reference to entry after releasing the guard, so we make a weak reference here.
        auto weakEntry = MakeWeak(entry);
        YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
        Profiler_.Update(SizeCounter_, Map_.size());
        guard.Release();
        InvokeGet(weakEntry, key, /* isPeriodicUpdate */ false);
        return {future, true};
    }
}

template <class TKey, class TValue>
TFuture<TValue> TAsyncExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    return GetExtended(key).Future;
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::Get(
    const std::vector<TKey>& keys)
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
                    Profiler_.Increment(HitCounter_);
                    results[index] = entry->Future;
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
                    Profiler_.Increment(HitCounter_);
                    results[index] = entry->Future;
                    entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                    continue;
                }
            }

            Profiler_.Increment(MissedCounter_);

            auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            auto entry = New<TEntry>(accessDeadline);

            invokeIndexes.push_back(index);
            invokeEntries.push_back(entry);
            results[index] = entry->Future;

            YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
        }

        Profiler_.Update(SizeCounter_, Map_.size());

        std::vector<TKey> invokeKeys;
        for (auto index : invokeIndexes) {
            invokeKeys.push_back(keys[index]);
        }

        guard.Release();
        InvokeGetMany(invokeEntries, invokeKeys, /* isPeriodicUpdate */ false);
    }

    return CombineAll(results);
}

template <class TKey, class TValue>
std::optional<TErrorOr<TValue>> TAsyncExpiringCache<TKey, TValue>::Find(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();

    NConcurrency::TReaderGuard guard(SpinLock_);

    auto it = Map_.find(key);
    if (it != Map_.end()) {
        const auto& entry = it->second;
        if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
            entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            Profiler_.Increment(HitCounter_);
            return entry->Future.Get();
        }
    }
    Profiler_.Increment(MissedCounter_);
    return std::nullopt;
}

template <class TKey, class TValue>
std::vector<std::optional<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::Find(const std::vector<TKey>& keys)
{
    auto now = NProfiling::GetCpuInstant();

    std::vector<std::optional<TErrorOr<TValue>>> results(keys.size());
    std::vector<size_t> fetchIndexes;

    NConcurrency::TReaderGuard guard(SpinLock_);

    for (size_t index = 0; index < keys.size(); ++index) {
        auto it = Map_.find(keys[index]);
        if (it != Map_.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
                Profiler_.Increment(HitCounter_);
                results[index] = entry->Future.Get();
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            } else {
                Profiler_.Increment(MissedCounter_);
            }
        } else {
            Profiler_.Increment(MissedCounter_);
        }
    }
    return results;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Invalidate(const TKey& key)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    auto it = Map_.find(key);
    if (it != Map_.end() && it->second->Promise.IsSet()) {
        NConcurrency::TDelayedExecutor::CancelAndClear(it->second->ProbationCookie);
        Map_.erase(it);
        Profiler_.Update(SizeCounter_, Map_.size());
        OnEvicted(key);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Set(const TKey& key, TErrorOr<TValue> valueOrError)
{
    auto now = NProfiling::GetCpuInstant();
    auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
    auto expirationTime = valueOrError.IsOK() ? Config_->ExpireAfterSuccessfulUpdateTime : Config_->ExpireAfterFailedUpdateTime;

    NConcurrency::TWriterGuard guard(SpinLock_);
    auto it = Map_.find(key);
    if (it != Map_.end()) {
        const auto& entry = it->second;
        if (!entry->Promise.IsSet()) {
            entry->Promise.Set(std::move(valueOrError));
        } else {
            entry->Promise = MakePromise(std::move(valueOrError));
            entry->Future = entry->Promise.ToFuture();
        }
        if (expirationTime == TDuration::Zero()) {
            Map_.erase(it);
            OnEvicted(key);
        } else {
            entry->AccessDeadline = accessDeadline;
            entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
        }
    } else if (expirationTime != TDuration::Zero()) {
        auto entry = New<TEntry>(accessDeadline);
        entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
        entry->Promise.Set(std::move(valueOrError));
        YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
        if (!Config_->BatchUpdate && valueOrError.IsOK() && Config_->RefreshTime) {
            entry->ProbationCookie = NConcurrency::TDelayedExecutor::Submit(
                BIND_DONT_CAPTURE_TRACE_CONTEXT(
                    &TAsyncExpiringCache::InvokeGet,
                    MakeWeak(this),
                    MakeWeak(entry),
                    key,
                    /* isPeriodicUpdate */ true),
                *Config_->RefreshTime);
        }
    }
    Profiler_.Update(SizeCounter_, Map_.size());
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Clear()
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    if (!Config_->BatchUpdate) {
        for (const auto& [key, entry] : Map_) {
            if (entry->Promise.IsSet()) {
                NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
            }
        }
    }
    Map_.clear();
    Profiler_.Update(SizeCounter_, 0);
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::SetResult(
    const TWeakPtr<TEntry>& weakEntry,
    const TKey& key,
    const TErrorOr<TValue>& valueOrError,
    bool isPeriodicUpdate)
{
    auto entry = weakEntry.Lock();
    if (!entry) {
        return;
    }

    // Ignore cancelation errors during periodic update.
    if (isPeriodicUpdate && valueOrError.FindMatching(NYT::EErrorCode::Canceled)) {
        return;
    }

    auto it = Map_.find(key);
    YT_ASSERT(it != Map_.end() && it->second == entry);

    auto now = NProfiling::GetCpuInstant();
    auto expirationTime = valueOrError.IsOK() ? Config_->ExpireAfterSuccessfulUpdateTime : Config_->ExpireAfterFailedUpdateTime;
    entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
    if (entry->Promise.IsSet()) {
        entry->Promise = MakePromise(valueOrError);
        entry->Future = entry->Promise.ToFuture();
    } else {
        entry->Promise.Set(valueOrError);
    }

    if (now > entry->AccessDeadline || expirationTime == TDuration::Zero()) {
        Map_.erase(it);
        Profiler_.Update(SizeCounter_, Map_.size());
        OnEvicted(key);
        return;
    }

    if (!Config_->BatchUpdate && valueOrError.IsOK() && Config_->RefreshTime) {
        entry->ProbationCookie = NConcurrency::TDelayedExecutor::Submit(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(
                &TAsyncExpiringCache::InvokeGet,
                MakeWeak(this),
                MakeWeak(entry),
                key,
                /* isPeriodicUpdate */ true),
            *Config_->RefreshTime);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGet(
    const TWeakPtr<TEntry>& weakEntry,
    const TKey& key,
    bool isPeriodicUpdate)
{
    if (isPeriodicUpdate && TryEraseExpired(weakEntry, key)) {
        return;
    }

    DoGet(key, isPeriodicUpdate)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TValue>& valueOrError) {
            NConcurrency::TWriterGuard guard(SpinLock_);

            SetResult(weakEntry, key, valueOrError, isPeriodicUpdate);
        }));
}

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TryEraseExpired(const TWeakPtr<TEntry>& weakEntry, const TKey& key)
{
    auto entry = weakEntry.Lock();
    if (!entry) {
        return true;
    }

    auto now = NProfiling::GetCpuInstant();

    if (now > entry->AccessDeadline) {
        NConcurrency::TWriterGuard writerGuard(SpinLock_);
        if (auto it = Map_.find(key); it != Map_.end() && now > it->second->AccessDeadline) {
            Map_.erase(it);
            Profiler_.Update(SizeCounter_, Map_.size());
            OnEvicted(key);
        }
        return true;
    }
    return false;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGetMany(
    const std::vector<TWeakPtr<TEntry>>& entries,
    const std::vector<TKey>& keys,
    bool isPeriodicUpdate)
{
    DoGetMany(keys, isPeriodicUpdate)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<TValue>>>& valueOrError) {
            NConcurrency::TWriterGuard guard(SpinLock_);

            if (valueOrError.IsOK()) {
                for (size_t index = 0; index < keys.size(); ++index) {
                    SetResult(entries[index], keys[index], valueOrError.Value()[index], isPeriodicUpdate);
                }
            } else {
                for (size_t index = 0; index < keys.size(); ++index) {
                    SetResult(entries[index], keys[index], TError(valueOrError), isPeriodicUpdate);
                }
            }

            if (isPeriodicUpdate) {
                NConcurrency::TDelayedExecutor::Submit(
                    BIND(&TAsyncExpiringCache::UpdateAll, MakeWeak(this)),
                    *Config_->RefreshTime);
            }
        }));
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::DoGetMany(
    const std::vector<TKey>& keys,
    bool isPeriodicUpdate) noexcept
{
    std::vector<TFuture<TValue>> results;
    for (const auto& key : keys) {
        results.push_back(DoGet(key, isPeriodicUpdate));
    }
    return CombineAll(std::move(results));
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnEvicted(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::UpdateAll()
{
    std::vector<TWeakPtr<TEntry>> entries;
    std::vector<TKey> keys;
    std::vector<TKey> expiredKeys;

    auto now = NProfiling::GetCpuInstant();

    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        for (const auto& [key, entry] : Map_) {
            if (entry->Promise.IsSet()) {
                if (now > entry->AccessDeadline) {
                    expiredKeys.push_back(key);
                } else if (entry->Future.Get().IsOK()) {
                    keys.push_back(key);
                    entries.push_back(MakeWeak(entry));
                }
            }
        }
    }

    if (!expiredKeys.empty()) {
        NConcurrency::TWriterGuard guard(SpinLock_);
        for (const auto& key : expiredKeys) {
            if (auto it = Map_.find(key); it != Map_.end()) {
                const auto& [_, entry] = *it;
                if (entry->Promise.IsSet()) {
                    if (now > entry->AccessDeadline) {
                        Map_.erase(it);
                        Profiler_.Update(SizeCounter_, Map_.size());
                        OnEvicted(key);
                    } else if (entry->Future.Get().IsOK()) {
                        keys.push_back(key);
                        entries.push_back(MakeWeak(entry));
                    }
                }
            }
        }
    }

    if (entries.empty()) {
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TAsyncExpiringCache::UpdateAll, MakeWeak(this)),
            *Config_->RefreshTime);
    } else {
        InvokeGetMany(entries, keys, /* isPeriodicUpdate */ true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
