#pragma once
#ifndef EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_expiring_cache.h"
// For the sake of sane code completion.
#include "async_expiring_cache.h"
#endif

#include "config.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/profiling/timing.h>

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
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Config_(std::move(config))
    , Logger_(std::move(logger))
    , HitCounter_(profiler.Counter("/hit"))
    , MissedCounter_(profiler.Counter("/miss"))
    , SizeCounter_(profiler.Gauge("/size"))
{
    if (Config_->BatchUpdate && Config_->RefreshTime && *Config_->RefreshTime) {
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TAsyncExpiringCache::UpdateAll, MakeWeak(this)),
            *Config_->RefreshTime);
    }
}

template <class TKey, class TValue>
typename TAsyncExpiringCache<TKey, TValue>::TExtendedGetResult TAsyncExpiringCache<TKey, TValue>::GetExtended(
    const TKey& key)
{
    const auto& Logger = Logger_;
    auto now = NProfiling::GetCpuInstant();

    // Fast path.
    {
        auto guard = ReaderGuard(SpinLock_);

        if (auto it = Map_.find(key); it != Map_.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now)) {
                HitCounter_.Increment();
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                OnHit(key);
                return {entry->Future, false};
            }
        }
    }

    // Slow path.
    {
        auto guard = WriterGuard(SpinLock_);

        if (auto it = Map_.find(key); it != Map_.end()) {
            auto& entry = it->second;
            if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
                Map_.erase(it);
                OnRemoved(key);
            } else {
                HitCounter_.Increment();
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                OnHit(key);
                if (!entry->Future.IsSet()) {
                    YT_LOG_DEBUG("Waiting for cache entry (Key: %v)",
                        key);
                }
                return {entry->Future, false};
            }
        }

        MissedCounter_.Increment();
        auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
        auto entry = New<TEntry>(accessDeadline);
        auto future = entry->Future;
        // NB: we don't want to hold a strong reference to entry after releasing the guard, so we make a weak reference here.
        auto weakEntry = MakeWeak(entry);
        YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
        OnAdded(key);
        SizeCounter_.Update(Map_.size());
        guard.Release();
        YT_LOG_DEBUG("Populating cache entry (Key: %v)",
            key);
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
    const auto& Logger = Logger_;
    auto now = NProfiling::GetCpuInstant();

    std::vector<TFuture<TValue>> results(keys.size());
    std::vector<TKey> keysToWaitFor;

    auto handleHit = [&] (size_t index, const TEntryPtr& entry) {
        const auto& key = keys[index];
        HitCounter_.Increment();
        results[index] = entry->Future;
        if (!entry->Future.IsSet()) {
            keysToWaitFor.push_back(key);
        }
        entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
        OnHit(key);
    };

    std::vector<size_t> preliminaryIndexesToPopulate;

    // Fast path.
    {
        auto guard = ReaderGuard(SpinLock_);

        for (size_t index = 0; index < keys.size(); ++index) {
            const auto& key = keys[index];
            if (auto it = Map_.find(key); it != Map_.end()) {
                const auto& entry = it->second;
                if (!entry->IsExpired(now)) {
                    handleHit(index, entry);
                    continue;
                }
            }
            preliminaryIndexesToPopulate.push_back(index);
        }
    }

    // Slow path.
    if (!preliminaryIndexesToPopulate.empty()) {
        std::vector<size_t> finalIndexesToPopulate;
        std::vector<TWeakPtr<TEntry>> entriesToPopulate;

        auto guard = WriterGuard(SpinLock_);

        for (auto index : preliminaryIndexesToPopulate) {
            const auto& key = keys[index];
            if (auto it = Map_.find(keys[index]); it != Map_.end()) {
                auto& entry = it->second;
                if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                    NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
                    Map_.erase(it);
                    OnRemoved(key);
                } else {
                    handleHit(index, entry);
                    continue;
                }
            }

            MissedCounter_.Increment();

            auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            auto entry = New<TEntry>(accessDeadline);

            finalIndexesToPopulate.push_back(index);
            entriesToPopulate.push_back(entry);
            results[index] = entry->Future;

            YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
            OnAdded(key);
        }

        SizeCounter_.Update(Map_.size());

        std::vector<TKey> keysToPopulate;
        for (auto index : finalIndexesToPopulate) {
            keysToPopulate.push_back(keys[index]);
        }

        guard.Release();

        if (!keysToWaitFor.empty()) {
            YT_LOG_DEBUG("Waiting for cache entries (Keys: %v)",
                keysToWaitFor);
        }

        if (!keysToPopulate.empty()) {
            YT_LOG_DEBUG("Populating cache entries (Keys: %v)",
                keysToPopulate);
            InvokeGetMany(entriesToPopulate, keysToPopulate, /* isPeriodicUpdate */ false);
        }
    }

    return AllSet(results);
}

template <class TKey, class TValue>
std::optional<TErrorOr<TValue>> TAsyncExpiringCache<TKey, TValue>::Find(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();

    auto guard = ReaderGuard(SpinLock_);

    if (auto it = Map_.find(key); it != Map_.end()) {
        const auto& entry = it->second;
        if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
            HitCounter_.Increment();
            entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
            OnHit(key);
            return entry->Future.Get();
        }
    }

    MissedCounter_.Increment();
    return std::nullopt;
}

template <class TKey, class TValue>
std::vector<std::optional<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::Find(const std::vector<TKey>& keys)
{
    auto now = NProfiling::GetCpuInstant();

    std::vector<std::optional<TErrorOr<TValue>>> results(keys.size());
    std::vector<size_t> indexesToPopulate;

    auto guard = ReaderGuard(SpinLock_);

    for (size_t index = 0; index < keys.size(); ++index) {
        const auto& key = keys[index];
        if (auto it = Map_.find(key); it != Map_.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
                HitCounter_.Increment();
                results[index] = entry->Future.Get();
                entry->AccessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
                OnHit(key);
            } else {
                MissedCounter_.Increment();
            }
        } else {
            MissedCounter_.Increment();
        }
    }
    return results;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Invalidate(const TKey& key)
{
    auto guard = WriterGuard(SpinLock_);

    if (auto it = Map_.find(key); it != Map_.end() && it->second->Promise.IsSet()) {
        NConcurrency::TDelayedExecutor::CancelAndClear(it->second->ProbationCookie);
        Map_.erase(it);
        OnRemoved(key);
        SizeCounter_.Update(Map_.size());
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Set(const TKey& key, TErrorOr<TValue> valueOrError)
{
    auto isValueOK = valueOrError.IsOK();
    auto now = NProfiling::GetCpuInstant();
    auto accessDeadline = now + NProfiling::DurationToCpuDuration(Config_->ExpireAfterAccessTime);
    auto expirationTime = isValueOK ? Config_->ExpireAfterSuccessfulUpdateTime : Config_->ExpireAfterFailedUpdateTime;

    auto guard = WriterGuard(SpinLock_);

    if (auto it = Map_.find(key); it != Map_.end()) {
        const auto& entry = it->second;
        if (entry->Promise.IsSet()) {
            entry->Promise = MakePromise(std::move(valueOrError));
            entry->Future = entry->Promise.ToFuture();
        } else {
            entry->Promise.Set(std::move(valueOrError));
        }
        if (expirationTime == TDuration::Zero()) {
            Map_.erase(it);
            OnRemoved(key);
        } else {
            entry->AccessDeadline = accessDeadline;
            entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
            OnHit(key);
        }
    } else if (expirationTime != TDuration::Zero()) {
        auto entry = New<TEntry>(accessDeadline);
        entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
        entry->Promise.Set(std::move(valueOrError));
        YT_VERIFY(Map_.emplace(key, std::move(entry)).second);
        OnAdded(key);
        if (!Config_->BatchUpdate && isValueOK && Config_->RefreshTime && *Config_->RefreshTime) {
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

    SizeCounter_.Update(Map_.size());
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Clear()
{
    auto guard = WriterGuard(SpinLock_);

    if (!Config_->BatchUpdate) {
        for (const auto& [key, entry] : Map_) {
            if (entry->Promise.IsSet()) {
                NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
            }
        }
    }

    for (const auto& [key, value] : Map_) {
        OnRemoved(key);
    }
    Map_.clear();
    SizeCounter_.Update(0);
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
    if (it == Map_.end() || it->second != entry) {
        return;
    }

    auto now = NProfiling::GetCpuInstant();
    bool canCacheEntry = valueOrError.IsOK() || CanCacheError(valueOrError);

    auto expirationTime = TDuration::Zero();
    if (canCacheEntry) {
        if (valueOrError.IsOK()) {
            expirationTime = Config_->ExpireAfterSuccessfulUpdateTime;
        } else {
            expirationTime = Config_->ExpireAfterFailedUpdateTime;
        }
    }

    bool entryUpdated = false;

    if (canCacheEntry || !entry->Promise.IsSet()) {
        entry->UpdateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);
        if (entry->Promise.IsSet()) {
            entry->Promise = MakePromise(valueOrError);
            entry->Future = entry->Promise.ToFuture();
        } else {
            entry->Promise.Set(valueOrError);
        }
        entryUpdated = true;
    }

    if (entry->IsExpired(now) || (entryUpdated && expirationTime == TDuration::Zero())) {
        Map_.erase(it);
        OnRemoved(key);
        SizeCounter_.Update(Map_.size());
        return;
    }

    if (!Config_->BatchUpdate && valueOrError.IsOK() && Config_->RefreshTime && *Config_->RefreshTime) {
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
            auto guard = WriterGuard(SpinLock_);

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
        auto writerGuard = WriterGuard(SpinLock_);

        if (auto it = Map_.find(key); it != Map_.end() && now > it->second->AccessDeadline) {
            Map_.erase(it);
            OnRemoved(key);
            SizeCounter_.Update(Map_.size());
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
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<TValue>>>& valuesOrError) {
            auto guard = WriterGuard(SpinLock_);

            for (size_t index = 0; index < keys.size(); ++index) {
                SetResult(
                    entries[index],
                    keys[index],
                    valuesOrError.IsOK() ? valuesOrError.Value()[index] : TErrorOr<TValue>(TError(valuesOrError)),
                    isPeriodicUpdate);
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
    return AllSet(std::move(results));
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnAdded(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnRemoved(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnHit(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::CanCacheError(const TError& error) noexcept
{
    return true;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::UpdateAll()
{
    std::vector<TWeakPtr<TEntry>> entries;
    std::vector<TKey> keys;
    std::vector<TKey> expiredKeys;

    auto now = NProfiling::GetCpuInstant();

    {
        auto guard = ReaderGuard(SpinLock_);
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
        auto guard = WriterGuard(SpinLock_);

        for (const auto& key : expiredKeys) {
            if (auto it = Map_.find(key); it != Map_.end()) {
                const auto& [_, entry] = *it;
                if (entry->Promise.IsSet()) {
                    if (now > entry->AccessDeadline) {
                        Map_.erase(it);
                        OnRemoved(key);
                        SizeCounter_.Update(Map_.size());
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
