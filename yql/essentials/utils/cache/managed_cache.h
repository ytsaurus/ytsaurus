#pragma once

#include "managed_cache_listener.h"
#include "managed_cache_storage.h"
#include "managed_cache_maintenance.h"

#include <library/cpp/threading/task_scheduler/task_scheduler.h>

namespace NYql {

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TManagedCache: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TManagedCache>;

        TManagedCache(
            TTaskScheduler& scheduler,
            TManagedCacheConfig config,
            TManagedCacheStorage<TKey, TValue>::TQuery query,
            TIntrusivePtr<IManagedCacheListener> listener)
            : Storage_(new TManagedCacheStorage<TKey, TValue>(std::move(query), listener))
        {
            auto token = Cancellation_.Token();
            TManagedCacheMaintenance<TKey, TValue> maintenance(Storage_, config, listener);
            const auto tick = [token, maintenance]() mutable -> bool {
                if (token.IsCancellationRequested()) {
                    return false;
                }
                maintenance.Tick();
                return true;
            };
            Y_ENSURE(scheduler.AddRepeatedFunc(tick, config.UpdatePeriod));
        }

        NThreading::TFuture<TValue> Get(const TKey& key) const {
            return Storage_->Get(key);
        }

        ~TManagedCache() {
            Cancellation_.Cancel();
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        NThreading::TCancellationTokenSource Cancellation_;
    };

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    TManagedCache<TKey, TValue>::TPtr StartManagedCache(
        TTaskScheduler& scheduler,
        TManagedCacheConfig config,
        typename TManagedCacheStorage<TKey, TValue>::TQuery query,
        TIntrusivePtr<IManagedCacheListener> listener = new IManagedCacheListener()) {
        return new TManagedCache<TKey, TValue>(
            scheduler,
            std::move(config),
            std::move(query),
            std::move(listener));
    }

} // namespace NYql
