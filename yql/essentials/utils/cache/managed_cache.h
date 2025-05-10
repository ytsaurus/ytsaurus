#pragma once

#include "managed_cache_storage.h"
#include "managed_cache_maintenance.h"

#include <util/thread/factory.h>

namespace NYql {

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TManagedCache: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TManagedCache>;

        TManagedCache(
            IThreadFactory* executor,
            TManagedCacheConfig config,
            TManagedCacheStorage<TKey, TValue>::TQuery query)
            : Storage_(new TManagedCacheStorage<TKey, TValue>(std::move(query)))
        {
            auto token = Cancellation_.Token();
            MaintenanceThread_ = executor->Run([storage = Storage_, config, token]() {
                TManagedCacheMaintenance<TKey, TValue>(storage, config).Run(token);
            });
        }

        NThreading::TFuture<TValue> Get(const TKey& key) const {
            return Storage_->Get(key);
        }

        ~TManagedCache() {
            Cancellation_.Cancel();
            MaintenanceThread_->Join();
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        NThreading::TCancellationTokenSource Cancellation_;
        THolder<IThreadFactory::IThread> MaintenanceThread_;
    };

    template <
        NPrivate::CCacheKey TKey,
        NPrivate::CCacheValue TValue,
        class TQuery = TManagedCacheStorage<TKey, TValue>::TQuery>
    TManagedCache<TKey, TValue>::TPtr StartManagedCache(
        IThreadFactory* executor, TManagedCacheConfig config, TQuery query) {
        return new TManagedCache<TKey, TValue>(
            executor, std::move(config), std::move(query));
    }

} // namespace NYql
