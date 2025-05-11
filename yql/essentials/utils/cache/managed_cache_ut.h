#pragma once

#include "managed_cache_storage.h"
#include "managed_cache_maintenance.h"

namespace NYql {

    using TKey = TString;
    using TValue = TString;

    using TCacheStorage = TManagedCacheStorage<TKey, TValue>;
    using TCacheMaintenance = TManagedCacheMaintenance<TKey, TValue>;

    inline TCacheStorage::TQuery MakeDummyQuery(size_t* served = nullptr, bool* isFailing = nullptr) {
        if (served != nullptr) {
            *served = 0;
        }

        if (isFailing != nullptr) {
            *isFailing = false;
        }

        return [served, isFailing](const TVector<TKey>& keys) {
            if (served != nullptr) {
                *served += 1;
            }

            if (isFailing != nullptr && *isFailing) {
                return NThreading::MakeErrorFuture<TVector<TValue>>(
                    std::make_exception_ptr(yexception() << "O_o"));
            }

            return NThreading::MakeFuture<TVector<TValue>>(keys);
        };
    }

    struct TCountingCacheListener: IManagedCacheStorageListener {
        void OnEvict(size_t count) override {
            Evicted += count;
        }

        size_t Evicted = 0;
    };

} // namespace NYql
