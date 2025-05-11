#pragma once

#include "managed_cache_storage.h"

#include <library/cpp/threading/cancellation/cancellation_token.h>

#include <util/datetime/base.h>

namespace NYql {

    struct TManagedCacheConfig {
        TDuration UpdatePeriod = TDuration::Seconds(5);
        size_t EvictionFrequency = 3;
    };

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TManagedCacheMaintenance {
    public:
        using TPtr = THolder<TManagedCacheMaintenance>;

        TManagedCacheMaintenance(
            TManagedCacheStorage<TKey, TValue>::TPtr storage,
            TManagedCacheConfig config)
            : Storage_(std::move(storage))
            , Config_(std::move(config))
        {
            Y_ENSURE(
                TDuration::MicroSeconds(100) <= Config_.UpdatePeriod &&
                    Config_.UpdatePeriod <= TDuration::Days(7),
                "UpdatePeriod must be in [100ms, 7d], got " << Config_.UpdatePeriod);
            Y_ENSURE(
                1 <= Config_.EvictionFrequency &&
                    Config_.EvictionFrequency <= 10'000,
                "EvictionFrequency must be in [1, 10'000], got " << Config_.EvictionFrequency);
        }

        void Run(NThreading::TCancellationToken token) {
            while (!token.IsCancellationRequested()) {
                Tick();
                Sleep(Config_.UpdatePeriod);
            }
        }

        void Tick() {
            Tick_ += 1;
            if (Tick_ % Config_.EvictionFrequency == 0) {
                Storage_->Evict();
            }
            Storage_->Update();
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        size_t Tick_ = 0;
        TManagedCacheConfig Config_;
    };

} // namespace NYql
