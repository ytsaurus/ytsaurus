#pragma once

#include "managed_cache_storage.h"

#include <library/cpp/threading/cancellation/cancellation_token.h>

#include <util/datetime/base.h>

namespace NYql {

    struct TManagedCacheConfig {
        TDuration TickPause = TDuration::Seconds(5);
        size_t UpdateFrequency = 1;
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
            Y_ENSURE(TDuration::MicroSeconds(100) < Config_.TickPause);
            Y_ENSURE(Config_.TickPause < TDuration::Days(7));
            Y_ENSURE(0 < Config_.UpdateFrequency);
            Y_ENSURE(Config_.UpdateFrequency <= Config_.EvictionFrequency);
            Y_ENSURE(Config_.EvictionFrequency <= 10'000);
        }

        void Run(NThreading::TCancellationToken token) {
            while (!token.IsCancellationRequested()) {
                Tick();
                Sleep(Config_.TickPause);
            }
        }

        void Tick() {
            Tick_ += 1;
            if (Tick_ % Config_.EvictionFrequency == 0) {
                Storage_->Evict();
            }
            if (Tick_ % Config_.UpdateFrequency == 0) {
                Storage_->Update();
            }
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        size_t Tick_ = 0;
        TManagedCacheConfig Config_;
    };

} // namespace NYql
