#pragma once

#include "managed_cache_listener.h"
#include "managed_cache_storage.h"

#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/datetime/base.h>

namespace NYql {

    struct TManagedCacheConfig {
        TDuration UpdatePeriod = TDuration::Seconds(5);
        size_t UpdatesPerEviction = 3;
    };

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TManagedCacheMaintenance: public TTaskScheduler::IRepeatedTask {
    public:
        using TPtr = THolder<TManagedCacheMaintenance>;
        using TListenerPtr = TIntrusivePtr<IManagedCacheMaintainenceListener>;

        TManagedCacheMaintenance(
            TManagedCacheStorage<TKey, TValue>::TPtr storage,
            TManagedCacheConfig config,
            TListenerPtr listener = new IManagedCacheMaintainenceListener(),
            NThreading::TCancellationToken token = NThreading::TCancellationTokenSource().Token())
            : Storage_(std::move(storage))
            , Config_(std::move(config))
            , Listener_(std::move(listener))
            , Token_(std::move(token))
        {
            Y_ENSURE(
                TDuration::MicroSeconds(100) <= Config_.UpdatePeriod &&
                    Config_.UpdatePeriod <= TDuration::Days(7),
                "UpdatePeriod must be in [100ms, 7d], got " << Config_.UpdatePeriod);
            Y_ENSURE(
                1 <= Config_.UpdatesPerEviction &&
                    Config_.UpdatesPerEviction <= 10'000,
                "EvictionFrequency must be in [1, 10'000], got " << Config_.UpdatesPerEviction);
        }

        bool Process() override {
            if (Token_.IsCancellationRequested()) {
                return false;
            }
            Tick();
            Sleep(Config_.UpdatePeriod);
            return true;
        }

        void Tick() try {
            Listener_->OnTickBegin();
            Tick_ += 1;
            if (Tick_ % Config_.UpdatesPerEviction == 0) {
                Storage_->Evict();
            }
            Storage_->Update();
            Listener_->OnTickSucceded();
        } catch (const std::exception& e) {
            Listener_->OnTickFailed(e);
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        size_t Tick_ = 0;
        TManagedCacheConfig Config_;
        TListenerPtr Listener_;
        NThreading::TCancellationToken Token_;
    };

} // namespace NYql
