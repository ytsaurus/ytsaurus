#pragma once

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/time_provider/monotonic_provider.h>

#include <util/system/mutex.h>

namespace NSQLComplete {

    struct TLocalCacheConfig {
        size_t Capacity = 1 * 1024 * 1024;
        TDuration TTL = TDuration::Seconds(8);
    };

    namespace NPrivate {

        template <CCacheValue TValue>
        struct TLocalCacheCell {
            TValue Value;
            NMonotonic::TMonotonic Deadline;
            size_t KeySize = 0;
        };

        template <CCacheKey TKey, CCacheValue TValue>
        class TLocalCache: public ICache<TKey, TValue> {
        private:
            using TEntry = ICache<TKey, TValue>::TEntry;
            using TCell = TLocalCacheCell<TValue>;

            using TStorage = TLFUCache<
                TKey, TCell,
                TNoopDelete, std::allocator<TKey>,
                TByteSize<TCell>>;

        public:
            TLocalCache(TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> clock, TLocalCacheConfig config)
                : Clock_(std::move(clock))
                , Config_(std::move(config))
                , Origin_(/* maxSize = */ Config_.Capacity)
            {
            }

            NThreading::TFuture<TEntry> Get(const TKey& key) const override {
                TEntry entry;
                with_lock (Mutex_) {
                    if (auto it = Origin_.Find(key); it != Origin_.End()) {
                        entry.Value = it->Value;
                        entry.IsExpired = (it->Deadline < Clock_->Now());
                    }
                }
                return NThreading::MakeFuture(std::move(entry));
            }

            NThreading::TFuture<void> Update(const TKey& key, TValue value) const override {
                TLocalCacheCell entry = {
                    .Value = std::move(value),
                    .Deadline = Clock_->Now() + Config_.TTL,
                    .KeySize = TByteSize<TKey>()(key),
                };
                with_lock (Mutex_) {
                    Origin_.Update(key, std::move(entry));
                }
                return NThreading::MakeFuture();
            }

        private:
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> Clock_;
            TLocalCacheConfig Config_;

            TMutex Mutex_;
            mutable TStorage Origin_;
        };

    } // namespace NPrivate

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    ICache<TKey, TValue>::TPtr MakeLocalCache(
        TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> clock,
        TLocalCacheConfig config) {
        return new NPrivate::TLocalCache<TKey, TValue>(std::move(clock), std::move(config));
    }

    template <NPrivate::CCacheValue TValue>
    struct TByteSize<NPrivate::TLocalCacheCell<TValue>> {
        size_t operator()(const NPrivate::TLocalCacheCell<TValue>& x) const noexcept {
            return sizeof(x);
        }
    };

} // namespace NSQLComplete
