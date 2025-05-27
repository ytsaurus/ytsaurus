#pragma once

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/system/rwlock.h>

namespace NSQLComplete {

    struct TLocalCacheConfig {
        size_t Capacity = 256 * 1024 * 1024;
        TDuration TTL = TDuration::Seconds(8);
    };

    namespace NPrivate {

        template <CCacheKey TKey, CCacheValue TValue, CSizeProvider<TValue> TSizeProvider>
        class TLocalCache: public ICache<TKey, TValue> {
        private:
            using TEntry = ICache<TKey, TValue>::TEntry;

            struct TCell {
                TValue Value;
                TInstant Death;
            };

            struct TCellSizeProvider {
                size_t operator()(const TCell& entry) const {
                    return TSizeProvider()(entry.Value);
                };
            };

            using TStorage = TLFUCache<
                TKey, TCell,
                TNoopDelete, std::allocator<TKey>,
                TCellSizeProvider>;

        public:
            TLocalCache(TIntrusivePtr<ITimeProvider> clock, TLocalCacheConfig config)
                : Clock_(std::move(clock))
                , Config_(std::move(config))
                , Origin_(/* maxSize = */ Config_.Capacity)
            {
            }

            NThreading::TFuture<TEntry> Get(const TKey& key) const override {
                TEntry entry;
                {
                    TReadGuard _(Mutex_);
                    if (auto it = Origin_.Find(key); it != Origin_.End()) {
                        entry.Value = it->Value;
                        entry.IsExpired = (it->Death < Clock_->Now());
                    }
                }
                return NThreading::MakeFuture(std::move(entry));
            }

            NThreading::TFuture<void> Update(const TKey& key, TValue value) const override {
                TCell entry = {
                    .Value = std::move(value),
                    .Death = Clock_->Now() + Config_.TTL,
                };
                {
                    TWriteGuard _(Mutex_);
                    Origin_.Update(key, std::move(entry));
                }
                return NThreading::MakeFuture();
            }

        private:
            TIntrusivePtr<ITimeProvider> Clock_;
            TLocalCacheConfig Config_;

            TRWMutex Mutex_;
            mutable TStorage Origin_;
        };

    } // namespace NPrivate

    template <
        NPrivate::CCacheKey TKey,
        NPrivate::CCacheValue TValue,
        NPrivate::CSizeProvider<TValue> TSizeProvider = TUniformSizeProvider<TValue>>
    ICache<TKey, TValue>::TPtr MakeLocalCache(
        TIntrusivePtr<ITimeProvider> clock,
        TLocalCacheConfig config) {
        return new NPrivate::TLocalCache<TKey, TValue, TSizeProvider>(std::move(clock), std::move(config));
    }

} // namespace NSQLComplete
