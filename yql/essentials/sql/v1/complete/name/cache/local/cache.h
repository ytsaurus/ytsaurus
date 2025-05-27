#pragma once

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

#include <library/cpp/cache/thread_safe_cache.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NSQLComplete {

    struct TLocalCacheConfiguration {
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

            struct TEntrySizeProvider {
                size_t operator()(const TCell& entry) const {
                    return TSizeProvider()(entry.Value);
                };
            };

            using TCache = TThreadSafeLFUCacheWithSizeProvider<TKey, TCell, TEntrySizeProvider, TKey>;

            class TCallbacks: public TCache::ICallbacks {
            public:
                TKey GetKey(TKey key) const override {
                    return key;
                }

                TCell* CreateObject(TKey /* value */) const override {
                    Y_ABORT();
                }
            };

        public:
            TLocalCache(
                TIntrusivePtr<ITimeProvider> clock,
                TLocalCacheConfiguration config)
                : Clock_(std::move(clock))
                , Config_(std::move(config))
                , Origin_(Callbacks_)
            {
            }

            NThreading::TFuture<TEntry> Get(const TKey& key) const override {
                TEntry entry;
                if (typename TCache::TPtr ptr = Origin_.GetOrNull(key)) {
                    entry.Value = ptr->Value;
                    entry.IsExpired = (ptr->Death < Clock_->Now());
                }
                return NThreading::MakeFuture(std::move(entry));
            }

            NThreading::TFuture<void> Update(const TKey& key, TValue value) const override {
                TCell* entry = new TCell{
                    .Value = std::move(value),
                    .Death = Clock_->Now() + Config_.TTL,
                };
                Origin_.Update(key, typename TCache::TPtr(entry));
                return NThreading::MakeFuture();
            }

        private:
            TIntrusivePtr<ITimeProvider> Clock_;
            TLocalCacheConfiguration Config_;
            TCallbacks Callbacks_;
            mutable TCache Origin_;
        };

    } // namespace NPrivate

    template <
        NPrivate::CCacheKey TKey,
        NPrivate::CCacheValue TValue,
        NPrivate::CSizeProvider<TValue> TSizeProvider = TUniformSizeProvider<TValue>>
    ICache<TKey, TValue>::TPtr MakeLocalCache(
        TIntrusivePtr<ITimeProvider> clock,
        TLocalCacheConfiguration config) {
        return new NPrivate::TLocalCache<TKey, TValue, TSizeProvider>(std::move(clock), std::move(config));
    }

} // namespace NSQLComplete
