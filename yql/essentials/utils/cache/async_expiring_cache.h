#pragma once

#include <concepts>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <util/datetime/base.h>
#include <util/system/rwlock.h>

namespace NYql {

    namespace NPrivate {

        template <class T>
        concept CHashable = requires(const T& x) {
            { std::hash<T>()(x) } -> std::convertible_to<std::size_t>;
        };

        template <class T>
        concept CCacheKey = std::regular<T> && CHashable<T>;

        template <class T>
        concept CCacheValue = std::regular<T>;

    } // namespace NPrivate

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TAsyncExpiringCache: public TThrRefBase {
    private:
        struct TEntry {
            NThreading::TFuture<TValue> Value;
            bool IsReferenced = true;
            bool IsUpdated = true;
        };

        using TStorage = TConcurrentHashMap<TKey, TEntry>;
        using TBucket = typename TStorage::TBucket;
        using TBucketGuard = typename TStorage::TBucketGuard;
        using TActualMap = typename TStorage::TActualMap;

    public:
        using TPtr = TIntrusivePtr<TAsyncExpiringCache>;
        using TQuery = std::function<NThreading::TFuture<TValue>(const TKey&)>;

        TAsyncExpiringCache(
            size_t updateFrequency,
            size_t evictionFrequency,
            TQuery query)
            : Query_(std::move(query))
            , UpdateFrequency_(updateFrequency)
            , EvictionFrequency_(evictionFrequency)
        {
        }

        NThreading::TFuture<TValue> Get(const TKey& key) {
            TBucket& bucket = Storage_.GetBucketForKey(key);
            TBucketGuard guard(bucket.GetMutex());

            if (TEntry* entry = bucket.TryGetUnsafe(key)) {
                entry->IsReferenced = true;
                return entry->Value;
            }

            NThreading::TFuture<TValue> value = Query_(key);

            TActualMap& map = bucket.GetMap();
            map[key] = TEntry{
                .Value = value,
                .IsReferenced = true,
                .IsUpdated = true,
            };

            return value;
        }

        void OnTick() {
            Tick_.fetch_add(1);

            if (Tick_ % UpdateFrequency_ == 0) {
                OnUpdate();
            }

            if (Tick_ % EvictionFrequency_ == 0) {
                OnEvict();
            }
        }

    private:
        void OnUpdate() {
            ForEachBucket([query = Query_](TActualMap& bucket) {
                for (auto& [key, entry] : bucket) {
                    if (entry.IsUpdated) {
                        entry.IsUpdated = false;
                        continue;
                    }

                    entry.IsUpdated = true;
                    entry.Value = query(key);
                }
            });
        }

        void OnEvict() {
            ForEachBucket([](TActualMap& bucket) {
                TVector<TKey> abandoned;
                for (auto& [key, entry] : bucket) {
                    if (entry.IsReferenced) {
                        entry.IsReferenced = false;
                    } else {
                        abandoned.emplace_back(key);
                    }
                }
                for (const TKey& key : abandoned) {
                    bucket.erase(key);
                }
            });
        }

        template <class Action>
        void ForEachBucket(Action&& action) {
            for (TBucket& bucket : Storage_.Buckets) {
                TBucketGuard guard(bucket.GetMutex());
                action(bucket.GetMap());
            }
        }

        TQuery Query_;
        TStorage Storage_;

        std::atomic<size_t> Tick_ = 0;
        size_t UpdateFrequency_;
        size_t EvictionFrequency_;
    };

    struct TAsyncExpiringCacheConfig {
        TDuration TickPeriod = TDuration::Seconds(5);
        size_t UpdateFrequency = 1;
        size_t EvictionFrequency = 3;
    };

} // namespace NYql
