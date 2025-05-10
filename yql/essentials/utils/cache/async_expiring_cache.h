#pragma once

#include <concepts>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <util/datetime/base.h>

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

    struct TAsyncExpiringCacheConfig {
        TDuration TickPeriod = TDuration::Seconds(5);
        size_t UpdateFrequency = 1;
        size_t EvictionFrequency = 3;
    };

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
        using TQuery = std::function<NThreading::TFuture<TVector<TValue>>(const TVector<TKey>&)>;

        TAsyncExpiringCache(TAsyncExpiringCacheConfig config, TQuery query)
            : Query_(std::move(query))
            , UpdateFrequency_(config.UpdateFrequency)
            , EvictionFrequency_(config.EvictionFrequency)
        {
            Y_ENSURE(0 < UpdateFrequency_);
            Y_ENSURE(UpdateFrequency_ <= EvictionFrequency_);
            Y_ENSURE(EvictionFrequency_ <= 1'000);
        }

        NThreading::TFuture<TValue> Get(const TKey& key) {
            TBucket& bucket = Storage_.GetBucketForKey(key);
            TBucketGuard guard(bucket.GetMutex());

            if (TEntry* entry = bucket.TryGetUnsafe(key)) {
                entry->IsReferenced = true;
                return entry->Value;
            }

            TEntry& entry = bucket.GetMap()[key];

            entry.Value = Query_({key}).Apply([](auto f) {
                TVector<TValue> values = f.ExtractValue();
                Y_ENSURE(values.size() == 1);
                return std::move(values[0]);
            });

            entry.IsReferenced = true;
            entry.IsUpdated = true;

            return entry.Value;
        }

        void OnTick() {
            Tick_.fetch_add(1);
            if (Tick_ % EvictionFrequency_ == 0) {
                OnEvict();
            }
            if (Tick_ % UpdateFrequency_ == 0) {
                OnUpdate();
            }
        }

    private:
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

        auto OnQueryFinished(TVector<TKey> keys, THashMap<std::uintptr_t, TVector<size_t>> indeciesByBuckets) {
            return [keys = std::move(keys), buckets = std::move(indeciesByBuckets)](auto f) {
                TVector<TValue> values = f.ExtractValue();
                Y_ENSURE(keys.size() == values.size());
                Y_ENSURE(keys.size() == buckets.size());

                for (auto& [bucketPtr, indecies] : buckets) {
                    TBucket& bucket = *reinterpret_cast<TBucket*>(bucketPtr);
                    TBucketGuard guard(bucket.GetMutex());

                    TActualMap& map = bucket.GetMap();
                    for (size_t i : indecies) {
                        TEntry& entry = map[keys[i]];
                        entry.Value = NThreading::MakeFuture(std::move(values[i]));
                        entry.IsUpdated = true;
                    }
                }
            };
        }

        void OnUpdate() {
            TVector<TKey> outdatedKeys;
            THashMap<std::uintptr_t, TVector<size_t>> indeciesByBuckets;

            ForEachBucket([&](TActualMap& bucket) {
                for (auto& [key, entry] : bucket) {
                    if (entry.IsUpdated) {
                        entry.IsUpdated = false;
                    } else {
                        indeciesByBuckets[reinterpret_cast<std::uintptr_t>(&bucket)]
                            .emplace_back(outdatedKeys.size());
                        outdatedKeys.emplace_back(key);
                    }
                }
            });

            if (outdatedKeys.empty()) {
                return;
            }

            Query_(outdatedKeys)
                .Apply(OnQueryFinished(
                    std::move(outdatedKeys),
                    std::move(indeciesByBuckets)))
                .Wait();
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

} // namespace NYql
