#pragma once

#include <concepts>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <util/system/mutex.h>

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
    class TManagedCacheStorage: public TThrRefBase {
    private:
        struct TEntry {
            NThreading::TFuture<TValue> Value;
            bool IsReferenced = true;
            bool IsUpdated = true;
        };

        using TContainer = TConcurrentHashMap<TKey, TEntry>;
        using TBucket = typename TContainer::TBucket;
        using TBucketGuard = typename TContainer::TBucketGuard;
        using TActualMap = typename TContainer::TActualMap;

    public:
        using TPtr = TIntrusivePtr<TManagedCacheStorage>;
        using TQuery = std::function<NThreading::TFuture<TVector<TValue>>(const TVector<TKey>&)>;

        explicit TManagedCacheStorage(TQuery query)
            : Query_(std::move(query))
        {
        }

        NThreading::TFuture<TValue> Get(const TKey& key) {
            TBucket& bucket = Container_.GetBucketForKey(key);
            TBucketGuard guard(bucket.GetMutex());

            if (TEntry* entry = bucket.TryGetUnsafe(key);
                entry != nullptr && !entry->Value.HasException()) {
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

        void Evict() {
            TGuard guard(TickMutex_);
            ResetReferencedAndEvictAbandoned();
        }

        void Update() {
            TGuard guard(TickMutex_);

            TVector<TKey> outdated;
            THashMap<std::uintptr_t, TVector<size_t>> indeciesByBuckets;
            ResetUpdatedAndCollectOutdated(outdated, indeciesByBuckets);

            if (outdated.empty()) {
                return;
            }

            TVector<TValue> values = Query_(outdated).ExtractValueSync();
            ApplyBatchUpdate(
                std::move(outdated),
                std::move(values),
                std::move(indeciesByBuckets));
        }

    private:
        void ResetReferencedAndEvictAbandoned() {
            ForEachBucketLocked([](TActualMap& bucket) {
                TVector<TKey> abandoned;

                for (auto& [key, entry] : bucket) {
                    if (entry.IsReferenced && !entry.Value.HasException()) {
                        entry.IsReferenced = false;
                        continue;
                    }

                    abandoned.emplace_back(key);
                }

                for (const TKey& key : abandoned) {
                    bucket.erase(key);
                }
            });
        }

        void ResetUpdatedAndCollectOutdated(
            TVector<TKey>& outdated,
            THashMap<std::uintptr_t, TVector<size_t>>& indeciesByBuckets) {
            ForEachBucketLocked([&](TActualMap& bucket) {
                for (auto& [key, entry] : bucket) {
                    if (entry.IsUpdated) {
                        entry.IsUpdated = false;
                        continue;
                    }

                    if (entry.Value.HasException()) {
                        continue;
                    }

                    auto ptr = reinterpret_cast<std::uintptr_t>(&bucket);
                    indeciesByBuckets[ptr].emplace_back(outdated.size());
                    outdated.emplace_back(key);
                }
            });
        }

        void ApplyBatchUpdate(
            TVector<TKey> keys,
            TVector<TValue> values,
            THashMap<std::uintptr_t, TVector<size_t>> buckets) {
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
        }

        template <std::invocable<TActualMap&> Action>
        void ForEachBucketLocked(Action&& action) {
            for (TBucket& bucket : Container_.Buckets) {
                TBucketGuard guard(bucket.GetMutex());
                action(bucket.GetMap());
            }
        }

        TQuery Query_;
        TContainer Container_;
        TMutex TickMutex_;
    };

} // namespace NYql
