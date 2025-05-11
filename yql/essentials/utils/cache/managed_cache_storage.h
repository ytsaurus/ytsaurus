#pragma once

#include "managed_cache_listener.h"

#include <concepts>
#include <functional>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

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

        struct TOutdatedState {
            TVector<TKey> Keys;
            THashMap<std::uintptr_t, TVector<size_t>> IdxByBuckets;
        };

    public:
        using TPtr = TIntrusivePtr<TManagedCacheStorage>;

        // [WARN] The client is responsible to prepare a query with a timeout to
        // keep the liveness property of the Update procedure.
        using TQuery = std::function<NThreading::TFuture<TVector<TValue>>(const TVector<TKey>&)>;

        TManagedCacheStorage(
            TQuery query,
            IManagedCacheListener::TPtr listener = MakeDummyManagedCacheListener())
            : Query_(std::move(query))
            , Listener_(std::move(listener))
        {
        }

        NThreading::TFuture<TValue> Get(const TKey& key) {
            TBucket& bucket = Container_.GetBucketForKey(key);
            TBucketGuard guard(bucket.GetMutex());

            if (TEntry* entry = bucket.TryGetUnsafe(key);
                entry != nullptr && !entry->Value.HasException()) {
                entry->IsReferenced = true;

                Listener_->OnHit();
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

            Listener_->OnMiss();
            return entry.Value;
        }

        void Evict() {
            TGuard guard(TickMutex_);
            ResetReferencedAndEvictAbandoned();
        }

        void Update() {
            TGuard guard(TickMutex_);

            TOutdatedState outdated;
            ResetUpdatedAndCollectOutdated(outdated);

            if (outdated.Keys.empty()) {
                return;
            }

            try {
                TVector<TValue> values = Query_(outdated.Keys).ExtractValueSync();
                UpdateBatch(std::move(outdated), std::move(values));
            } catch (...) {
                std::exception_ptr exception = std::current_exception();
                InvalidateBatch(std::move(outdated), exception);
                throw;
            }
        }

    private:
        void ResetReferencedAndEvictAbandoned() {
            size_t evicted = 0;

            ForEachBucketLocked([&](TActualMap& bucket) {
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

                evicted += abandoned.size();
            });

            Listener_->OnEvict(evicted);
        }

        void ResetUpdatedAndCollectOutdated(TOutdatedState& outdated) {
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
                    outdated.IdxByBuckets[ptr].emplace_back(outdated.Keys.size());
                    outdated.Keys.emplace_back(key);
                }
            });

            Listener_->OnUpdate(outdated.Keys.size());
        }

        void UpdateBatch(TOutdatedState outdated, TVector<TValue> values) {
            Y_ENSURE(outdated.Keys.size() == values.size());
            ForEachEntryLocked(std::move(outdated), [&](size_t i, TEntry& entry) {
                entry.Value = NThreading::MakeFuture(std::move(values[i]));
                entry.IsUpdated = true;
            });
        }

        void InvalidateBatch(TOutdatedState outdated, std::exception_ptr exception) {
            ForEachEntryLocked(std::move(outdated), [&](size_t, TEntry& entry) {
                entry.Value = NThreading::MakeErrorFuture<TValue>(exception);
            });
        }

        template <std::invocable<size_t, TEntry&> Action>
        void ForEachEntryLocked(TOutdatedState outdated, Action&& action) {
            Y_ENSURE(outdated.Keys.size() == outdated.IdxByBuckets.size());
            for (auto& [bucketPtr, indecies] : outdated.IdxByBuckets) {
                TBucket& bucket = *reinterpret_cast<TBucket*>(bucketPtr);
                TBucketGuard guard(bucket.GetMutex());

                TActualMap& map = bucket.GetMap();
                for (size_t i : indecies) {
                    TEntry& entry = map[outdated.Keys[i]];
                    action(i, entry);
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
        IManagedCacheListener::TPtr Listener_;
    };

} // namespace NYql
