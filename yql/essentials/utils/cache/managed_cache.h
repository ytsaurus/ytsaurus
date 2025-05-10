#pragma once

#include <concepts>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <util/datetime/base.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

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

        void Evict() {
            TGuard guard(TickMutex_);
            OnEvict(guard);
        }

        void Update() {
            TGuard guard(TickMutex_);
            OnUpdate(guard);
        }

    private:
        void OnEvict(const TGuard<TMutex>& /* tickMutex */) {
            ForEachBucketLocked([](TActualMap& bucket) {
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

        void OnUpdate(const TGuard<TMutex>& tickMutex) {
            TVector<TKey> outdated;
            THashMap<std::uintptr_t, TVector<size_t>> indeciesByBuckets;

            ForEachBucketLocked([&](TActualMap& bucket) {
                for (auto& [key, entry] : bucket) {
                    if (entry.IsUpdated) {
                        entry.IsUpdated = false;
                    } else {
                        indeciesByBuckets[reinterpret_cast<std::uintptr_t>(&bucket)]
                            .emplace_back(outdated.size());
                        outdated.emplace_back(key);
                    }
                }
            });

            if (outdated.empty()) {
                return;
            }

            TVector<TValue> values = Query_(outdated).ExtractValueSync();
            ApplyBatchUpdate(
                std::move(outdated),
                std::move(values),
                std::move(indeciesByBuckets),
                tickMutex);
        }

        void ApplyBatchUpdate(
            TVector<TKey> keys,
            TVector<TValue> values,
            THashMap<std::uintptr_t, TVector<size_t>> buckets,
            const TGuard<TMutex>& /* tickMutex */) {
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

    template <NPrivate::CCacheKey TKey, NPrivate::CCacheValue TValue>
    class TManagedCache: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TManagedCache>;

        TManagedCache(
            IThreadFactory* executor,
            TManagedCacheConfig config,
            TManagedCacheStorage<TKey, TValue>::TQuery query)
            : Storage_(new TManagedCacheStorage<TKey, TValue>(std::move(query)))
        {
            auto token = Cancellation_.Token();
            MaintenanceThread_ = executor->Run([storage = Storage_, config, token]() {
                TManagedCacheMaintenance<TKey, TValue>(storage, config).Run(token);
            });
        }

        NThreading::TFuture<TValue> Get(const TKey& key) {
            return Storage_->Get(key);
        }

        ~TManagedCache() {
            Cancellation_.Cancel();
            MaintenanceThread_->Join();
        }

    private:
        TManagedCacheStorage<TKey, TValue>::TPtr Storage_;
        NThreading::TCancellationTokenSource Cancellation_;
        THolder<IThreadFactory::IThread> MaintenanceThread_;
    };

    template <
        NPrivate::CCacheKey TKey,
        NPrivate::CCacheValue TValue,
        class TQuery = TManagedCacheStorage<TKey, TValue>::TQuery>
    TManagedCache<TKey, TValue>::TPtr StartManagedCache(
        IThreadFactory* executor, TManagedCacheConfig config, TQuery query) {
        return new TManagedCache<TKey, TValue>(
            executor, std::move(config), std::move(query));
    }

} // namespace NYql
