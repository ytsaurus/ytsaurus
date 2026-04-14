#include "hedging_manager_registry.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/adaptive_hedging_manager.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/digest/multi.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NProfiling;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ExpiredRegistryEvictionPeriod = TDuration::Minutes(1);
static constexpr auto StatisticsCollectionPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

bool THedgingUnit::operator==(const THedgingUnit& other) const
{
    return
        UserTag == other.UserTag &&
        HunkChunk == other.HunkChunk;
}

THedgingUnit::operator size_t() const
{
    return MultiHash(
        UserTag,
        HunkChunk);
}

////////////////////////////////////////////////////////////////////////////////

class TTabletHedgingManagerRegistry
    : public ITabletHedgingManagerRegistry
{
public:
    TTabletHedgingManagerRegistry(
        TAdaptiveHedgingManagerConfigPtr storeChunkConfig,
        TAdaptiveHedgingManagerConfigPtr hunkChunkConfig,
        TProfiler profiler)
        : StoreChunkConfig_(std::move(storeChunkConfig))
        , HunkChunkConfig_(std::move(hunkChunkConfig))
        , Profiler_(std::move(profiler))
    { }

    IAdaptiveHedgingManagerPtr GetOrCreateHedgingManager(const THedgingUnit& hedgingUnit) override
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = HedgingUnitToHedgingManagerWithSensors_.find(hedgingUnit);
            if (it != HedgingUnitToHedgingManagerWithSensors_.end()) {
                return it->second.HedgingManager;
            }
        }

        auto hedgingManagerWithSensors = DoCreateHedgingManagerWithSensors(hedgingUnit);

        {
            auto guard = WriterGuard(SpinLock_);
            auto [it, _] = HedgingUnitToHedgingManagerWithSensors_.try_emplace(hedgingUnit, hedgingManagerWithSensors);
            return it->second.HedgingManager;
        }
    }

    void CollectStatistics() override
    {
        auto guard = ReaderGuard(SpinLock_);
        for (const auto& [hedgingUnit, hedgingManagerWithSensors] : HedgingUnitToHedgingManagerWithSensors_) {
            if (hedgingManagerWithSensors.HedgingManager) {
                auto statistics = hedgingManagerWithSensors.HedgingManager->CollectStatistics();
                hedgingManagerWithSensors.PrimaryRequestCount.Increment(statistics.PrimaryRequestCount);
                hedgingManagerWithSensors.SecondaryRequestCount.Increment(statistics.SecondaryRequestCount);
                hedgingManagerWithSensors.QueuedRequestCount.Increment(statistics.QueuedRequestCount);
                hedgingManagerWithSensors.MaxQueueSize.Update(statistics.MaxQueueSize);
                hedgingManagerWithSensors.HedgingDelay.Update(statistics.HedgingDelay);
            }
        }
    }

private:
    struct THedgingManagerWithSensors
    {
        IAdaptiveHedgingManagerPtr HedgingManager;

        NProfiling::TCounter PrimaryRequestCount;
        NProfiling::TCounter SecondaryRequestCount;
        NProfiling::TCounter QueuedRequestCount;
        NProfiling::TGauge MaxQueueSize;
        NProfiling::TTimeGauge HedgingDelay;
    };

    const TAdaptiveHedgingManagerConfigPtr StoreChunkConfig_;
    const TAdaptiveHedgingManagerConfigPtr HunkChunkConfig_;
    const TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<THedgingUnit, THedgingManagerWithSensors> HedgingUnitToHedgingManagerWithSensors_;


    THedgingManagerWithSensors DoCreateHedgingManagerWithSensors(const THedgingUnit& hedgingUnit)
    {
        const auto& config = hedgingUnit.HunkChunk
            ? HunkChunkConfig_
            : StoreChunkConfig_;

        if (!config->SecondaryRequestRatio) {
            return THedgingManagerWithSensors{};
        }

        auto customizedProfiler = Profiler_;
        if (hedgingUnit.HunkChunk) {
            customizedProfiler = customizedProfiler.WithPrefix("/hunks");
        }
        customizedProfiler = customizedProfiler.WithPrefix("/hedging_manager");
        if (hedgingUnit.UserTag) {
            customizedProfiler = customizedProfiler.WithTag("user", *hedgingUnit.UserTag);
        }

        return THedgingManagerWithSensors{
            .HedgingManager = CreateAdaptiveHedgingManager(std::move(config)),

            .PrimaryRequestCount = customizedProfiler.Counter("/primary_request_count"),
            .SecondaryRequestCount = customizedProfiler.Counter("/secondary_request_count"),
            .QueuedRequestCount = customizedProfiler.Counter("/queued_request_count"),
            .MaxQueueSize = customizedProfiler.Gauge("/max_queue_size"),
            .HedgingDelay = customizedProfiler.TimeGauge("/hedging_delay"),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletHedgingManagerRegistryPtr CreateTabletHedgingManagerRegistry(
    TAdaptiveHedgingManagerConfigPtr storeChunkConfig,
    TAdaptiveHedgingManagerConfigPtr hunkChunkConfig,
    TProfiler profiler)
{
    return New<TTabletHedgingManagerRegistry>(
        std::move(storeChunkConfig),
        std::move(hunkChunkConfig),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

class THedgingManagerRegistry
    : public IHedgingManagerRegistry
{
public:
    explicit THedgingManagerRegistry(IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
    {
        ScheduleDeleteExpiredRegistries();
        ScheduleStatisticsCollection();
    }

    ITabletHedgingManagerRegistryPtr GetOrCreateTabletHedgingManagerRegistry(
        TTableId tableId,
        const TAdaptiveHedgingManagerConfigPtr& storeChunkConfig,
        const TAdaptiveHedgingManagerConfigPtr& hunkChunkConfig,
        const TProfiler& profiler) override
    {
        THedgingManagerRegistryKey key{
            .TableId = tableId,
            .StoreChunkConfig = ConvertToYsonString(storeChunkConfig),
            .HunkChunkConfig = ConvertToYsonString(hunkChunkConfig),
        };

        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = KeyToRegistry_.find(key);
            if (it != KeyToRegistry_.end()) {
                if (auto hedgingManagerRegistry = it->second.Lock()) {
                    return hedgingManagerRegistry;
                }
            }
        }

        auto hedgingManagerRegistry = CreateTabletHedgingManagerRegistry(
            storeChunkConfig,
            hunkChunkConfig,
            profiler);

        {
            auto guard = WriterGuard(SpinLock_);
            auto it = KeyToRegistry_.find(key);
            if (it != KeyToRegistry_.end()) {
                if (auto hedgingManagerRegistry = it->second.Lock()) {
                    return hedgingManagerRegistry;
                }
                KeyToRegistry_.erase(it);
            }
            EmplaceOrCrash(KeyToRegistry_, key, hedgingManagerRegistry);

            return hedgingManagerRegistry;
        }
    }

private:
    const IInvokerPtr Invoker_;

    struct THedgingManagerRegistryKey
    {
        TTableId TableId;
        TYsonString StoreChunkConfig;
        TYsonString HunkChunkConfig;

        operator size_t() const
        {
            return MultiHash(
                TableId,
                StoreChunkConfig,
                HunkChunkConfig);
        }
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<THedgingManagerRegistryKey, TWeakPtr<ITabletHedgingManagerRegistry>> KeyToRegistry_;


    void ScheduleDeleteExpiredRegistries()
    {
        TDelayedExecutor::Submit(
            BIND(&THedgingManagerRegistry::DeleteExpiredRegistries,
                MakeWeak(this))
            .Via(Invoker_),
            ExpiredRegistryEvictionPeriod);
    }

    void ScheduleStatisticsCollection()
    {
        TDelayedExecutor::Submit(
            BIND(&THedgingManagerRegistry::CollectStatistics,
                MakeWeak(this))
            .Via(Invoker_),
            StatisticsCollectionPeriod);
    }

    void DeleteExpiredRegistries()
    {
        std::vector<THedgingManagerRegistryKey> expiredKeys;

        {
            auto guard = ReaderGuard(SpinLock_);
            for (const auto& [key, weakRegistry] : KeyToRegistry_) {
                if (weakRegistry.IsExpired()) {
                    expiredKeys.push_back(key);
                }
            }
        }

        if (!expiredKeys.empty()) {
            auto guard = WriterGuard(SpinLock_);
            for (const auto& expiredKey : expiredKeys) {
                auto it = KeyToRegistry_.find(expiredKey);
                if (it != KeyToRegistry_.end() && it->second.IsExpired()) {
                    KeyToRegistry_.erase(it);
                }
            }
        }

        ScheduleDeleteExpiredRegistries();
    }

    void CollectStatistics()
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            std::vector<ITabletHedgingManagerRegistryPtr> registries;
            for (const auto& [key, weakRegistry] : KeyToRegistry_) {
                if (auto tabletRegistry = weakRegistry.Lock()) {
                    tabletRegistry->CollectStatistics();
                }
            }
        }

        ScheduleStatisticsCollection();
    }
};

////////////////////////////////////////////////////////////////////////////////

IHedgingManagerRegistryPtr CreateHedgingManagerRegistry(IInvokerPtr invoker)
{
    return New<THedgingManagerRegistry>(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
