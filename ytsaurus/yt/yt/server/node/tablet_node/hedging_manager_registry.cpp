#include "hedging_manager_registry.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/hedging_manager.h>
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

////////////////////////////////////////////////////////////////////////////////

bool THedgingUnit::operator == (const THedgingUnit& other) const
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

    IHedgingManagerPtr GetOrCreateHedgingManager(const THedgingUnit& hedgingUnit) override
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = HedgingUnitToHedgingManager_.find(hedgingUnit);
            if (it != HedgingUnitToHedgingManager_.end()) {
                return it->second;
            }
        }

        auto hedgingManager = DoCreateHedgingManager(hedgingUnit);

        {
            auto guard = WriterGuard(SpinLock_);
            auto [it, _] = HedgingUnitToHedgingManager_.try_emplace(hedgingUnit, hedgingManager);
            return it->second;
        }
    }

private:
    const TAdaptiveHedgingManagerConfigPtr StoreChunkConfig_;
    const TAdaptiveHedgingManagerConfigPtr HunkChunkConfig_;
    const TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<THedgingUnit, IHedgingManagerPtr> HedgingUnitToHedgingManager_;


    IHedgingManagerPtr DoCreateHedgingManager(const THedgingUnit& hedgingUnit)
    {
        const auto& config = hedgingUnit.HunkChunk
            ? HunkChunkConfig_
            : StoreChunkConfig_;

        if (!config->MaxBackupRequestRatio) {
            return nullptr;
        }

        auto customizedProfiler = Profiler_;
        if (hedgingUnit.HunkChunk) {
            customizedProfiler = customizedProfiler.WithPrefix("/hunks");
        }
        customizedProfiler = customizedProfiler.WithPrefix("/hedging_manager");
        if (hedgingUnit.UserTag) {
            customizedProfiler = customizedProfiler.WithTag("user", *hedgingUnit.UserTag);
        }

        return CreateAdaptiveHedgingManager(
            config,
            customizedProfiler);
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
            BIND([weakRegistry = MakeWeak(this)] {
                if (auto registry = weakRegistry.Lock()) {
                    registry->DeleteExpiredRegistries();
                }
            })
            .Via(Invoker_),
            ExpiredRegistryEvictionPeriod);
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
};

////////////////////////////////////////////////////////////////////////////////

IHedgingManagerRegistryPtr CreateHedgingManagerRegistry(IInvokerPtr invoker)
{
    return New<THedgingManagerRegistry>(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
