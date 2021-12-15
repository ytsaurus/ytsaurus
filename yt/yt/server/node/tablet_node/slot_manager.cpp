#include "slot_manager.h"

#include "bootstrap.h"
#include "private.h"
#include "slot_provider.h"
#include "tablet_slot.h"
#include "structured_logger.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TSlotManager
    : public ISlotManager
{
public:
    explicit TSlotManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TSlotManager::OnScanSlots, Unretained(this)),
            Config_->SlotScanPeriod))
        , OrchidService_(CreateOrchidService())
    { }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto cellar = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet);
        cellar->RegisterOccupierProvider(CreateTabletSlotOccupierProvider(Config_, Bootstrap_));

        cellar->SubscribeCreateOccupant(BIND(&TSlotManager::UpdateMemoryPoolWeights, MakeWeak(this)));
        cellar->SubscribeRemoveOccupant(BIND(&TSlotManager::UpdateMemoryPoolWeights, MakeWeak(this)));
        cellar->SubscribeUpdateOccupant(BIND(&TSlotManager::UpdateMemoryPoolWeights, MakeWeak(this)));

        SlotScanExecutor_->Start();
    }

    bool IsOutOfMemory(const std::optional<TString>& poolTag) const override
    {
        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        return tracker->IsExceeded(EMemoryCategory::TabletDynamic, poolTag);
    }

    double GetUsedCpu(double cpuPerTabletSlot) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        double result = 0;
        for (const auto& occupant : Occupants()) {
            if (!occupant) {
                continue;
            }

            if (auto occupier = occupant->GetTypedOccupier<ITabletSlot>()) {
                result += occupier->GetUsedCpu(cpuPerTabletSlot);
            }
        }

        return result;
    }

    ITabletSlotPtr FindSlot(NHydra::TCellId id) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto occupant = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet)->FindOccupant(id)) {
            return occupant->GetTypedOccupier<ITabletSlot>();
        }
        return nullptr;
    }

    const IYPathServicePtr& GetOrchidService() const override
    {
        return OrchidService_;
    }

    DEFINE_SIGNAL_OVERRIDE(void(), BeginSlotScan);
    DEFINE_SIGNAL_OVERRIDE(void(ITabletSlotPtr), ScanSlot);
    DEFINE_SIGNAL_OVERRIDE(void(), EndSlotScan);

private:
    IBootstrap* const Bootstrap_;
    const TTabletNodeConfigPtr Config_;
    const TPeriodicExecutorPtr SlotScanExecutor_;
    const IYPathServicePtr OrchidService_;

    using TBundlesMemoryPoolWeights = THashMap<TString, int>;
    TBundlesMemoryPoolWeights BundlesMemoryPoolWeights_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddChild("dynamic_memory_pool_weights", IYPathService::FromMethod(
                &TSlotManager::GetDynamicMemoryPoolWeightsOrchid,
                MakeWeak(this)))
            ->AddChild("memory_usage_stats", IYPathService::FromMethod(
                &TSlotManager::GetMemoryUsageStats,
                MakeWeak(this)));
    }

    void GetDynamicMemoryPoolWeightsOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .DoMapFor(BundlesMemoryPoolWeights_, [] (TFluentMap fluent, const auto& pair) {
                fluent
                    .Item(pair.first).Value(pair.second);
            });
    }

    void UpdateMemoryPoolWeights()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();

        auto update = [&] (const TString& bundleName, int weight) {
            YT_LOG_DEBUG("Tablet cell bundle memory pool weight updated (Bundle: %v, Weight: %v)",
                bundleName,
                weight);
            memoryTracker->SetPoolWeight(bundleName, weight);
        };

        TBundlesMemoryPoolWeights weights;
        for (const auto& occupant : Occupants()) {
            if (occupant) {
                weights[occupant->GetCellBundleName()] += occupant->GetDynamicOptions()->DynamicMemoryPoolWeight;
            }
        }

        for (const auto& [bundle, weight] : weights) {
            if (auto it = BundlesMemoryPoolWeights_.find(bundle); !it || it->second != weight) {
                update(bundle, weight);
            }
        }
        for (const auto& [bundle, _] : BundlesMemoryPoolWeights_) {
            if (!weights.contains(bundle)) {
                update(bundle, 0);
            }
        }

        BundlesMemoryPoolWeights_ = std::move(weights);
    }

    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Slot scan started");

        Bootstrap_->GetStructuredLogger()->LogEvent("begin_slot_scan");

        BeginSlotScan_.Fire();

        std::vector<TFuture<void>> asyncResults;
        for (const auto& occupant : Occupants()) {
            if (!occupant) {
                continue;
            }

            auto occupier = occupant->GetTypedOccupier<ITabletSlot>();
            if (!occupier) {
                continue;
            }

            asyncResults.push_back(
                BIND([=, this_ = MakeStrong(this)] () {
                    ScanSlot_.Fire(occupier);
                })
                .AsyncVia(occupier->GetGuardedAutomatonInvoker())
                .Run()
                // Silent any error to avoid premature return from WaitFor.
                .Apply(BIND([] (const TError&) { })));
        }
        auto result = WaitFor(AllSucceeded(asyncResults));
        YT_VERIFY(result.IsOK());

        EndSlotScan_.Fire();

        Bootstrap_->GetStructuredLogger()->LogEvent("end_slot_scan");

        YT_LOG_DEBUG("Slot scan completed");
    }

    const std::vector<ICellarOccupantPtr>& Occupants() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet)->Occupants();
    }

    void GetMemoryUsageStats(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TTabletCellMemoryStats>> futureStats;

        for (const auto& occupant : Occupants()) {
            if (!occupant) {
                continue;
            }

            if (auto occupier = occupant->GetTypedOccupier<ITabletSlot>()) {
                futureStats.push_back(occupier->GetMemoryStats());
            }
        }

        auto rawStats = WaitFor(AllSucceeded(futureStats))
            .ValueOrThrow();

        auto summary = CalcNodeMemoryUsageSummary(rawStats);

        // Fill overall limits.
        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();
        summary.Overall.Dynamic.Limit = memoryTracker->GetLimit(EMemoryCategory::TabletDynamic);
        summary.Overall.Static.Limit = memoryTracker->GetLimit(EMemoryCategory::TabletStatic);
        summary.Overall.RowCache.Limit = memoryTracker->GetLimit(EMemoryCategory::LookupRowsCache);

        // Fill per bundle limits.
        for (auto& [bundleName, bundleStat] : summary.Bundles) {
            bundleStat.Overall.Dynamic.Limit = memoryTracker->GetLimit(EMemoryCategory::TabletDynamic, bundleName);
            bundleStat.Overall.Static.Limit = memoryTracker->GetLimit(EMemoryCategory::TabletStatic, bundleName);
        }

        BuildNodeMemoryStatsYson(summary, consumer);
    }

    void BuildNodeMemoryStatsYson(const TNodeMemoryUsageSummary& summary,  IYsonConsumer* consumer) const
    {
        auto buildMemoryStats = BIND(&TSlotManager::BuildMemoryStatsYson, Unretained(this));

        BuildYsonFluently(consumer)
        .BeginMap()
            .Item("overall").DoMap(BIND(buildMemoryStats, summary.Overall))
            .Item("bundles").DoMapFor(
                summary.Bundles,
                [&] (auto fluent, const auto& bundlePair) {
                    fluent
                        .Item(bundlePair.first).DoMap(
                            BIND(&TSlotManager::BuildBundleMemoryStatsYson, Unretained(this), bundlePair.second));
                })
             .Item("tables").DoMapFor(
                summary.Tables,
                [&] (auto fluent, const auto& tablePair) {
                    fluent
                        .Item(tablePair.first).DoMap(BIND(buildMemoryStats, tablePair.second));
                })
        .EndMap();
    }

    void BuildBundleMemoryStatsYson(const TBundleMemoryUsageSummary& stats, TFluentMap fluent) const
    {
        auto buildMemoryStats = BIND(&TSlotManager::BuildMemoryStatsYson, Unretained(this));

        fluent
            .Item("overall").DoMap(BIND(buildMemoryStats, stats.Overall))
            .Item("cells").DoMapFor(
                stats.TabletCells,
                [&] (auto fluent, const auto& cellPair) {
                    fluent
                        .Item(ToString(cellPair.first)).DoMap(BIND(buildMemoryStats, cellPair.second));
                });
    }

    void BuildMemoryStatsYson(const TMemoryStats& stats, TFluentMap fluent) const
    {
        auto buildValue = BIND(&TSlotManager::BuildMemoryStatsValueYson, Unretained(this));

        fluent
            .Item("tablet_dynamic").DoMap(BIND(buildValue, stats.Dynamic))
            .Item("tablet_dynamic_backing").DoMap(BIND(buildValue, stats.DynamicBacking))
            .Item("tablet_static").DoMap(BIND(buildValue, stats.Static))
            .Item("row_cache").DoMap(BIND(buildValue, stats.RowCache))
            .Item("preload_store_count").Value(stats.PreloadStoreCount)
            .Item("preload_pending_store_count").Value(stats.PendingStoreCount)
            .Item("preload_pending_bytes").Value(stats.PendingStoreBytes)
            .Item("preload_failed_store_count").Value(stats.PreloadStoreFailedCount)
            .Item("preload_errors").Value(stats.PreloadErrors);
    }

    void BuildMemoryStatsValueYson(const TMemoryStats::TValue& value, TFluentMap fluent) const
    {
        fluent
            .Item("usage").Value(value.Usage)
            .DoIf(value.Limit, [&] (auto fluent) {
                fluent.Item("limit").Value(value.Limit);
            });
    }
};

ISlotManagerPtr CreateSlotManager(IBootstrap* bootstrap)
{
    return New<TSlotManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
