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
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
            ->AddChild("memory_usage_statistics", IYPathService::FromMethod(
                &TSlotManager::GetMemoryUsageStatistics,
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
                BIND([=, this, this_ = MakeStrong(this)] () {
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

    void GetMemoryUsageStatistics(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TTabletCellMemoryStatistics>> futureStatistics;

        for (const auto& occupant : Occupants()) {
            if (!occupant) {
                continue;
            }

            if (auto occupier = occupant->GetTypedOccupier<ITabletSlot>()) {
                futureStatistics.push_back(occupier->GetMemoryStatistics());
            }
        }

        auto rawStatistics = WaitFor(AllSucceeded(futureStatistics))
            .ValueOrThrow();

        auto summary = CalculateNodeMemoryUsageSummary(rawStatistics);

        // Fill total limits.
        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();
        summary.Total.Dynamic = {
            .Usage = memoryTracker->GetUsed(EMemoryCategory::TabletDynamic),
            .Limit = memoryTracker->GetLimit(EMemoryCategory::TabletDynamic),
        };
        summary.Total.Static.Limit = memoryTracker->GetLimit(EMemoryCategory::TabletStatic);
        summary.Total.RowCache.Limit = memoryTracker->GetLimit(EMemoryCategory::LookupRowsCache);

        // Fill per bundle limits.
        for (auto& [bundleName, bundleStat] : summary.Bundles) {
            bundleStat.Total.Dynamic = {
                .Usage = memoryTracker->GetUsed(EMemoryCategory::TabletDynamic, bundleName),
                .Limit = memoryTracker->GetLimit(EMemoryCategory::TabletDynamic, bundleName),
            };
        }

        BuildNodeMemoryStatisticsYson(summary, consumer);
    }

    void BuildNodeMemoryStatisticsYson(const TNodeMemoryUsageSummary& summary,  IYsonConsumer* consumer) const
    {
        auto buildMemoryStatistics = BIND(&TSlotManager::BuildMemoryStatisticsYson, Unretained(this));

        auto bundleByTable = [&] (const TString& tablePath) {
            auto it = summary.TablePathToBundleName.find(tablePath);
            YT_ASSERT(it != summary.TablePathToBundleName.end());
            return it != summary.TablePathToBundleName.end()
                ? it->second
                : "";
        };

        BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").DoMap(BIND(buildMemoryStatistics, summary.Total))
            .Item("bundles").DoMapFor(
                summary.Bundles,
                [&] (auto fluent, const auto& bundlePair) {
                    fluent
                        .Item(bundlePair.first).DoMap(
                            BIND(&TSlotManager::BuildBundleMemoryStatisticsYson, Unretained(this), bundlePair.second));
                })
            .Item("tables").DoMapFor(
                summary.Tables,
                [&] (auto fluent, const auto& tablePair) {
                    fluent
                        .Item(tablePair.first).BeginMap()
                            .Item("tablet_cell_bundle").Value(bundleByTable(tablePair.first))
                            .Do(BIND(buildMemoryStatistics, tablePair.second))
                        .EndMap();
                })
        .EndMap();
    }

    void BuildBundleMemoryStatisticsYson(const TBundleMemoryUsageSummary& statistics, TFluentMap fluent) const
    {
        auto buildMemoryStatistics = BIND(&TSlotManager::BuildMemoryStatisticsYson, Unretained(this));

        fluent
            .Item("total").DoMap(BIND(buildMemoryStatistics, statistics.Total))
            .Item("cells").DoMapFor(
                statistics.TabletCells,
                [&] (auto fluent, const auto& cellPair) {
                    fluent
                        .Item(ToString(cellPair.first)).DoMap(BIND(buildMemoryStatistics, cellPair.second));
                });
    }

    void BuildMemoryStatisticsYson(const TMemoryStatistics& statistics, TFluentMap fluent) const
    {
        auto buildValue = BIND(&TSlotManager::BuildMemoryStatisticsValueYson, Unretained(this));

        fluent
            .Item("tablet_dynamic").BeginMap()
                .Item("active").Value(statistics.DynamicActive)
                .Item("passive").Value(statistics.DynamicPassive)
                .Item("backing").Value(statistics.DynamicBacking)
                .Do(BIND(&TSlotManager::BuildMemoryStatisticsValueYson, Unretained(this), statistics.Dynamic))
            .EndMap()
            .Item("tablet_static").DoMap(BIND(buildValue, statistics.Static))
            .Item("row_cache").DoMap(BIND(buildValue, statistics.RowCache))
            .Item("preload_store_count").Value(statistics.PreloadStoreCount)
            .Item("preload_pending_store_count").Value(statistics.PreloadPendingStoreCount)
            .Item("preload_pending_bytes").Value(statistics.PreloadPendingBytes)
            .Item("preload_failed_store_count").Value(statistics.PreloadFailedStoreCount)
            .Item("preload_errors").Value(statistics.PreloadErrors);
    }

    void BuildMemoryStatisticsValueYson(const std::optional<TMemoryStatistics::TValue>& value, TFluentMap fluent) const
    {
        if (!value) {
            return;
        }

        fluent
            .Item("usage").Value(value->Usage)
            .DoIf(value->Limit.has_value(), [&] (auto fluent) {
                fluent.Item("limit").Value(*(value->Limit));
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

ISlotManagerPtr CreateSlotManager(IBootstrap* bootstrap)
{
    return New<TSlotManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
