#include "automaton.h"
#include "slot_manager.h"
#include "private.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "security_manager.h"
#include "structured_logger.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>
#include <yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/server/node/data_node/config.h>
#include <yt/server/node/data_node/legacy_master_connector.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/virtual.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NDataNode;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TSlotManager
    : public ISlotManager
{
public:
    explicit TSlotManager(NClusterNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TSlotManager::OnScanSlots, Unretained(this)),
            Config_->SlotScanPeriod))
        , OrchidService_(TOrchidService::Create(
            MakeWeak(this),
            Bootstrap_->GetControlInvoker()))
    { }

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SetTotalTabletSlotCount(Config_->ResourceLimits->Slots);
        SlotScanExecutor_->Start();

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TSlotManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual int GetTotalTabletSlotCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return IndexToSlot_.size();
    }

    virtual int GetAvailableTabletSlotCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int availableSlotCount = 0;
        for (const auto& slot : IndexToSlot_) {
            if (!slot) {
                ++availableSlotCount;
            }
        }

        return availableSlotCount;
    }

    virtual int GetUsedTabletSlotCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return IndexToSlot_.size() - GetAvailableTabletSlotCount();
    }

    virtual bool HasFreeTabletSlots() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return GetAvailableTabletSlotCount() > 0;
    }

    virtual bool IsOutOfMemory(const std::optional<TString>& poolTag) const override
    {
        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        return tracker->IsExceeded(EMemoryCategory::TabletDynamic, poolTag);
    }

    virtual double GetUsedCpu(double cpuPerTabletSlot) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        double result = 0;
        for (const auto& slot : IndexToSlot_) {
            if (slot) {
                result += slot->GetUsedCpu(cpuPerTabletSlot);
            }
        }
        return result;
    }

    virtual const std::vector<TTabletSlotPtr>& Slots() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return IndexToSlot_;
    }

    virtual TTabletSlotPtr FindSlot(TCellId id) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(CellIdToSlotLock_);
        auto it = CellIdToSlot_.find(id);
        return it == CellIdToSlot_.end() ? nullptr : it->second;
    }


    virtual void CreateSlot(const TCreateTabletSlotInfo& createInfo) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int index = GetFreeSlotIndex();

        auto slot = New<TTabletSlot>(index, Config_, createInfo, Bootstrap_);
        slot->Initialize();

        IndexToSlot_[index] = slot;

        {
            auto guard = WriterGuard(CellIdToSlotLock_);
            YT_VERIFY(CellIdToSlot_.emplace(slot->GetCellId(), slot).second);
        }

        UpdateTabletCellBundleMemoryPoolWeight(createInfo.tablet_cell_bundle());
    }

    virtual void ConfigureSlot(
        const TTabletSlotPtr& slot,
        const TConfigureTabletSlotInfo& configureInfo) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        slot->Configure(configureInfo);
    }

    virtual TFuture<void> RemoveSlot(const TTabletSlotPtr& slot) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return slot->Finalize()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TError&) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                if (IndexToSlot_[slot->GetIndex()] == slot) {
                    IndexToSlot_[slot->GetIndex()].Reset();
                }

                {
                    auto guard = WriterGuard(CellIdToSlotLock_);
                    if (auto it = CellIdToSlot_.find(slot->GetCellId()); it && it->second == slot) {
                        CellIdToSlot_.erase(it);
                    }
                }

                UpdateTabletCellBundleMemoryPoolWeight(slot->GetTabletCellBundleName());
            }).Via(Bootstrap_->GetControlInvoker()));
    }


    virtual std::vector<TTabletSnapshotPtr> GetTabletSnapshots() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        std::vector<TTabletSnapshotPtr> snapshots;
        snapshots.reserve(TabletIdToSnapshot_.size());
        for (const auto& [tabletId, snapshot] : TabletIdToSnapshot_) {
            snapshots.push_back(snapshot);
        }
        return snapshots;
    }

    virtual TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoFindTabletSnapshot(tabletId, std::nullopt);
    }

    virtual TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindLatestTabletSnapshot(tabletId);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        return snapshot;
    }

    virtual TTabletSnapshotPtr FindTabletSnapshot(TTabletId tabletId, TRevision mountRevision) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        return snapshot && snapshot->MountRevision == mountRevision
            ? snapshot
            : nullptr;
    }

    virtual TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId,
        TRevision mountRevision) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        snapshot->ValidateMountRevision(mountRevision);
        return snapshot;
    }

    virtual void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTimestamp timestamp) override
    {
        if (timestamp != AsyncLastCommittedTimestamp) {
            const auto& hydraManager = tabletSnapshot->HydraManager;
            if (!hydraManager->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }
        }
    }

    virtual void RegisterTabletSnapshot(
        const TTabletSlotPtr& slot,
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto newSnapshot = tablet->BuildSnapshot(slot, epoch);

        {
            auto guard = WriterGuard(TabletSnapshotsSpinLock_);
            auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
            for (auto it = range.first; it != range.second; ++it) {
                auto& snapshot = it->second;
                if (snapshot->CellId == slot->GetCellId()) {
                    auto deadSnapshot = std::move(snapshot);
                    snapshot = std::move(newSnapshot);
                    guard.Release();
                    // This is where deadSnapshot dies. It's also nice to have logging moved outside
                    // of a critical section.
                    YT_LOG_DEBUG("Tablet snapshot updated (TabletId: %v, CellId: %v)",
                        tablet->GetId(),
                        slot->GetCellId());
                    return;
                }
            }
            TabletIdToSnapshot_.emplace(tablet->GetId(), newSnapshot);
        }

        YT_LOG_DEBUG("Tablet snapshot registered (TabletId: %v, CellId: %v)",
            tablet->GetId(),
            slot->GetCellId());
    }

    virtual void UnregisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
        for (auto it = range.first; it != range.second; ++it) {
            auto snapshot = it->second;
            if (snapshot->CellId == slot->GetCellId()) {
                guard.Release();

                YT_LOG_DEBUG("Tablet snapshot unregistered; eviction scheduled (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    slot->GetCellId());

                TDelayedExecutor::Submit(
                    BIND(&TSlotManager::EvictTabletSnapshot, MakeStrong(this), tablet->GetId(), snapshot)
                        .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()),
                    Config_->TabletSnapshotEvictionTimeout);

                break;
            }
        }
        // NB: It's fine not to find anything.
    }

    virtual void UnregisterTabletSnapshots(TTabletSlotPtr slot) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TTabletSnapshotPtr> deadSnapshots;

        {
            auto guard = WriterGuard(TabletSnapshotsSpinLock_);
            for (auto it = TabletIdToSnapshot_.begin(); it != TabletIdToSnapshot_.end();) {
                auto jt = it++;
                auto& snapshot = jt->second;
                if (snapshot->CellId == slot->GetCellId()) {
                    deadSnapshots.emplace_back(std::move(snapshot));
                    TabletIdToSnapshot_.erase(jt);
                }
            }
            // NB: It's fine not to find anything.
        }

        // This is where deadSnapshots die. It's also nice to have logging moved outside
        // of a critical section.
        for (const auto& snapshot : deadSnapshots) {
            YT_LOG_DEBUG("Tablet snapshot unregistered (TabletId: %v, CellId: %v)",
                snapshot->TabletId,
                snapshot->CellId);
        }
    }

    virtual void UpdateTabletCellBundleMemoryPoolWeight(const TString& bundleName) override
    {
        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();

        i64 totalWeight = 0;
        for (const auto& slot : IndexToSlot_) {
            if (slot && slot->GetTabletCellBundleName() == bundleName) {
                totalWeight += slot->GetDynamicOptions()->DynamicMemoryPoolWeight;
            }
        }

        YT_LOG_DEBUG("Tablet cell bundle memory pool weight updated (Bundle: %v, Weight: %v)",
            bundleName,
            totalWeight);

        memoryTracker->SetPoolWeight(bundleName, totalWeight);
    }

    virtual void PopulateAlerts(std::vector<TError>* /*alerts*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }


    virtual IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    virtual void ValidateCellSnapshot(IAsyncZeroCopyInputStreamPtr reader) override
    {
        if (Slots().empty()) {
            THROW_ERROR_EXCEPTION("No tablet slots in node config");
        }

        // We create fake tablet slot here populating descriptors with the least amount
        // of data such that configuration succeeds.
        {
            auto options = New<TTabletCellOptions>();
            options->SnapshotAccount = "a";
            options->ChangelogAccount = "a";

            NTabletClient::NProto::TCreateTabletSlotInfo protoInfo;
            ToProto(protoInfo.mutable_cell_id(), TGuid{});
            protoInfo.set_peer_id(0);
            protoInfo.set_options(ConvertToYsonString(*options).ToString());
            protoInfo.set_tablet_cell_bundle("b");

            CreateSlot(protoInfo);
        }

        {
            TCellDescriptor cellDescriptor;
            cellDescriptor.CellId = TGuid{};
            cellDescriptor.ConfigVersion = 0;
            TCellPeerDescriptor peerDescriptor;
            peerDescriptor.SetVoting(true);
            cellDescriptor.Peers = {peerDescriptor};
            // Object type should match EObjectType::Transaction, which is 1.
            TGuid prerequisiteTransactionId(1, 1, 1, 1);

            NTabletClient::NProto::TConfigureTabletSlotInfo protoInfo;
            ToProto(protoInfo.mutable_cell_descriptor(), cellDescriptor);
            ToProto(protoInfo.mutable_prerequisite_transaction_id(), prerequisiteTransactionId);

            const auto& slot = Slots()[0];
            YT_VERIFY(slot);
            ConfigureSlot(slot, protoInfo);
        }

        const auto& slot = Slots()[0];
        BIND([&] {
            const auto& hydraManager = slot->GetHydraManager();
            hydraManager->ValidateSnapshot(reader);
        })
            .AsyncVia(slot->GetAutomatonInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }


    DEFINE_SIGNAL(void(), BeginSlotScan);
    DEFINE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DEFINE_SIGNAL(void(), EndSlotScan);

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TSlotManager> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        virtual std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& slot : owner->Slots()) {
                    if (keys.size() >= limit) {
                        break;
                    }
                    if (slot) {
                        keys.push_back(ToString(slot->GetCellId()));
                    }
                }
            }
            return keys;
        }

        virtual i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->GetUsedTabletSlotCount();
            }
            return 0;
        }

        virtual IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto slot = owner->FindSlot(TCellId::FromString(key))) {
                    return slot->GetOrchidService();
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TSlotManager> Owner_;

        explicit TOrchidService(TWeakPtr<TSlotManager> owner)
            : Owner_(std::move(owner))
        { }

        DECLARE_NEW_FRIEND();
    };

    NClusterNode::TBootstrap* const Bootstrap_;
    const TTabletNodeConfigPtr Config_;

    std::vector<TTabletSlotPtr> IndexToSlot_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, CellIdToSlotLock_);
    THashMap<TCellId, TTabletSlotPtr> CellIdToSlot_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, TabletSnapshotsSpinLock_);
    THashMultiMap<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;

    IYPathServicePtr OrchidService_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    TTabletSnapshotPtr DoFindTabletSnapshot(TTabletId tabletId, std::optional<TRevision> mountRevision)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TTabletSnapshotPtr snapshot;

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tabletId);

        // NB: It is uncommon but possible to have multiple cells pretending to serve the same tablet.
        // Tie breaking order is:
        //  - if there is an instance with matching mount revision, pick it;
        //  - if there are active instances, pick one of them with maximum mount revision;
        //  - otherwise pick the instance with maximum mount revision.
        // Outdated snapshots are useful for AsyncLastCommitted reads and ReadDynamicStore requests.

        auto getComparisonSurrogate = [] (const TTabletSnapshotPtr& snapshot) {
            return std::make_pair(snapshot->HydraManager->IsActive(), snapshot->MountRevision);
        };

        for (auto it = range.first; it != range.second; ++it) {
            if (mountRevision && it->second->MountRevision == mountRevision) {
                return it->second;
            }

            if (!snapshot || getComparisonSurrogate(snapshot) < getComparisonSurrogate(it->second)) {
                snapshot = it->second;
            }
        }

        return snapshot;
    }

    void EvictTabletSnapshot(TTabletId tabletId, const TTabletSnapshotPtr& snapshot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(TabletSnapshotsSpinLock_);

        auto range = TabletIdToSnapshot_.equal_range(tabletId);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == snapshot) {
                TabletIdToSnapshot_.erase(it);
                guard.Release();

                // This is where snapshot dies. It's also nice to have logging moved outside
                // of a critical section.
                YT_LOG_DEBUG("Tablet snapshot evicted (TabletId: %v, CellId: %v)",
                    tabletId,
                    snapshot->CellId);
                break;
            }
        }
    }

    void ThrowOnMissingTabletSnapshot(
        TTabletId tabletId,
        TCellId cellId,
        const TTabletSnapshotPtr& snapshot)
    {
        if (snapshot) {
            return;
        }

        if (!cellId) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }

        auto slot = FindSlot(cellId);
        if (!slot){
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchCell,
                "Cell %v is not known",
                cellId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        }

        auto hydraManager = slot->GetHydraManager();
        if (hydraManager && !hydraManager->IsActive()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchCell,
                "Cell %v is not active")
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        } else {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        }
    }


    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Slot scan started");

        Bootstrap_->GetTabletNodeStructuredLogger()->LogEvent("begin_slot_scan");

        BeginSlotScan_.Fire();

        std::vector<TFuture<void>> asyncResults;
        for (const auto& slot : IndexToSlot_) {
            if (!slot) {
                continue;
            }

            asyncResults.push_back(
                BIND([=, this_ = MakeStrong(this)] () {
                    ScanSlot_.Fire(slot);
                })
                .AsyncVia(slot->GetGuardedAutomatonInvoker())
                .Run()
                // Silent any error to avoid premature return from WaitFor.
                .Apply(BIND([] (const TError&) { })));
        }
        auto result = WaitFor(AllSucceeded(asyncResults));
        YT_VERIFY(result.IsOK());

        EndSlotScan_.Fire();

        Bootstrap_->GetTabletNodeStructuredLogger()->LogEvent("end_slot_scan");

        YT_LOG_DEBUG("Slot scan completed");
    }


    void SetTotalTabletSlotCount(int slotCount)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IndexToSlot_.size() == slotCount) {
            return;
        }

        YT_LOG_INFO("Updating tablet slot count (OldTabletSlotCount: %v, NewTabletSlotCount: %v)",
            IndexToSlot_.size(),
            slotCount);

        if (slotCount < IndexToSlot_.size()) {
            std::vector<TFuture<void>> futures;
            for (int slotIndex = slotCount; slotIndex < IndexToSlot_.size(); ++slotIndex) {
                if (const auto& slot = IndexToSlot_[slotIndex]) {
                    futures.push_back(RemoveSlot(slot));
                }
            }

            auto error = WaitFor(AllSet(std::move(futures)));
            YT_LOG_ALERT_UNLESS(error.IsOK(), error, "Failed to finalize tablet slot during slot manager reconfiguration");
        }

        while (IndexToSlot_.size() > slotCount) {
            if (const auto& slot = IndexToSlot_.back()) {
                THROW_ERROR_EXCEPTION("Slot %v with cell %d did not finalize properly, total slot count update failed",
                    slot->GetIndex(),
                    slot->GetCellId());
            }
            IndexToSlot_.pop_back();
        }
        IndexToSlot_.resize(slotCount);

        if (slotCount > 0) {
            // Requesting latest timestamp enables periodic background time synchronization.
            // For tablet nodes, it is crucial because of non-atomic transactions that require
            // in-sync time for clients.
            Bootstrap_
                ->GetMasterConnection()
                ->GetTimestampProvider()
                ->GetLatestTimestamp();
        }
    }

    int GetFreeSlotIndex()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (int index = 0; index < IndexToSlot_.size(); ++index) {
            if (!IndexToSlot_[index]) {
                return index;
            }
        }
        YT_ABORT();
    }


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Update tablet slot count.
        auto tabletSlotCount = newConfig->TabletNode->Slots.value_or(Config_->ResourceLimits->Slots);
        SetTotalTabletSlotCount(tabletSlotCount);
    }
};

ISlotManagerPtr CreateSlotManager(NClusterNode::TBootstrap* bootstrap)
{
    return New<TSlotManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
