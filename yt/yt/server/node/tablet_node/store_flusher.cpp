#include "in_memory_manager.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "store_flusher.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTransactionClient;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher
    : public IStoreFlusher
{
public:
    explicit TStoreFlusher(NClusterNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode)
        , ThreadPool_(New<TThreadPool>(Config_->StoreFlusher->ThreadPoolSize, "StoreFlush"))
        , Semaphore_(New<TProfiledAsyncSemaphore>(
            Config_->StoreFlusher->MaxConcurrentFlushes,
            Profiler.Gauge("/running_store_flushes")))
        , MinForcedFlushDataSize_(Config_->StoreFlusher->MinForcedFlushDataSize)
    { }

    virtual void Start() override
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TStoreFlusher::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreFlusher::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreFlusher::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreFlusher::OnEndSlotScan, MakeStrong(this)));
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TTabletNodeConfigPtr Config_;

    const NProfiling::TProfiler Profiler = TabletNodeProfiler.WithPrefix("/store_flusher");

    const TThreadPoolPtr ThreadPool_;
    const TProfiledAsyncSemaphorePtr Semaphore_;

    std::atomic<i64> MinForcedFlushDataSize_;

    NProfiling::TGauge DynamicMemoryUsageActiveCounter_ = Profiler.WithTag("memory_type", "active").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsagePassiveCounter_ = Profiler.WithTag("memory_type", "passive").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsageBackingCounter_ = Profiler.WithTag("memory_type", "backing").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsageOtherCounter_ = Profiler.WithTag("memory_type", "other").Gauge("/dynamic_memory_usage");

    struct TForcedRotationCandidate
    {
        i64 MemoryUsage;
        TTabletId TabletId;
        TRevision MountRevision;
        TString TabletLoggingTag;
        ITabletSlotPtr Slot;
    };

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    i64 PassiveMemoryUsage_;
    i64 ActiveMemoryUsage_;
    i64 BackingMemoryUsage_;

    struct TTabletCellBundleData
    {
        double ForcedRotationMemoryRatio = 0;
        bool EnableForcedRotationBackingMemoryAccounting = true;
        bool EnablePerBundleMemoryLimit = true;
        i64 PassiveMemoryUsage = 0;
        i64 BackingMemoryUsage = 0;
        std::vector<TForcedRotationCandidate> ForcedRotationCandidates;
    };

    THashMap<TString, TTabletCellBundleData> TabletCellBundleData_;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreFlusher;
        ThreadPool_->Configure(config->ThreadPoolSize.value_or(Config_->StoreFlusher->ThreadPoolSize));
        Semaphore_->SetTotal(config->MaxConcurrentFlushes.value_or(Config_->StoreFlusher->MaxConcurrentFlushes));
        MinForcedFlushDataSize_.store(config->MinForcedFlushDataSize.value_or(Config_->StoreFlusher->MinForcedFlushDataSize));
    }

    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, this locking is redundant.
        auto guard = Guard(SpinLock_);
        ActiveMemoryUsage_ = 0;
        PassiveMemoryUsage_ = 0;
        BackingMemoryUsage_ = 0;
        TabletCellBundleData_.clear();
    }

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->StoreFlusher;
        if (!dynamicConfig->Enable) {
            return;
        }

        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        {
            auto guard = Guard(SpinLock_);
            TabletCellBundleData_.emplace(
                slot->GetTabletCellBundleName(),
                TTabletCellBundleData{
                    .ForcedRotationMemoryRatio = slot->GetDynamicOptions()->ForcedRotationMemoryRatio,
                    .EnableForcedRotationBackingMemoryAccounting = slot->GetDynamicOptions()->EnableForcedRotationBackingMemoryAccounting,
                    .EnablePerBundleMemoryLimit = slot->GetDynamicOptions()->EnableTabletDynamicMemoryLimit,
                });
        }

        const auto& tabletManager = slot->GetTabletManager();
        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            ScanTablet(slot, tablet);
        }
    }

    void OnEndSlotScan()
    {
        decltype(TabletCellBundleData_) tabletCellBundles;

        // NB: Strictly speaking, this locking is redundant.
        {
            auto guard = Guard(SpinLock_);
            TabletCellBundleData_.swap(tabletCellBundles);
        }

        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        auto otherUsage = tracker->GetUsed(EMemoryCategory::TabletDynamic) -
            ActiveMemoryUsage_ - PassiveMemoryUsage_ - BackingMemoryUsage_;

        DynamicMemoryUsageActiveCounter_.Update(ActiveMemoryUsage_);
        DynamicMemoryUsagePassiveCounter_.Update(PassiveMemoryUsage_);
        DynamicMemoryUsageBackingCounter_.Update(BackingMemoryUsage_);
        DynamicMemoryUsageOtherCounter_.Update(otherUsage);

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->StoreFlusher;
        bool enableForcedRotationBackingMemoryAccounting =
            dynamicConfig->EnableForcedRotationBackingMemoryAccounting.value_or(
                Config_->EnableForcedRotationBackingMemoryAccounting);
        double forcedRotationMemoryRatio =
            dynamicConfig->ForcedRotationMemoryRatio.value_or(
                Config_->ForcedRotationMemoryRatio);

        for (auto& pair : tabletCellBundles) {
            // NB: Cannot use structured bindings since 'isRotationForced' lambda modifies
            // local variable 'bundleData'.
            const auto& bundleName = pair.first;
            auto& bundleData = pair.second;
            auto& candidates = bundleData.ForcedRotationCandidates;

            // Order candidates by increasing memory usage.
            std::sort(
                candidates. begin(),
                candidates.end(),
                [] (const TForcedRotationCandidate& lhs, const TForcedRotationCandidate& rhs) {
                    return lhs.MemoryUsage < rhs.MemoryUsage;
                });

            const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

            auto bundleMemoryUsed = tracker->GetUsed(EMemoryCategory::TabletDynamic, bundleName);
            auto bundleMemoryLimit = tracker->GetLimit(EMemoryCategory::TabletDynamic, bundleName);

            auto isRotationForced = [&] {
                // Per-bundle memory pressure.
                if (bundleData.EnablePerBundleMemoryLimit) {
                    auto adjustedBundleMemoryUsed = bundleMemoryUsed;
                    adjustedBundleMemoryUsed -= bundleData.PassiveMemoryUsage;
                    if (!bundleData.EnableForcedRotationBackingMemoryAccounting) {
                        adjustedBundleMemoryUsed -= bundleData.BackingMemoryUsage;
                    }
                    if (adjustedBundleMemoryUsed > bundleMemoryLimit * bundleData.ForcedRotationMemoryRatio) {
                        return true;
                    }
                }

                // Global memory pressure.
                auto adjustedGlobalMemoryUsed = tracker->GetUsed(EMemoryCategory::TabletDynamic);
                adjustedGlobalMemoryUsed -= PassiveMemoryUsage_;
                if (!enableForcedRotationBackingMemoryAccounting) {
                    adjustedGlobalMemoryUsed -= BackingMemoryUsage_;
                }
                return adjustedGlobalMemoryUsed > tracker->GetLimit(EMemoryCategory::TabletDynamic) * forcedRotationMemoryRatio;
            };

            // Pick the heaviest candidates until no more rotations are needed.
            while (isRotationForced() && !candidates.empty()) {
                auto candidate = candidates.back();
                candidates.pop_back();

                auto tabletId = candidate.TabletId;
                auto mountRevision = candidate.MountRevision;
                auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tabletId, mountRevision);
                if (!tabletSnapshot) {
                    continue;
                }

                YT_LOG_INFO("Scheduling store rotation due to memory pressure condition (%v, "
                    "GlobalMemory: {TotalUsage: %v, PassiveUsage: %v, BackingUsage: %v, Limit: %v}, "
                    "Bundle: %v, "
                    "BundleMemory: {TotalUsage: %v, PassiveUsage: %v, BackingUsage: %v, Limit: %v}, "
                    "TabletMemoryUsage: %v, ForcedRotationMemoryRatio: %v)",
                    candidate.TabletLoggingTag,
                    tracker->GetUsed(EMemoryCategory::TabletDynamic),
                    PassiveMemoryUsage_,
                    BackingMemoryUsage_,
                    tracker->GetLimit(EMemoryCategory::TabletDynamic),
                    bundleName,
                    bundleMemoryUsed,
                    bundleData.PassiveMemoryUsage,
                    bundleData.BackingMemoryUsage,
                    bundleMemoryLimit,
                    candidate.MemoryUsage,
                    bundleData.ForcedRotationMemoryRatio);

                const auto& slot = candidate.Slot;
                auto invoker = slot->GetGuardedAutomatonInvoker();
                invoker->Invoke(BIND([slot, tabletId] () {
                    const auto& tabletManager = slot->GetTabletManager();
                    auto* tablet = tabletManager->FindTablet(tabletId);
                    if (!tablet) {
                        return;
                    }
                    tabletManager->ScheduleStoreRotation(tablet);
                }));

                PassiveMemoryUsage_ += candidate.MemoryUsage;
                bundleData.PassiveMemoryUsage += candidate.MemoryUsage;
            }
        }
    }

    void ScanTablet(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        ScanTabletForRotation(slot, tablet);
        ScanTabletForFlush(slot, tablet);
        ScanTabletForMemoryUsage(slot, tablet);
    }

    void ScanTabletForRotation(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        const auto& tabletManager = slot->GetTabletManager();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& bundleName = slot->GetTabletCellBundleName();

        if (tablet->ComputeDynamicStoreCount() >= DynamicStoreCountLimit) {
            auto error = TError("Dynamic store count limit is exceeded")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Rotation)
                << TErrorAttribute("limit", DynamicStoreCountLimit);
            YT_LOG_DEBUG(error);
            tablet->RuntimeData()->Errors[ETabletBackgroundActivity::Rotation].Store(error);
            return;
        }

        tablet->RuntimeData()->Errors[ETabletBackgroundActivity::Rotation].Store(TError());

        if (storeManager->IsOverflowRotationNeeded()) {
            YT_LOG_DEBUG("Scheduling store rotation due to overflow (%v)",
                tablet->GetLoggingTag());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        if (storeManager->IsPeriodicRotationNeeded()) {
            YT_LOG_INFO("Scheduling periodic store rotation (%v)",
                tablet->GetLoggingTag());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        if (storeManager->IsForcedRotationPossible()) {
            const auto& store = tablet->GetActiveStore();
            if (!storeManager->IsRotationScheduled() &&
                store->GetCompressedDataSize() >= MinForcedFlushDataSize_.load())
            {
                auto guard = Guard(SpinLock_);
                TabletCellBundleData_[bundleName].ForcedRotationCandidates.push_back({
                    store->GetDynamicMemoryUsage(),
                    tablet->GetId(),
                    tablet->GetMountRevision(),
                    tablet->GetLoggingTag(),
                    slot
                });
            }
        }
    }

    void ScanTabletForFlush(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        tablet->UpdateUnflushedTimestamp();

        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            ScanStoreForFlush(slot, tablet, store);
        }
    }

    void ScanTabletForMemoryUsage(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        const auto& bundleName = slot->GetTabletCellBundleName();

        tablet->UpdateUnflushedTimestamp();

        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            auto memoryUsage = store->GetDynamicMemoryUsage();
            switch (store->GetStoreState()) {
                case EStoreState::PassiveDynamic: {
                    auto guard = Guard(SpinLock_);
                    PassiveMemoryUsage_ += memoryUsage;
                    TabletCellBundleData_[bundleName].PassiveMemoryUsage += memoryUsage;
                    break;
                }

                case EStoreState::ActiveDynamic: {
                    auto guard = Guard(SpinLock_);
                    ActiveMemoryUsage_ += memoryUsage;
                    break;
                }

                case EStoreState::Persistent: {
                    if (auto backingStore = store->AsChunk()->GetBackingStore()) {
                        auto guard = Guard(SpinLock_);
                        auto backingMemoryUsage = backingStore->GetDynamicMemoryUsage();
                        BackingMemoryUsage_ += backingMemoryUsage;
                        TabletCellBundleData_[bundleName].BackingMemoryUsage += backingMemoryUsage;
                    }
                    break;
                }

                default:
                    break;
            }
        }

    }

    void ScanStoreForFlush(const ITabletSlotPtr& slot, TTablet* tablet, const IStorePtr& store)
    {
        if (!store->IsDynamic()) {
            return;
        }

        auto dynamicStore = store->AsDynamic();
        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsStoreFlushable(dynamicStore)) {
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tablet->GetId(), tablet->GetMountRevision());
        if (!tabletSnapshot) {
            return;
        }

        auto guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_);
        if (!guard) {
            return;
        }

        auto state = tablet->GetState();
        auto flushCallback = storeManager->BeginStoreFlush(
            dynamicStore,
            tabletSnapshot,
            IsInUnmountWorkflow(state));

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreFlusher::FlushStore,
            MakeStrong(this),
            Passed(std::move(guard)),
            slot,
            tablet,
            dynamicStore,
            flushCallback));
    }

    void FlushStore(
        TAsyncSemaphoreGuard /*guard*/,
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        IDynamicStorePtr store,
        TStoreFlushCallback flushCallback)
    {
        const auto& storeManager = tablet->GetStoreManager();
        auto tabletId = tablet->GetId();
        auto writerProfiler = New<TWriterProfiler>();

        auto Logger = TabletNodeLogger
            .WithTag("%v, StoreId: %v",
                tablet->GetLoggingTag(),
                store->GetId());

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tablet->GetId(), tablet->GetMountRevision());
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting flush");
            storeManager->BackoffStoreFlush(store);
            return;
        }

        bool failed = false;

        try {
            NProfiling::TWallTimer timer;

            YT_LOG_INFO("Store flush started");

            auto transactionAttributes = CreateEphemeralAttributes();
            transactionAttributes->Set("title", Format("Store flush: table %v, store %v, tablet %v",
                tabletSnapshot->TablePath,
                store->GetId(),
                tabletId));
            auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                NTransactionClient::ETransactionType::Master,
                TTransactionStartOptions{
                    .AutoAbort = false,
                    .Attributes = std::move(transactionAttributes),
                    .CoordinatorMasterCellTag = CellTagFromId(tablet->GetId()),
                    .ReplicateToMasterCellTags = {}
                });
            auto transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            const auto& mountConfig = tablet->GetSettings().MountConfig;
            auto currentTimestamp = transaction->GetStartTimestamp();
            auto retainedTimestamp = std::min(
                InstantToTimestamp(TimestampToInstant(currentTimestamp).second - mountConfig->MinDataTtl).second,
                currentTimestamp);

            YT_LOG_INFO("Store flush transaction created (TransactionId: %v)",
                transaction->GetId());

            auto throttler = Bootstrap_->GetTabletNodeOutThrottler(EWorkloadCategory::SystemTabletStoreFlush);

            auto asyncFlushResult = BIND(flushCallback)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run(transaction, std::move(throttler), currentTimestamp, writerProfiler);

            auto flushResult = WaitFor(asyncFlushResult)
                .ValueOrThrow();

            tablet->ThrottleTabletStoresUpdate(slot, Logger);

            NTabletServer::NProto::TReqUpdateTabletStores updateTabletStoresReq;
            ToProto(updateTabletStoresReq.mutable_tablet_id(), tabletId);
            updateTabletStoresReq.set_mount_revision(tablet->GetMountRevision());
            for (auto& descriptor : flushResult.StoresToAdd) {
                *updateTabletStoresReq.add_stores_to_add() = std::move(descriptor);
            }
            for (auto& descriptor : flushResult.HunkChunksToAdd) {
                *updateTabletStoresReq.add_hunk_chunks_to_add() = std::move(descriptor);
            }
            ToProto(updateTabletStoresReq.add_stores_to_remove()->mutable_store_id(), store->GetId());
            updateTabletStoresReq.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Flush));

            if (tabletSnapshot->Settings.MountConfig->EnableDynamicStoreRead) {
                int potentialDynamicStoreCount = tablet->DynamicStoreIdPool().size() + tablet->ComputeDynamicStoreCount();

                // NB: Race is possible here. Consider a tablet with an active store, two passive
                // dynamic stores and empty pool. If both passive stores are flushed concurrently
                // then both of them might fill transaction actions when there are three dynamic
                // stores. Hence dynamic store id will not be requested and the pool will remain
                // empty after the flush.
                //
                // However, this is safe because dynamic store id will be requested upon rotation
                // and the tablet will have two dynamic stores as usual.
                if (potentialDynamicStoreCount <= DynamicStoreIdPoolSize) {
                    updateTabletStoresReq.set_request_dynamic_store_id(true);
                    YT_LOG_DEBUG("Dynamic store id requested with flush (PotentialDynamicStoreCount: %v)",
                        potentialDynamicStoreCount);
                }
            }

            if (tabletSnapshot->Settings.MountConfig->MergeRowsOnFlush) {
                updateTabletStoresReq.set_retained_timestamp(retainedTimestamp);
            }

            auto actionData = MakeTransactionActionData(updateTabletStoresReq);
            auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tabletSnapshot->TabletId));
            transaction->AddAction(masterCellId, actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            storeManager->EndStoreFlush(store);
            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Flush].Store(TError());

            YT_LOG_INFO("Store flush completed (WallTime: %v)",
                timer.GetElapsedTime());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Flush);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Flush].Store(error);
            YT_LOG_ERROR(error, "Error flushing tablet store, backing off");

            storeManager->BackoffStoreFlush(store);
            failed = true;
        }

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::StoreFlush, failed);
    }
};

IStoreFlusherPtr CreateStoreFlusher(NClusterNode::TBootstrap* bootstrap)
{
    return New<TStoreFlusher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
