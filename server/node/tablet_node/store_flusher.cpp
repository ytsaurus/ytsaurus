#include "store_flusher.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "in_memory_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "public.h"
#include "tablet_profiling.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/lib/hive/hive_manager.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/transaction_client/action.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

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
    : public TRefCounted
{
public:
    TStoreFlusher(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Profiler("/tablet_node/store_flusher")
        , ThreadPool_(New<TThreadPool>(Config_->StoreFlusher->ThreadPoolSize, "StoreFlush"))
        , Semaphore_(New<TProfiledAsyncSemaphore>(Config_->StoreFlusher->MaxConcurrentFlushes, Profiler, "/running_store_fluhses"))
        , StoreFlushTag_(NProfiling::TProfileManager::Get()->RegisterTag("method", "store_flush"))
        , StoreFlushFailedTag_(NProfiling::TProfileManager::Get()->RegisterTag("method", "store_flush_failed"))
        , DynamicMemoryUsageActiveCounter_("/dynamic_memory_usage", {NProfiling::TProfileManager::Get()->RegisterTag("memory_type", "active")})
        , DynamicMemoryUsagePassiveCounter_("/dynamic_memory_usage", {NProfiling::TProfileManager::Get()->RegisterTag("memory_type", "passive")})
        , DynamicMemoryUsageBackingCounter_("/dynamic_memory_usage", {NProfiling::TProfileManager::Get()->RegisterTag("memory_type", "backing")})
        , DynamicMemoryUsageOtherCounter_("/dynamic_memory_usage", {NProfiling::TProfileManager::Get()->RegisterTag("memory_type", "other")})
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreFlusher::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreFlusher::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreFlusher::OnEndSlotScan, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;

    NClusterNode::TBootstrap* const Bootstrap_;

    const NProfiling::TProfiler Profiler;
    TThreadPoolPtr ThreadPool_;
    TProfiledAsyncSemaphorePtr Semaphore_;

    const NProfiling::TTagId StoreFlushTag_;
    const NProfiling::TTagId StoreFlushFailedTag_;

    NProfiling::TSimpleGauge DynamicMemoryUsageActiveCounter_;
    NProfiling::TSimpleGauge DynamicMemoryUsagePassiveCounter_;
    NProfiling::TSimpleGauge DynamicMemoryUsageBackingCounter_;
    NProfiling::TSimpleGauge DynamicMemoryUsageOtherCounter_;

    struct TForcedRotationCandidate
    {
        i64 MemoryUsage;
        TTabletId TabletId;
        TString TabletLoggingId;
        TTabletSlotPtr Slot;
    };

    TSpinLock SpinLock_;
    i64 PassiveMemoryUsage_;
    i64 ActiveMemoryUsage_;
    i64 BackingMemoryUsage_;
    std::vector<TForcedRotationCandidate> ForcedRotationCandidates_;

    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, this locking is redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        ActiveMemoryUsage_ = 0;
        PassiveMemoryUsage_ = 0;
        BackingMemoryUsage_ = 0;
        ForcedRotationCandidates_.clear();
    }

    void OnScanSlot(const TTabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }
        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void OnEndSlotScan()
    {
        std::vector<TForcedRotationCandidate> candidates;

        // NB: Strictly speaking, this locking is redundant.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            ForcedRotationCandidates_.swap(candidates);
        }

        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        auto otherUsage = tracker->GetUsed(EMemoryCategory::TabletDynamic) -
            ActiveMemoryUsage_ - PassiveMemoryUsage_ - BackingMemoryUsage_;

        Profiler.Update(DynamicMemoryUsageActiveCounter_, ActiveMemoryUsage_);
        Profiler.Update(DynamicMemoryUsagePassiveCounter_, PassiveMemoryUsage_);
        Profiler.Update(DynamicMemoryUsageBackingCounter_, BackingMemoryUsage_);
        Profiler.Update(DynamicMemoryUsageOtherCounter_, otherUsage);

        // Order candidates by increasing memory usage.
        std::sort(
            candidates. begin(),
            candidates.end(),
            [] (const TForcedRotationCandidate& lhs, const TForcedRotationCandidate& rhs) {
                return lhs.MemoryUsage < rhs.MemoryUsage;
            });

        // Pick the heaviest candidates until no more rotations are needed.
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        while (slotManager->IsRotationForced(PassiveMemoryUsage_) && !candidates.empty()) {
            auto candidate = candidates.back();
            candidates.pop_back();

            auto tabletId = candidate.TabletId;
            auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId);
            if (!tabletSnapshot)
                continue;

            YT_LOG_INFO("Scheduling store rotation due to memory pressure condition (%v, "
                "TotalMemoryUsage: %v, PassiveMemoryUsage: %v, TabletMemoryUsage: %v, "
                "MemoryLimit: %v)",
                candidate.TabletLoggingId,
                tracker->GetUsed(EMemoryCategory::TabletDynamic),
                PassiveMemoryUsage_,
                candidate.MemoryUsage,
                tracker->GetLimit(EMemoryCategory::TabletDynamic));

            const auto& slot = candidate.Slot;
            auto invoker = slot->GetGuardedAutomatonInvoker();
            invoker->Invoke(BIND([slot, tabletId] () {
                const auto& tabletManager = slot->GetTabletManager();
                auto* tablet = tabletManager->FindTablet(tabletId);
                if (!tablet)
                    return;
                tabletManager->ScheduleStoreRotation(tablet);
            }));

            PassiveMemoryUsage_ += candidate.MemoryUsage;
        }
    }

    void ScanTablet(const TTabletSlotPtr& slot, TTablet* tablet)
    {
        const auto& tabletManager = slot->GetTabletManager();
        const auto& storeManager = tablet->GetStoreManager();

        tablet->UpdateUnflushedTimestamp();

        if (storeManager->IsOverflowRotationNeeded()) {
            YT_LOG_DEBUG("Scheduling store rotation due to overflow (%v)",
                tablet->GetLoggingId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        if (storeManager->IsPeriodicRotationNeeded()) {
            YT_LOG_INFO("Scheduling periodic store rotation (%v)",
                tablet->GetLoggingId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        for (const auto& pair : tablet->StoreIdMap()) {
            const auto& store = pair.second;
            ScanStore(slot, tablet, store);
            if (store->GetStoreState() == EStoreState::PassiveDynamic) {
                TGuard<TSpinLock> guard(SpinLock_);
                PassiveMemoryUsage_ += store->GetDynamicMemoryUsage();
            } else if (store->GetStoreState() == EStoreState::ActiveDynamic) {
                TGuard<TSpinLock> guard(SpinLock_);
                ActiveMemoryUsage_ += store->GetDynamicMemoryUsage();
            } else if (store->GetStoreState() == EStoreState::Persistent) {
                auto backingStore = store->AsChunk()->GetBackingStore();
                if (backingStore) {
                    TGuard<TSpinLock> guard(SpinLock_);
                    BackingMemoryUsage_ += backingStore->GetDynamicMemoryUsage();
                }
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (storeManager->IsForcedRotationPossible()) {
                const auto& store = tablet->GetActiveStore();
                i64 memoryUsage = store->GetDynamicMemoryUsage();
                if (storeManager->IsRotationScheduled()) {
                    PassiveMemoryUsage_ += memoryUsage;
                } else if (store->GetCompressedDataSize() >= Config_->StoreFlusher->MinForcedFlushDataSize) {
                    ForcedRotationCandidates_.push_back({
                        memoryUsage,
                        tablet->GetId(),
                        tablet->GetLoggingId(),
                        slot
                    });
                }
            }
        }
    }

    void ScanStore(const TTabletSlotPtr& slot, TTablet* tablet, const IStorePtr& store)
    {
        if (!store->IsDynamic()) {
            return;
        }

        auto dynamicStore = store->AsDynamic();
        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsStoreFlushable(dynamicStore)) {
            return;
        }

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tablet->GetId());
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
        const TTabletSlotPtr& slot,
        TTablet* tablet,
        IDynamicStorePtr store,
        TStoreFlushCallback flushCallback)
    {
        const auto& storeManager = tablet->GetStoreManager();
        auto tabletId = tablet->GetId();
        TWriterProfilerPtr writerProfiler = New<TWriterProfiler>();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("%v, StoreId: %v",
            tablet->GetLoggingId(),
            store->GetId());

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tablet->GetId());
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
                    .ReplicateToMasterCellTags = TCellTagList()
                });
            auto transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            auto currentTimestamp = transaction->GetStartTimestamp();
            auto retainedTimestamp = std::min(
                InstantToTimestamp(TimestampToInstant(currentTimestamp).second - tablet->GetConfig()->MinDataTtl).second,
                currentTimestamp
            );

            YT_LOG_INFO("Store flush transaction created (TransactionId: %v)",
                transaction->GetId());

            auto throttler = Bootstrap_->GetTabletNodeOutThrottler(EWorkloadCategory::SystemTabletStoreFlush);

            auto asyncFlushResult = flushCallback
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run(transaction, std::move(throttler), currentTimestamp, writerProfiler);

            auto flushResult = WaitFor(asyncFlushResult)
                .ValueOrThrow();

            YT_LOG_INFO("Store chunks written (ChunkIds: %v)",
                MakeFormattableView(flushResult, [] (TStringBuilderBase* builder, const TAddStoreDescriptor& descriptor) {
                    FormatValue(builder, FromProto<TChunkId>(descriptor.store_id()), TStringBuf());
                }));

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(tablet->GetMountRevision());
            ToProto(actionRequest.mutable_stores_to_add(), flushResult);
            ToProto(actionRequest.add_stores_to_remove()->mutable_store_id(), store->GetId());
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Flush));

            if (tablet->GetConfig()->EnableDynamicStoreRead) {
                int dynamicStoreCount = tablet->DynamicStoreIdPool().size();
                for (const auto& store : tablet->GetEden()->Stores()) {
                    if (store->IsDynamic()) {
                        ++dynamicStoreCount;
                    }
                }

                // NB: Race is possible here. Consider a tablet with an active store, two passive
                // dynamic stores and empty pool. If both passive stores are flushed concurrently
                // then both of them might fill transaction actions when there are three dynamic
                // stores. Hence dynamic store id will not be requested and the pool will remain
                // empty after the flush.
                //
                // However, this is safe because dynamic store id will be requested upon rotation
                // and the tablet will have two dynamic stores as usual.
                if (dynamicStoreCount <= DynamicStoreIdPoolSize) {
                    actionRequest.set_request_dynamic_store_id(true);
                    YT_LOG_DEBUG("Dynamic store id requested with flush (DynamicStoreCount: %v)",
                        dynamicStoreCount);
                }
            }

            if (tabletSnapshot->Config->MergeRowsOnFlush) {
                actionRequest.set_retained_timestamp(retainedTimestamp);
            }

            auto actionData = MakeTransactionActionData(actionRequest);
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
        writerProfiler->Profile(tabletSnapshot, failed ? StoreFlushFailedTag_ : StoreFlushTag_);
    }
};

////////////////////////////////////////////////////////////////////////////////

void StartStoreFlusher(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    if (config->EnableStoreFlusher) {
        New<TStoreFlusher>(config, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
