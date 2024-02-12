#include "store_flusher.h"

#include "background_activity_orchid.h"
#include "bootstrap.h"
#include "public.h"
#include "slot_manager.h"
#include "store_detail.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

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

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/virtual.h>

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
using namespace NYson;
using namespace NYTree;
using namespace NTracing;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFlushTaskInfo
    : public TTaskInfoBase
{
    TStoreId StoreId;

    bool ComparePendingTasks(const TFlushTaskInfo& /*other*/) const
    {
        return false;
    }
};

void Serialize(const TFlushTaskInfo& task, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Do([&] (auto fluent) {
            Serialize(static_cast<const TTaskInfoBase&>(task), fluent.GetConsumer());
        })
        .Item("store_id").Value(task.StoreId)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

using TFlushOrchid = TBackgroundActivityOrchid<TFlushTaskInfo>;
using TFlushOrchidPtr = TIntrusivePtr<TFlushOrchid>;

DEFINE_REFCOUNTED_TYPE(TFlushOrchid);

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher
    : public IStoreFlusher
{
public:
    explicit TStoreFlusher(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode)
        , ThreadPool_(CreateThreadPool(Config_->StoreFlusher->ThreadPoolSize, "StoreFlush"))
        , Semaphore_(New<TProfiledAsyncSemaphore>(
            Config_->StoreFlusher->MaxConcurrentFlushes,
            Profiler_.Gauge("/running_store_flushes")))
        , Orchid_(New<TFlushOrchid>(
            Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode->StoreFlusher->Orchid))
        , OrchidService_(CreateOrchidService())
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TStoreFlusher::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreFlusher::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreFlusher::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreFlusher::OnEndSlotScan, MakeStrong(this)));
    }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

private:
    IBootstrap* const Bootstrap_;
    const TTabletNodeConfigPtr Config_;

    const NProfiling::TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/store_flusher");

    const IThreadPoolPtr ThreadPool_;
    const TProfiledAsyncSemaphorePtr Semaphore_;

    const TFlushOrchidPtr Orchid_;
    IYPathServicePtr OrchidService_;

    NProfiling::TGauge DynamicMemoryUsageActiveCounter_ = Profiler_.WithTag("memory_type", "active").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsagePassiveCounter_ = Profiler_.WithTag("memory_type", "passive").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsageBackingCounter_ = Profiler_.WithTag("memory_type", "backing").Gauge("/dynamic_memory_usage");
    NProfiling::TGauge DynamicMemoryUsageOtherCounter_ = Profiler_.WithTag("memory_type", "other").Gauge("/dynamic_memory_usage");

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    i64 PassiveMemoryUsage_;
    i64 ActiveMemoryUsage_;
    i64 BackingMemoryUsage_;

    IYPathServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer)
                    .Value(true);
            }))
            ->AddChild("flush_tasks", IYPathService::FromProducer(
                BIND(&TFlushOrchid::Serialize, MakeWeak(Orchid_))))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreFlusher;
        ThreadPool_->Configure(config->ThreadPoolSize.value_or(Config_->StoreFlusher->ThreadPoolSize));
        Semaphore_->SetTotal(config->MaxConcurrentFlushes.value_or(Config_->StoreFlusher->MaxConcurrentFlushes));
        Orchid_->Reconfigure(config->Orchid);
    }

    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, this locking is redundant.
        auto guard = Guard(SpinLock_);
        ActiveMemoryUsage_ = 0;
        PassiveMemoryUsage_ = 0;
        BackingMemoryUsage_ = 0;

        Orchid_->ClearPendingTasks();
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

        const auto& tabletManager = slot->GetTabletManager();
        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            ScanTablet(slot, tablet);
        }
    }

    void OnEndSlotScan()
    {
        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        auto otherUsage = tracker->GetUsed(EMemoryCategory::TabletDynamic) -
            ActiveMemoryUsage_ - PassiveMemoryUsage_ - BackingMemoryUsage_;

        DynamicMemoryUsageActiveCounter_.Update(ActiveMemoryUsage_);
        DynamicMemoryUsagePassiveCounter_.Update(PassiveMemoryUsage_);
        DynamicMemoryUsageBackingCounter_.Update(BackingMemoryUsage_);
        DynamicMemoryUsageOtherCounter_.Update(otherUsage);
    }

    void ScanTablet(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        ScanTabletForRotationErrors(tablet);
        ScanTabletForFlush(slot, tablet);
        ScanTabletForLookupCacheReallocation(tablet);
        ScanTabletForMemoryUsage(tablet);
    }

    void ScanTabletForRotationErrors(TTablet* tablet)
    {
        if (tablet->GetDynamicStoreCount() >= DynamicStoreCountLimit) {
            auto error = TError("Dynamic store count limit is exceeded")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Rotation)
                << TErrorAttribute("limit", DynamicStoreCountLimit);
            YT_LOG_DEBUG(error);
            tablet->RuntimeData()->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Rotation].Store(error);
            return;
        }

        tablet->RuntimeData()->Errors
            .BackgroundErrors[ETabletBackgroundActivity::Rotation].Store(TError());
    }

    void ScanTabletForFlush(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        tablet->UpdateUnflushedTimestamp();

        const auto& rowCache = tablet->GetRowCache();
        if (rowCache && rowCache->GetReallocatingItems()) {
            return;
        }

        std::vector<TGuid> taskIds(tablet->StoreIdMap().size());
        int index = 0;

        TFlushOrchid::TTaskMap pendingTasks;
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            if (store->IsDynamic() &&
                tablet->GetStoreManager()->IsStoreFlushable(store->AsDynamic()))
            {
                auto taskId = TGuid::Create();
                taskIds[index] = taskId;
                pendingTasks.emplace(
                    taskId,
                    TFlushTaskInfo{
                        TTaskInfoBase{
                            .TaskId = taskId,
                            .TabletId = tablet->GetId(),
                            .MountRevision = tablet->GetMountRevision(),
                            .TablePath = tablet->GetTablePath(),
                            .TabletCellBundle = slot->GetTabletCellBundleName(),
                        },
                        storeId,
                    }
                );
            }
            ++index;
        }

        Orchid_->ResetPendingTasks(std::move(pendingTasks));

        auto taskIdIt = taskIds.begin();
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            ScanStoreForFlush(slot, tablet, store, *(taskIdIt++));
        }
    }

    void ScanTabletForLookupCacheReallocation(TTablet* tablet)
    {
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            if (!store->IsDynamic()) {
                continue;
            }

            auto dynamicStore = store->AsDynamic();
            if (dynamicStore->GetFlushState() == EStoreFlushState::Running) {
                return;
            }
        }

        const auto& rowCache = tablet->GetRowCache();
        if (!rowCache || rowCache->GetReallocatingItems() || !rowCache->GetAllocator()->IsReallocationNeeded()) {
            return;
        }

        rowCache->SetReallocatingItems(true);

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreFlusher::ReallocateLookupCacheMemory,
            MakeStrong(this),
            tablet));
    }

    void ReallocateLookupCacheMemory(TTablet* tablet)
    {
        const auto& rowCache = tablet->GetRowCache();

        try {
            auto reallocateResult = BIND(&TRowCache::ReallocateItems, rowCache, Logger)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            WaitFor(reallocateResult)
                .ThrowOnError();

            rowCache->SetReallocatingItems(false);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error reallocating cache memory (TabletId: %v)", tablet->GetId());
        }
    }

    void ScanTabletForMemoryUsage(TTablet* tablet)
    {
        i64 passiveMemoryUsage = 0;
        i64 activeMemoryUsage = 0;
        i64 backingMemoryUsage = 0;

        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            auto memoryUsage = store->GetDynamicMemoryUsage();
            switch (store->GetStoreState()) {
                case EStoreState::PassiveDynamic:
                    passiveMemoryUsage += memoryUsage;
                    break;

                case EStoreState::ActiveDynamic:
                    activeMemoryUsage += memoryUsage;
                    break;

                case EStoreState::Persistent:
                    if (auto backingStore = store->AsChunk()->GetBackingStore()) {
                        backingMemoryUsage += backingStore->GetDynamicMemoryUsage();
                    }
                    break;

                default:
                    break;
            }
        }

        auto guard = Guard(SpinLock_);
        PassiveMemoryUsage_ += passiveMemoryUsage;
        ActiveMemoryUsage_ += activeMemoryUsage;
        BackingMemoryUsage_ += backingMemoryUsage;
    }

    void ScanStoreForFlush(const ITabletSlotPtr& slot, TTablet* tablet, const IStorePtr& store, TGuid taskId)
    {
        if (!store->IsDynamic()) {
            return;
        }

        auto dynamicStore = store->AsDynamic();
        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsStoreFlushable(dynamicStore)) {
            return;
        }

        const auto& movementData = tablet->SmoothMovementData();
        bool isCommonFlush = movementData.CommonDynamicStoreIds().contains(store->GetId());
        if (!movementData.IsTabletStoresUpdateAllowed(isCommonFlush)) {
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
            flushCallback,
            taskId));
    }

    void FlushStore(
        TAsyncSemaphoreGuard /*guard*/,
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        IDynamicStorePtr store,
        TStoreFlushCallback flushCallback,
        TGuid taskId)
    {
        const auto& storeManager = tablet->GetStoreManager();
        auto tabletId = tablet->GetId();
        auto writerProfiler = New<TWriterProfiler>();

        auto Logger = TabletNodeLogger
            .WithTag("%v, StoreId: %v",
                tablet->GetLoggingTag(),
                store->GetId());

        auto traceId = taskId;
        TTraceContextGuard traceContextGuard(
            TTraceContext::NewRoot("StoreFlusher", traceId));

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tablet->GetId(), tablet->GetMountRevision());
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting flush");
            storeManager->BackoffStoreFlush(store);
            return;
        }

        bool failed = false;

        Orchid_->OnTaskStarted(taskId);

        try {
            NProfiling::TWallTimer timer;

            YT_LOG_INFO("Store flush started");

            auto transactionAttributes = CreateEphemeralAttributes();
            transactionAttributes->Set("title", Format("Store flush: table %v, store %v, tablet %v",
                tabletSnapshot->TablePath,
                store->GetId(),
                tabletId));
            TTransactionStartOptions transactionOptions;
            transactionOptions.AutoAbort = false;
            transactionOptions.Attributes = std::move(transactionAttributes);
            transactionOptions.CoordinatorMasterCellTag = CellTagFromId(tablet->GetId());
            transactionOptions.ReplicateToMasterCellTags = {};
            transactionOptions.StartCypressTransaction = false;

            auto asyncTransaction = Bootstrap_->GetClient()->StartNativeTransaction(
                NTransactionClient::ETransactionType::Master,
                transactionOptions);
            auto transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            const auto& mountConfig = tablet->GetSettings().MountConfig;
            auto currentTimestamp = transaction->GetStartTimestamp();
            auto retainedTimestamp = CalculateRetainedTimestamp(currentTimestamp, mountConfig->MinDataTtl);

            YT_LOG_INFO("Store flush transaction created (TransactionId: %v)",
                transaction->GetId());

            tablet->GetStructuredLogger()->LogEvent("start_flush")
                .Item("store_id").Value(store->GetId())
                .Item("tablet_id").Value(tablet->GetId())
                .Item("transaction_id").Value(transaction->GetId())
                .Item("trace_id").Value(traceId);

            auto throttler = Bootstrap_->GetOutThrottler(EWorkloadCategory::SystemTabletStoreFlush);

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
            updateTabletStoresReq.set_create_hunk_chunks_during_prepare(true);

            ToProto(updateTabletStoresReq.add_stores_to_remove()->mutable_store_id(), store->GetId());
            updateTabletStoresReq.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Flush));

            // If dynamic stores for an ordered tablet are requested both with flush and
            // via AllocateDynamicStore, reordering is possible and dynamic stores will
            // occur in different order at master and at node.
            // See YT-15197.
            bool shouldRequestDynamicStoreId = tabletSnapshot->Settings.MountConfig->EnableDynamicStoreRead &&
                tabletSnapshot->PhysicalSchema->IsSorted();

            if (shouldRequestDynamicStoreId) {
                int potentialDynamicStoreCount = tablet->DynamicStoreIdPool().size() + tablet->GetDynamicStoreCount();

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

            tablet->GetStructuredLogger()->LogEvent("end_flush")
                .Item("store_id").Value(store->GetId())
                .Item("tablet_id").Value(tablet->GetId())
                .Item("store_ids_to_add")
                    .BeginList()
                        .DoFor(flushResult.StoresToAdd, [] (TFluentList fluent, const TAddStoreDescriptor& descriptor) {
                            fluent
                                .Item().Value(FromProto<TStoreId>(descriptor.store_id()));
                        })
                    .EndList()
                .Item("hunk_ids_to_add")
                    .BeginList()
                        .DoFor(flushResult.HunkChunksToAdd, [] (TFluentList fluent, const TAddHunkChunkDescriptor& descriptor) {
                            fluent
                                .Item().Value(FromProto<TChunkId>(descriptor.chunk_id()));
                        })
                    .EndList()
                .Item("trace_id").Value(traceId);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            storeManager->EndStoreFlush(store);
            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Flush].Store(TError());

            YT_LOG_INFO("Store flush completed (WallTime: %v)",
                timer.GetElapsedTime());

            Orchid_->OnTaskCompleted(taskId);
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Flush);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Flush].Store(error);
            YT_LOG_ERROR(error, "Error flushing tablet store, backing off");

            storeManager->BackoffStoreFlush(store);
            failed = true;

            Orchid_->OnTaskFailed(taskId);
        }

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::StoreFlush, failed);
    }
};

IStoreFlusherPtr CreateStoreFlusher(IBootstrap* bootstrap)
{
    return New<TStoreFlusher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
