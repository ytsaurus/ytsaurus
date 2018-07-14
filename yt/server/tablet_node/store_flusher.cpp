#include "store_flusher.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "config.h"
#include "sorted_dynamic_store.h"
#include "in_memory_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hive/hive_manager.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/transaction_client/action.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NHydra;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NTabletNode::NProto;

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
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Profiler("/tablet_node/store_flusher")
        , ThreadPool_(New<TThreadPool>(Config_->StoreFlusher->ThreadPoolSize, "StoreFlush"))
        , Semaphore_(New<TProfiledAsyncSemaphore>(Config_->StoreFlusher->MaxConcurrentFlushes, Profiler, "/running_store_fluhses"))
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreFlusher::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreFlusher::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreFlusher::OnEndSlotScan, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    const NProfiling::TProfiler Profiler;
    TThreadPoolPtr ThreadPool_;
    TProfiledAsyncSemaphorePtr Semaphore_;

    struct TForcedRotationCandidate
    {
        i64 MemoryUsage;
        TTabletId TabletId;
        TTabletSlotPtr Slot;
    };

    TSpinLock SpinLock_;
    i64 PassiveMemoryUsage_;
    std::vector<TForcedRotationCandidate> ForcedRotationCandidates_;


    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, this locking is redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        PassiveMemoryUsage_ = 0;
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

            const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
            LOG_INFO("Scheduling store rotation due to memory pressure condition (TabletId: %v, "
                "TotalMemoryUsage: %v, TabletMemoryUsage: %v, "
                "MemoryLimit: %v)",
                candidate.TabletId,
                tracker->GetUsed(EMemoryCategory::TabletDynamic),
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
            LOG_DEBUG("Scheduling store rotation due to overflow (TabletId: %v)",
                tablet->GetId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        if (storeManager->IsPeriodicRotationNeeded()) {
            LOG_INFO("Scheduling periodic store rotation (TabletId: %v)",
                tablet->GetId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        for (const auto& pair : tablet->StoreIdMap()) {
            const auto& store = pair.second;
            ScanStore(slot, tablet, store);
            if (store->GetStoreState() == EStoreState::PassiveDynamic) {
                TGuard<TSpinLock> guard(SpinLock_);
                PassiveMemoryUsage_ += store->GetMemoryUsage();
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (storeManager->IsForcedRotationPossible()) {
                const auto& store = tablet->GetActiveStore();
                i64 memoryUsage = store->GetMemoryUsage();
                if (storeManager->IsRotationScheduled()) {
                    PassiveMemoryUsage_ += memoryUsage;
                } else if (store->GetCompressedDataSize() >= Config_->StoreFlusher->MinForcedFlushDataSize) {
                    ForcedRotationCandidates_.push_back({
                        memoryUsage,
                        tablet->GetId(),
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

        auto flushCallback = storeManager->BeginStoreFlush(dynamicStore, tabletSnapshot);

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
        const auto& tabletId = tablet->GetId();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, StoreId: %v",
            tabletId,
            store->GetId());

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tablet->GetId());
        if (!tabletSnapshot) {
            LOG_DEBUG("Tablet snapshot is missing, aborting flush");
            storeManager->BackoffStoreFlush(store);
            return;
        }

        try {
            NProfiling::TWallTimer timer;

            LOG_INFO("Store flush started");

            TTransactionStartOptions options;
            options.AutoAbort = false;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Store flush: table %v, store %v, tablet %v",
                tabletSnapshot->TablePath,
                store->GetId(),
                tabletId));
            options.Attributes = std::move(attributes);

            auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                NTransactionClient::ETransactionType::Master,
                options);
            auto transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            LOG_INFO("Store flush transaction created (TransactionId: %v)",
                transaction->GetId());

            auto asyncFlushResult = flushCallback
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run(transaction);

            auto flushResult = WaitFor(asyncFlushResult)
                .ValueOrThrow();

            LOG_INFO("Store chunks written (ChunkIds: %v)",
                MakeFormattableRange(flushResult, [] (TStringBuilder* builder, const TAddStoreDescriptor& descriptor) {
                    FormatValue(builder, FromProto<TChunkId>(descriptor.store_id()), TStringBuf());
                }));

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tabletId);
            actionRequest.set_mount_revision(tablet->GetMountRevision());
            ToProto(actionRequest.mutable_stores_to_add(), flushResult);
            ToProto(actionRequest.add_stores_to_remove()->mutable_store_id(), store->GetId());

            auto actionData = MakeTransactionActionData(actionRequest);
            transaction->AddAction(Bootstrap_->GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellId(), actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            storeManager->EndStoreFlush(store);
            tabletSnapshot->RuntimeData->Errors[ETabletBackgroundActivity::Flush].Store(TError());

            LOG_INFO("Store flush completed (WallTime: %v)",
                timer.GetElapsedTime());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Flush);

            tabletSnapshot->RuntimeData->Errors[ETabletBackgroundActivity::Flush].Store(error);
            LOG_ERROR(error, "Error flushing tablet store, backing off");

            storeManager->BackoffStoreFlush(store);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void StartStoreFlusher(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    if (config->EnableStoreFlusher) {
        New<TStoreFlusher>(config, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
