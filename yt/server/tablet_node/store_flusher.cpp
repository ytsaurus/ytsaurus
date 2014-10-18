#include "stdafx.h"
#include "store_flusher.h"
#include "private.h"
#include "config.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "tablet_slot_manager.h"
#include "store_manager.h"

#include <core/misc/address.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/scheduler.h>
#include <core/concurrency/async_semaphore.h>

#include <core/ytree/attribute_helpers.h>

#include <core/logging/log.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_writer.h>
#include <ytlib/new_table_client/versioned_chunk_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/api/client.h>
#include <ytlib/api/transaction.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/hydra/mutation.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NTabletServer::NProto;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static const size_t MaxRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher;
typedef TIntrusivePtr<TStoreFlusher> TStoreFlusherPtr;

class TStoreFlusher
    : public TRefCounted
{
public:
    TStoreFlusher(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->StoreFlusher->ThreadPoolSize, "StoreFlush"))
        , Semaphore_(Config_->StoreFlusher->MaxConcurrentFlushes)
    { }

    void Start()
    {
        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        tabletSlotManager->SubscribeBeginSlotScan(BIND(&TStoreFlusher::BeginSlotScan, MakeStrong(this)));
        tabletSlotManager->SubscribeScanSlot(BIND(&TStoreFlusher::ScanSlot, MakeStrong(this)));
        tabletSlotManager->SubscribeEndSlotScan(BIND(&TStoreFlusher::EndSlotScan, MakeStrong(this)));
    }

private:
    TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    TThreadPoolPtr ThreadPool_;
    TAsyncSemaphore Semaphore_;

    struct TForcedRotationCandidate
    {
        i64 MemoryUsage;
        TTabletId TabletId;
    };

    TSpinLock SpinLock_;
    i64 PassiveMemoryUsage_;
    std::vector<TForcedRotationCandidate> ForcedRotationCandidates_;


    void BeginSlotScan()
    {
        // NB: No locking is needed.
        PassiveMemoryUsage_ = 0;
        ForcedRotationCandidates_.clear();
    }

    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

        auto tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(tablet);
        }
    }

    void EndSlotScan()
    {
        // NB: No locking is needed.
        // Order candidates by increasing memory usage.
        std::sort(
            ForcedRotationCandidates_. begin(),
            ForcedRotationCandidates_.end(),
            [] (const TForcedRotationCandidate& lhs, const TForcedRotationCandidate& rhs) {
                return lhs.MemoryUsage < rhs.MemoryUsage;
            });
        
        // Pick the heaviest candidates until no more rotations are needed.
        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        while (tabletSlotManager->IsRotationForced(PassiveMemoryUsage_) &&
               !ForcedRotationCandidates_.empty())
        {
            auto candidate = ForcedRotationCandidates_.back();
            ForcedRotationCandidates_.pop_back();

            auto tabletId = candidate.TabletId;
            auto tabletSnapshot = tabletSlotManager->FindTabletSnapshot(tabletId);
            if (!tabletSnapshot)
                continue;

            LOG_INFO("Scheduling store rotation due to memory pressure condition (TabletId: %v, "
                "TotalMemoryUsage: %v, TabletMemoryUsage: %v, "
                "MemoryLimit: %v)",
                candidate.TabletId,
                Bootstrap_->GetMemoryUsageTracker()->GetUsed(NCellNode::EMemoryConsumer::Tablet),
                candidate.MemoryUsage,
                Config_->MemoryLimit);

            auto slot = tabletSnapshot->Slot;
            auto invoker = slot->GetGuardedAutomatonInvoker();
            invoker->Invoke(BIND([slot, tabletId] () {
                auto tabletManager = slot->GetTabletManager();
                auto* tablet = tabletManager->FindTablet(tabletId);
                if (!tablet)
                    return;
                tabletManager->ScheduleStoreRotation(tablet);
            }));

            PassiveMemoryUsage_ += candidate.MemoryUsage;
        }
    }

    void ScanTablet(TTablet* tablet)
    {
        auto slot = tablet->GetSlot();
        auto tabletManager = slot->GetTabletManager();
        auto storeManager = tablet->GetStoreManager();

        if (storeManager->IsPeriodicRotationNeeded()) {
            LOG_INFO("Scheduling periodic store rotation (TabletId: %v)",
                tablet->GetId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        if (storeManager->IsOverflowRotationNeeded()) {
            LOG_INFO("Scheduling store rotation due to overflow (TabletId: %v)",
                tablet->GetId());
            tabletManager->ScheduleStoreRotation(tablet);
        }

        for (const auto& pair : tablet->Stores()) {
            const auto& store = pair.second;
            ScanStore(tablet, store);
            if (store->GetState() == EStoreState::PassiveDynamic) {
                TGuard<TSpinLock> guard(SpinLock_);
                auto dynamicStore = store->AsDynamicMemory();
                PassiveMemoryUsage_ += dynamicStore->GetMemoryUsage();
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto storeManager = tablet->GetStoreManager();
            if (storeManager->IsForcedRotationPossible()) {
                const auto& store = tablet->GetActiveStore();
                i64 memoryUsage = store->GetMemoryUsage();
                if (storeManager->IsRotationScheduled()) {
                    PassiveMemoryUsage_ += memoryUsage;
                } else {
                    TForcedRotationCandidate candidate;
                    candidate.TabletId = tablet->GetId();
                    candidate.MemoryUsage = memoryUsage;
                    ForcedRotationCandidates_.push_back(candidate);
                }
            }
        }
    }

    void ScanStore(TTablet* tablet, const IStorePtr& store)
    {
        if (store->GetState() != EStoreState::PassiveDynamic)
            return;

        auto guard = TAsyncSemaphoreGuard::TryAcquire(&Semaphore_);
        if (!guard)
            return;

        store->SetState(EStoreState::Flushing);

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreFlusher::FlushStore,
            MakeStrong(this),
            Passed(std::move(guard)),
            tablet,
            store));
    }


    void FlushStore(
        TAsyncSemaphoreGuard /*guard*/,
        TTablet* tablet,
        IStorePtr store)
    {
        // Capture everything needed below.
        // NB: Avoid accessing tablet from pool invoker.
        auto* slot = tablet->GetSlot();
        auto hydraManager = slot->GetHydraManager();
        auto tabletManager = slot->GetTabletManager();
        auto tabletId = tablet->GetId();
        auto writerOptions = tablet->GetWriterOptions();
        auto keyColumns = tablet->KeyColumns();
        auto schema = tablet->Schema();

        YCHECK(store->GetState() == EStoreState::Flushing);

        NLog::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, StoreId: %v",
            tabletId,
            store->GetId());

        auto automatonInvoker = tablet->GetEpochAutomatonInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        TObjectServiceProxy proxy(Bootstrap_->GetMasterClient()->GetMasterChannel());

        try {
            LOG_INFO("Store flush started");

            auto reader = store->CreateReader(
                MinKey(),
                MaxKey(),
                AsyncAllCommittedTimestamp,
                TColumnFilter());
        
            // NB: Memory store reader is always synchronous.
            YCHECK(reader->Open().Get().IsOK());

            SwitchTo(poolInvoker);

            ITransactionPtr transaction;
            {
                LOG_INFO("Creating store flush transaction");
                TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Flushing store %v, tablet %v",
                    store->GetId(),
                    tabletId));
                options.Attributes = attributes.get();

                auto transactionOrError = WaitFor(Bootstrap_->GetMasterClient()->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
                THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);

                transaction = transactionOrError.Value();
                LOG_INFO("Store flush transaction created (TransactionId: %v)",
                    transaction->GetId());
            }

            TReqCommitTabletStoresUpdate hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());
            {
                auto* descriptor = hydraRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }

            auto writer = CreateVersionedMultiChunkWriter(
                Config_->ChunkWriter,
                writerOptions,
                schema,
                keyColumns,
                Bootstrap_->GetMasterClient()->GetMasterChannel(),
                transaction->GetId());

            {
                auto result = WaitFor(writer->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
        
            std::vector<TVersionedRow> rows;
            rows.reserve(MaxRowsPerRead);

            while (true) {
                // NB: Memory store reader is always synchronous.
                reader->Read(&rows);
                if (rows.empty())
                    break;
                if (!writer->Write(rows)) {
                    auto result = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
            }

            {
                auto result = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            for (const auto& chunkSpec : writer->GetWrittenChunks()) {
                auto* descriptor = hydraRequest.add_stores_to_add();
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                ToProto(descriptor->mutable_backing_store_id(), store->GetId());
            }

            SwitchTo(automatonInvoker);

            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->Commit();

            LOG_INFO("Store flush completed");

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error flushing tablet store, backing off");
        
            SwitchTo(automatonInvoker);

            YCHECK(store->GetState() == EStoreState::Flushing);
            tabletManager->BackoffStore(store, EStoreState::FlushFailed);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

void StartStoreFlusher(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    New<TStoreFlusher>(config, bootstrap)->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
