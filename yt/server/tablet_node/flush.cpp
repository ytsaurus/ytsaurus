#include "stdafx.h"
#include "flush.h"
#include "private.h"
#include "config.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "tablet_cell_controller.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_writer.h>

#include <ytlib/api/transaction.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/hydra/mutation.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NApi;
using namespace NChunkClient;
using namespace NHydra;
using namespace NTabletServer::NProto;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;
static const TDuration ScanPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TStoreFlusherConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->PoolSize, "StoreFlush"))
        , ScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::ScanSlots, Unretained(this)),
            ScanPeriod))
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
    }

    void Start()
    {
        ScanExecutor_->Start();
    }

private:
    TStoreFlusherConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    TThreadPoolPtr ThreadPool_;

    TPeriodicExecutorPtr ScanExecutor_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void ScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Post ScanSlot callback to each leading slot.
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        const auto& slots = tabletCellController->Slots();
        for (const auto& slot : slots) {
            if (slot->GetState() == EPeerState::Leading) {
                slot->GetEpochAutomatonInvoker()->Invoke(
                    BIND(&TImpl::ScanSlot, MakeStrong(this), slot));
            }
        }
    }

    void ScanSlot(const TTabletSlotPtr& slot)
    {
        auto tabletManager = slot->GetTabletManager();
        auto tablets = tabletManager->Tablets().GetValues();
        for (auto* tablet : tablets) {
            ScanTablet(tablet);
        }
    }

    void ScanTablet(TTablet* tablet)
    {
        for (const auto& pair : tablet->Stores()) {
            ScanStore(tablet, pair.second);
        }
    }

    void ScanStore(TTablet* tablet, const IStorePtr& store)
    {
        // Preliminary check.
        if (store->GetState() != EStoreState::PassiveDynamic)
            return;

        tablet->GetEpochAutomatonInvoker()->Invoke(
            BIND(&TImpl::FlushStore, MakeStrong(this), tablet, store));
    }

    void FlushStore(TTablet* tablet, const IStorePtr& store)
    {
        // Final check.
        if (store->GetState() != EStoreState::PassiveDynamic)
            return;

        NLog::TTaggedLogger Logger(TabletNodeLogger);
        Logger.AddTag(Sprintf("TabletId: %s, StoreId: %s",
            ~ToString(tablet->GetId()),
            ~ToString(store->GetId())));

        auto* slot = tablet->GetSlot();
        auto hydraManager = slot->GetHydraManager();
        auto tabletManager = slot->GetTabletManager();

        auto automatonInvoker = tablet->GetEpochAutomatonInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        try {
            LOG_INFO("Store flush started");

            store->SetState(EStoreState::Flushing);

            SwitchTo(poolInvoker);

            // Write chunk.
            auto transactionManager = Bootstrap_->GetTransactionManager();
        
            TTransactionPtr transaction;
            {
                LOG_INFO("Creating upload transaction for store flush");
                NTransactionClient::TTransactionStartOptions options;
                options.AutoAbort = false;
                options.Attributes->Set("title", Sprintf("Flush of store %s, tablet %s",
                    ~ToString(store->GetId()),
                    ~ToString(tablet->GetId())));
                auto transactionOrError = WaitFor(transactionManager->Start(options));
                THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
                transaction = transactionOrError.GetValue();
            }
                            
            LOG_INFO("Started writing store chunk");
        
            auto reader = store->CreateReader(
                MinKey().Get(),
                MaxKey().Get(),
                AllCommittedTimestamp,
                TColumnFilter());
        
            // TODO(babenko): need some real writer here!
            IVersionedWriterPtr writer;
        
            std::vector<TVersionedRow> rows;
            while (reader->Read(&rows)) {
                // NB: Memory store reader is always synchronous.
                YCHECK(!rows.empty());
#if 0
                if (!writer->Write(rows)) {
                    auto result = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
#endif
            }
        
            {
#if 0
                auto result = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
#endif
            }
        
            // TODO(babenko): need some real chunk id here!
            TChunkId chunkId(0, NObjectClient::EObjectType::Chunk, 0, 0);

            LOG_INFO("Finished writing store chunk");
        
            SwitchTo(automatonInvoker);

            {
                TReqCommitFlushedChunk request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                ToProto(request.mutable_store_id(), store->GetId());
                ToProto(request.mutable_chunk_id(), chunkId);
                CreateMutation(slot->GetHydraManager(), request)
                    ->Commit();
            }

            // Just abandon the transaction, hopefully it will expire after the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error flushing tablet store, backing off");
        
            SwitchTo(automatonInvoker);

            YCHECK(store->GetState() == EStoreState::Flushing);
            tabletManager->SetStoreFailed(tablet, store, EStoreState::FlushFailed);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TStoreFlusher::TStoreFlusher(
    TStoreFlusherConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TStoreFlusher::~TStoreFlusher()
{ }

void TStoreFlusher::Start()
{
    Impl_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
