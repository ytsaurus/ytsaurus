#include "stdafx.h"
#include "store_flusher.h"
#include "private.h"
#include "config.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "tablet_cell_controller.h"

#include <core/misc/address.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>

#include <core/ytree/attribute_helpers.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_writer.h>
#include <ytlib/new_table_client/versioned_chunk_writer.h>
#include <ytlib/new_table_client/versioned_multi_chunk_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/hydra/mutation.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTransactionClient;
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

static auto& Logger = TabletNodeLogger;
static const size_t MaxRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusher
    : public TRefCounted
{
public:
    TStoreFlusher(
        TStoreFlusherConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->ThreadPoolSize, "StoreFlush"))
    { }

    void Start()
    {
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        tabletCellController->SubscribeSlotScan(BIND(&TStoreFlusher::ScanSlot, MakeStrong(this)));
    }

private:
    TStoreFlusherConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    TThreadPoolPtr ThreadPool_;


    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

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
        if (store->GetState() != EStoreState::PassiveDynamic)
            return;

        store->SetState(EStoreState::Flushing);

        tablet->GetEpochAutomatonInvoker()->Invoke(
            BIND(&TStoreFlusher::FlushStore, MakeStrong(this), tablet, store));
    }

    void FlushStore(TTablet* tablet, IStorePtr store)
    {
        YCHECK(store->GetState() == EStoreState::Flushing);

        NLog::TTaggedLogger Logger(TabletNodeLogger);
        Logger.AddTag(Sprintf("TabletId: %s, StoreId: %s",
            ~ToString(tablet->GetId()),
            ~ToString(store->GetId())));

        auto* slot = tablet->GetSlot();
        auto hydraManager = slot->GetHydraManager();
        auto tabletManager = slot->GetTabletManager();

        auto automatonInvoker = tablet->GetEpochAutomatonInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        TObjectServiceProxy proxy(Bootstrap_->GetMasterChannel());

        try {
            LOG_INFO("Store flush started");

            TReqCommitTabletStoresUpdate updateStoresRequest;
            ToProto(updateStoresRequest.mutable_tablet_id(), tablet->GetId());
            {
                auto* descriptor = updateStoresRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }

            auto reader = store->CreateReader(
                MinKey(),
                MaxKey(),
                AllCommittedTimestamp,
                TColumnFilter());
        
            // NB: Memory store reader is always synchronous.
            YCHECK(reader->Open().Get().IsOK());

            SwitchTo(poolInvoker);

            auto transactionManager = Bootstrap_->GetTransactionManager();
        
            TTransactionPtr transaction;
            {
                LOG_INFO("Creating store flush transaction");
                NTransactionClient::TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Sprintf("Flushing store %s, tablet %s",
                    ~ToString(store->GetId()),
                    ~ToString(tablet->GetId())));
                options.Attributes = attributes.get();
                auto transactionOrError = WaitFor(transactionManager->Start(options));
                THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
                transaction = transactionOrError.Value();
            }
       
            auto writerProvider = New<TVersionedChunkWriterProvider>(
                Config_->Writer,
                New<TChunkWriterOptions>(), // TODO(babenko): make configurable
                tablet->Schema(),
                tablet->KeyColumns());

             // TODO(babenko): make configurable
            auto multiChunkWriterOptions = New<TMultiChunkWriterOptions>();
            multiChunkWriterOptions->Account = "tmp";

            auto writer = New<TVersionedMultiChunkWriter>(
                Config_->Writer,
                multiChunkWriterOptions,
                writerProvider,
                Bootstrap_->GetMasterChannel(),
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
                auto* descriptor = updateStoresRequest.add_stores_to_add();
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
            }

            SwitchTo(automatonInvoker);

            CreateMutation(slot->GetHydraManager(), updateStoresRequest)
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
    TStoreFlusherConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    New<TStoreFlusher>(config, bootstrap)->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
