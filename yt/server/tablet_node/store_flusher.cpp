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

#include <ytlib/api/transaction.h>

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
using namespace NApi;
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

class TStoreFlusher::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TStoreFlusherConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->ThreadPoolSize, "StoreFlush"))
    { }

    void Start()
    {
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        tabletCellController->SubscribeSlotScan(BIND(&TImpl::ScanSlot, MakeStrong(this)));
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

        TObjectServiceProxy proxy(Bootstrap_->GetMasterChannel());

        try {
            LOG_INFO("Store flush started");

            store->SetState(EStoreState::Flushing);

            SwitchTo(poolInvoker);

            // Write chunk.
            auto transactionManager = Bootstrap_->GetTransactionManager();
        
            TTransactionPtr transaction;
            {
                LOG_INFO("Creating store flush transaction");
                NTransactionClient::TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Sprintf("Flush of store %s, tablet %s",
                    ~ToString(store->GetId()),
                    ~ToString(tablet->GetId())));
                options.Attributes = attributes.get();
                auto transactionOrError = WaitFor(transactionManager->Start(options));
                THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
                transaction = transactionOrError.GetValue();
            }
       
            auto reader = store->CreateReader(
                MinKey(),
                MaxKey(),
                AllCommittedTimestamp,
                TColumnFilter());
        
            // NB: Memory store reader is always synchronous.
            reader->Open().Get();

            auto writerProvider = New<TVersionedChunkWriterProvider>(
                Config_->Writer,
                New<TChunkWriterOptions>(), // TODO(babenko): make configurable
                tablet->Schema(),
                tablet->KeyColumns());

             // TODO(babenko): make configurable
            auto multiChunkWriterOptions = New<TMultiChunkWriterOptions>();
            multiChunkWriterOptions->Account = "tmp";

            auto rowsetWriter = New<TVersionedMultiChunkWriter>(
                Config_->Writer,
                multiChunkWriterOptions,
                writerProvider,
                Bootstrap_->GetMasterChannel(),
                transaction->GetId());

            {
                auto result = WaitFor(rowsetWriter->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
        
            std::vector<TVersionedRow> rows;
            rows.reserve(MaxRowsPerRead);

            while (true) {
                // NB: Memory store reader is always synchronous.
                reader->Read(&rows);
                if (rows.empty())
                    break;
                if (!rowsetWriter->Write(rows)) {
                    auto result = WaitFor(rowsetWriter->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
            }

            {
                auto result = WaitFor(rowsetWriter->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            SwitchTo(automatonInvoker);

            {
                TReqCommitFlushedChunks request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                ToProto(request.mutable_store_id(), store->GetId());
                for (const auto& chunkSpec : rowsetWriter->GetWrittenChunks()) {
                    auto* descriptor = request.add_chunks();
                    descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
                    descriptor->set_min_key(boundaryKeysExt.min());
                    descriptor->set_max_key(boundaryKeysExt.max());
                }
                CreateMutation(slot->GetHydraManager(), request)
                    ->Commit();
            }

            LOG_INFO("Store flush finished");

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
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
