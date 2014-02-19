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
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

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
static const TDuration ScanPeriod = TDuration::Seconds(1);
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

            TChunkId chunkId;
            auto nodeDirectory = New<TNodeDirectory>();
            std::vector<TChunkReplica> replicas;
            std::vector<TNodeDescriptor> targets;
            {
                // TODO(babenko): make configurable
                LOG_INFO("Creating flushed chunk");
                auto req = TMasterYPathProxy::CreateObjects();
                req->set_type(EObjectType::Chunk);
                req->set_account("tmp");
                ToProto(req->mutable_transaction_id(), transaction->GetId());
                auto* reqExt = req->MutableExtension(TReqCreateChunkExt::create_chunk_ext);
                reqExt->set_preferred_host_name(TAddressResolver::Get()->GetLocalHostName());
                reqExt->set_replication_factor(3);
                reqExt->set_upload_replication_factor(2);
                reqExt->set_movable(true);
                reqExt->set_vital(true);
                reqExt->set_erasure_codec(NErasure::ECodec::None);

                auto rsp = WaitFor(proxy.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
                chunkId = FromProto<TChunkId>(rsp->object_ids(0));
                const auto& rspExt = rsp->GetExtension(TRspCreateChunkExt::create_chunk_ext);
                nodeDirectory->MergeFrom(rspExt.node_directory());
                replicas = FromProto<TChunkReplica>(rspExt.replicas());
                if (replicas.size() < 2) {
                    THROW_ERROR_EXCEPTION("Not enough data nodes available: %d received, %d needed",
                        static_cast<int>(replicas.size()),
                        2);
                    return;
                }
                targets = nodeDirectory->GetDescriptors(replicas);
            }
                            
            LOG_INFO("Writing flushed chunk");
        
            auto reader = store->CreateReader(
                MinKey(),
                MaxKey(),
                AllCommittedTimestamp,
                TColumnFilter());
        
            // NB: Memory store reader is always synchronous.
            reader->Open().Get();

            auto chunkWriter = CreateReplicationWriter(
                Config_->Writer,
                chunkId,
                targets);
            chunkWriter->Open();

            auto rowsetWriter = CreateVersionedChunkWriter(
                Config_->Writer,
                New<TChunkWriterOptions>(), // TODO(babenko): make configurable
                tablet->Schema(),
                tablet->KeyColumns(),
                chunkWriter);
        
            std::vector<TVersionedRow> rows;
            rows.reserve(MaxRowsPerRead);

            TOwningKey minKey;
            TOwningKey maxKey;

            while (true) {
                // NB: Memory store reader is always synchronous.
                reader->Read(&rows);
                if (rows.empty())
                    break;
                
                if (!minKey) {
                    minKey = TOwningKey(rows.front().BeginKeys(), rows.front().EndKeys());
                }
                maxKey = TOwningKey(rows.back().BeginKeys(), rows.back().EndKeys());

                if (!rowsetWriter->Write(rows)) {
                    auto result = WaitFor(rowsetWriter->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
            }

            if (!minKey) {
                LOG_INFO("Store is empty, requesting drop");

                SwitchTo(automatonInvoker);

                // Simulate master response.
                TReqCommitFlushedChunk request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                ToProto(request.mutable_store_id(), store->GetId());
                CreateMutation(slot->GetHydraManager(), request)
                    ->Commit();
                return;
            }

            {
                auto result = WaitFor(rowsetWriter->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_INFO("Confirming flushed chunk");
            {
                auto request = TChunkYPathProxy::Confirm(FromObjectId(chunkId));
                *request->mutable_chunk_info() = chunkWriter->GetChunkInfo();
                for (int index : chunkWriter->GetWrittenIndexes()) {
                    request->add_replicas(ToProto<ui32>(replicas[index]));
                }
                *request->mutable_chunk_meta() = rowsetWriter->GetMasterMeta();
                auto rsp = WaitFor(proxy.Execute(request));
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            }

            SwitchTo(automatonInvoker);

            {
                TReqCommitFlushedChunk request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                ToProto(request.mutable_store_id(), store->GetId());
                ToProto(request.mutable_chunk_id(), chunkId);
                ToProto(request.mutable_min_key(), minKey);
                ToProto(request.mutable_max_key(), maxKey);
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
