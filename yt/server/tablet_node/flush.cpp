#include "stdafx.h"
#include "flush.h"
#include "private.h"
#include "config.h"
#include "dynamic_memory_store.h"
#include "persistent_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_writer.h>

#include <ytlib/api/transaction.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NApi;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

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
    { }

    TFuture<TChunkId> Enqueue(TTablet* tablet, int storeIndex)
    {
        return New<TSession>(this, tablet, storeIndex)->Run();
    }

private:
    TStoreFlusherConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    TThreadPoolPtr ThreadPool_;


    class TSession
        : public TRefCounted
    {
    public:
        explicit TSession(
            TIntrusivePtr<TImpl> owner,
            TTablet* tablet,
            int storeIndex)
            : Owner_(owner)
            , StoreIndex_(storeIndex)
            , Promise_(NewPromise<TChunkId>())
            , Logger(TabletNodeLogger)
        {
            TabletId_ = tablet->GetId();
            Store_ = tablet->PassiveStores()[StoreIndex_];

            auto* slot = tablet->GetSlot();

            TabletManager_ = slot->GetTabletManager();

            AutomatonInvoker_ = slot->GetEpochAutomatonInvoker();
            VERIFY_INVOKER_AFFINITY(AutomatonInvoker_, AutomatonThread);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            YCHECK(tablet->PassiveStores()[StoreIndex_] == Store_);

            ChunkListId_ = tablet->GetChunkListId();

            Logger.AddTag(Sprintf("TabletId: %s", ~ToString(TabletId_)));
        }

        TFuture<TChunkId> Run()
        {
            Start();
            return Promise_;
        }

    private:
        TIntrusivePtr<TImpl> Owner_;
        IStorePtr Store_;
        int StoreIndex_;

        TTabletManagerPtr TabletManager_;
        TTabletId TabletId_;
        TChunkListId ChunkListId_;
        IInvokerPtr AutomatonInvoker_;

        TPromise<TChunkId> Promise_;

        NLog::TTaggedLogger Logger;

        DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
        void DoRun()
        {
            VERIFY_THREAD_AFFINITY_ANY();

            try {
                // Write chunk.
                auto transactionManager = Owner_->Bootstrap_->GetTransactionManager();

                TTransactionPtr transaction;
                {
                    LOG_INFO("Creating upload transaction for store flush");
                    NTransactionClient::TTransactionStartOptions options;
                    options.Attributes->Set("title", Sprintf("Store flush for tablet %s", ~ToString(TabletId_)));
                    auto transactionOrError = WaitFor(transactionManager->Start(options));
                    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
                    transaction = transactionOrError.GetValue();
                }

            
                LOG_INFO("Started writing store chunk");

                auto reader = Store_->CreateReader(
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
                        auto writeResult = WaitFor(writer->GetReadyEvent());
                        THROW_ERROR_EXCEPTION_IF_FAILED(writeResult);
                    }
#endif
                }

                {
#if 0
                    auto closeResult = WaitFor(writer->Close());
                    THROW_ERROR_EXCEPTION_IF_FAILED(closeResult);
#endif
                }

                // TODO(babenko): need some real chunk id here!
                TChunkId chunkId;

                LOG_INFO("Finished writing store chunk");

                {
#if 0
                    LOG_INFO("Attaching flushed store chunk (ChunkId: %s, ChunkListId: %s)",
                        ~ToString(chunkId),
                        ~ToString(ChunkListId_));
                    TObjectServiceProxy proxy(Owner_->Bootstrap_->GetMasterChannel());
                    auto req = TChunkListYPathProxy::Attach(FromObjectId(ChunkListId_));
                    ToProto(req->add_children_ids(), chunkId);
                    auto rsp = WaitFor(proxy.Execute(req));
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error attaching flushed store chunk");
#endif
                }

                {
                    LOG_INFO("Committing store flush transaction");
                    auto commitResult = WaitFor(transaction->Commit());
                    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
                }

                LOG_INFO("Store flush completed");

                Promise_.Set(chunkId);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error flushing tablet store, backing off");

                TDelayedExecutor::Submit(
                    BIND(&TSession::Retry, MakeStrong(this))
                        .Via(AutomatonInvoker_),
                    Owner_->Config_->ErrorBackoffTime);
            }
        }

        void Retry()
        {
            VERIFY_THREAD_AFFINITY(AutomatonThread);
            
            auto* tablet = TabletManager_->FindTablet(TabletId_);
            if (!tablet)
                return;

            Start();
        }

        void Start()
        {
            Owner_->ThreadPool_->GetInvoker()->Invoke(BIND(
                &TSession::DoRun,
                MakeStrong(this)));
        }

    };

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

TFuture<TChunkId> TStoreFlusher::Enqueue(TTablet* tablet, int storeIndex)
{
    return Impl_->Enqueue(tablet, storeIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
