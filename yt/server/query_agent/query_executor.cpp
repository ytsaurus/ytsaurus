#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <core/misc/string.h>

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_chunk_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/tablet_client/public.h>

#include <server/data_node/block_store.h>

#include <server/tablet_node/tablet_cell_controller.h>
#include <server/tablet_node/tablet_manager.h>
#include <server/tablet_node/store_manager.h>
#include <server/tablet_node/tablet_slot.h>
#include <server/tablet_node/tablet.h>

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NQueryAgent {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NVersionedTableClient;
using namespace NNodeTrackerClient;
using namespace NTabletNode;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public IExecutor
    , public IEvaluateCallbacks
{
public:
    explicit TQueryExecutor(NCellNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Evaluator_(CreateEvaluator(
            Bootstrap_->GetQueryWorkerInvoker(),
            this))
    { }

    // IExecutor implementation.
    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
        LOG_DEBUG("Executing plan fragment (FragmentId: %s)",
            ~ToString(fragment.Id()));
        return Evaluator_->Execute(fragment, std::move(writer));
    }

    // IExecuteCallbacks implementation.
    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& split,
        TPlanContextPtr context) override
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(split, std::move(context));

            case EObjectType::Tablet: {
                auto asyncResult = BIND(&TQueryExecutor::DoGetTabletReaderControl, MakeStrong(this))
                    .Guarded()
                    .AsyncVia(Bootstrap_->GetControlInvoker())
                    .Run(split, std::move(context));
                auto resultOrError = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
                return resultOrError.GetValue();
            }

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                    ~FormatEnum(TypeFromId(objectId)).Quote());
        }
    }

private:
    NCellNode::TBootstrap* Bootstrap_;

    IExecutorPtr Evaluator_;


    ISchemedReaderPtr GetChunkReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto chunkId = FromProto<TChunkId>(split.chunk_id());
        LOG_DEBUG("Creating reader for chunk split (ChunkId: %s)",
            ~ToString(chunkId));

        auto masterChannel = Bootstrap_->GetMasterChannel();
        auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
        auto nodeDirectory = context->GetNodeDirectory();
        return CreateSchemedChunkReader(
            // TODO(babenko): make configuable
            New<TChunkReaderConfig>(),
            split,
            std::move(masterChannel),
            std::move(nodeDirectory),
            std::move(blockCache));
    }


    void ThrowNoSuchTablet(const TTabletId& tabletId)
    {
        THROW_ERROR_EXCEPTION("No such tablet %s",
            ~ToString(tabletId));
    }

    ISchemedReaderPtr DoGetTabletReaderControl(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto tabletId = FromProto<TTabletId>(split.chunk_id());
        auto cellController = Bootstrap_->GetTabletCellController();
        auto cellId = cellController->FindCellByTablet(tabletId);
        if (cellId == NullTabletCellId) {
            ThrowNoSuchTablet(tabletId);
        }

        auto slot = cellController->FindSlot(cellId);
        if (!slot) {
            ThrowNoSuchTablet(tabletId);
        }

        auto invoker = slot->GetGuardedAutomatonInvoker();
        if (!invoker) {
            ThrowNoSuchTablet(tabletId);
        }

        auto asyncResult = BIND(&TQueryExecutor::DoGetTabletReaderAutomaton, MakeStrong(this))
            .Guarded()
            .AsyncVia(invoker)
            .Run(tabletId, slot, split, std::move(context));

        if (asyncResult.IsCanceled()) {
            ThrowNoSuchTablet(tabletId);
        }

        auto resultOrError = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.GetValue();
    }

    ISchemedReaderPtr DoGetTabletReaderAutomaton(
        const TTabletId& tabletId,
        TTabletSlotPtr slot,
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto hydraManager = slot->GetHydraManager();
        if (hydraManager->GetAutomatonState() != NHydra::EPeerState::Leading) {
            THROW_ERROR_EXCEPTION("Cannot query tablet %s while cell is in %s state",
                ~ToString(tabletId),
                ~FormatEnum(hydraManager->GetAutomatonState()).Quote());
        }

        auto tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            ThrowNoSuchTablet(tabletId);
        }

        if (tablet->GetState() != NTabletNode::ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Cannot query tablet %s in %s state",
                ~ToString(tabletId),
                ~FormatEnum(tablet->GetState()).Quote());
        }

        LOG_DEBUG("Creating reader for tablet split (TabletId: %s, CellId: %s)",
            ~ToString(tabletId),
            ~ToString(slot->GetCellGuid()));

        const auto& storeManager = tablet->GetStoreManager();
        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        // TODO(babenko): timestamp
        return storeManager->CreateReader(
            std::move(lowerBound),
            std::move(upperBound),
            NVersionedTableClient::LastCommittedTimestamp);
    }

};

IExecutorPtr CreateQueryExecutor(NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

