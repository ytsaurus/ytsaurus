#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <core/misc/string.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/async_reader.h> // TODO(babenko): remove when switched to refcounted macros
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

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

#include <ytlib/tablet_client/public.h>

#include <server/data_node/block_store.h>

#include <server/tablet_node/tablet_cell_controller.h>
#include <server/tablet_node/tablet_manager.h>
#include <server/tablet_node/tablet_slot.h>
#include <server/tablet_node/tablet.h>
#include <server/tablet_node/tablet_reader.h>

#include <server/hydra/hydra_manager.h>

#include <server/data_node/local_chunk_reader.h>

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
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public IExecutor
    , public IEvaluateCallbacks
{
public:
    explicit TQueryExecutor(TBootstrap* bootstrap)
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
        auto asyncResult = BIND(&TQueryExecutor::DoGetReaderControl, MakeStrong(this))
            .Guarded()
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run(split, std::move(context));
        auto resultOrError = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.Value();
    }

private:
    TBootstrap* Bootstrap_;

    IExecutorPtr Evaluator_;


    ISchemedReaderPtr DoGetReaderControl(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return DoGetChunkReaderControl(split, std::move(context));

            case EObjectType::Tablet:
                return DoGetTabletReaderControl(split, std::move(context));

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                    ~FormatEnum(TypeFromId(objectId)).Quote());
        }
    }

    ISchemedReaderPtr DoGetChunkReaderControl(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto chunkId = FromProto<TChunkId>(split.chunk_id());
        auto lowerLimit = FromProto<TReadLimit>(split.lower_limit());
        auto upperLimit = FromProto<TReadLimit>(split.upper_limit());
        auto timestamp = GetTimestampFromDataSplit(split);

        auto chunkReader = CreateLocalChunkReader(Bootstrap_, chunkId);
        if (chunkReader) {
            LOG_DEBUG("Creating local reader for chunk split (ChunkId: %s)",
                ~ToString(chunkId));
        } else {
            LOG_DEBUG("Creating remote reader for chunk split (ChunkId: %s)",
                ~ToString(chunkId));

            auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
            auto masterChannel = Bootstrap_->GetMasterChannel();
            auto nodeDirectory = context->GetNodeDirectory();
            // TODO(babenko): make configurable
            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateReplicationReader(
                New<TReplicationReaderConfig>(),
                std::move(blockCache),
                std::move(masterChannel),
                std::move(nodeDirectory),
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        // TODO(babenko): make configurable
        return CreateSchemedChunkReader(
            New<TChunkReaderConfig>(),
            std::move(chunkReader),
            lowerLimit,
            upperLimit,
            timestamp);
    }


    void ThrowNoSuchTablet(const TTabletId& tabletId)
    {
        THROW_ERROR_EXCEPTION("Tablet %s is not known",
            ~ToString(tabletId));
    }

    ISchemedReaderPtr DoGetTabletReaderControl(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto tabletId = FromProto<TTabletId>(split.chunk_id());
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        auto cellId = tabletCellController->FindCellByTablet(tabletId);
        if (cellId == NullTabletCellId) {
            ThrowNoSuchTablet(tabletId);
        }

        auto slot = tabletCellController->FindSlot(cellId);
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
        return resultOrError.Value();
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

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        auto timestamp = GetTimestampFromDataSplit(split);
        return CreateSchemedTabletReader(
            tablet,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp);
    }

};

IExecutorPtr CreateQueryExecutor(TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

