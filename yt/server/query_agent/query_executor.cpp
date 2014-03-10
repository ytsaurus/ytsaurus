#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_chunk_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>
#include <ytlib/new_table_client/pipe.h>

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

class TLazySchemedReader
    : public ISchemedReader
{
public:
    explicit TLazySchemedReader(TFuture<TErrorOr<ISchemedReaderPtr>> futureUnderlyingReader)
        : FutureUnderlyingReader_(std::move(futureUnderlyingReader))
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return FutureUnderlyingReader_.Apply(
            BIND(&TLazySchemedReader::DoOpen, MakeStrong(this), schema));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YASSERT(UnderlyingReader_);
        return UnderlyingReader_->Read(rows);
    }

    virtual TAsyncError GetReadyEvent() override
    {
        YASSERT(UnderlyingReader_);
        return UnderlyingReader_->GetReadyEvent();
    }

private:
    TFuture<TErrorOr<ISchemedReaderPtr>> FutureUnderlyingReader_;

    ISchemedReaderPtr UnderlyingReader_;


    TAsyncError DoOpen(const TTableSchema& schema, TErrorOr<ISchemedReaderPtr> readerOrError)
    {
        if (!readerOrError.IsOK()) {
            return MakeFuture(TError(readerOrError));
        }

        YCHECK(!UnderlyingReader_);
        UnderlyingReader_ = readerOrError.Value();

        return UnderlyingReader_->Open(schema);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public IExecutor
    , public ICoordinateCallbacks
{
public:
    explicit TQueryExecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Coordinator_(CreateCoordinator(
            Bootstrap_->GetQueryWorkerInvoker(),
            this))
        , Evaluator_(CreateEvaluator(
            Bootstrap_->GetQueryWorkerInvoker(),
            this))
    { }


    // IExecutor implementation.
    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
        return Coordinator_->Execute(fragment, std::move(writer));
    }


    // ICoordinateCallbacks implementation.
    virtual bool CanSplit(const TDataSplit& /*split*/) override
    {
        return false;
    }

    virtual TFuture<TErrorOr<TDataSplits>> SplitFurther(
        const TDataSplit& /*split*/,
        TPlanContextPtr /*context*/) override
    {
        YUNREACHABLE();
    }

    virtual TGroupedDataSplits Regroup(
        const TDataSplits& splits,
        TPlanContextPtr context) override
    {
        TGroupedDataSplits result;
        for (const auto& split : splits) {
            result.emplace_back(1, split);
        }
        return result;
    }

    virtual ISchemedReaderPtr Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& /*collocatedSplit*/) override
    {
        auto pipe = New<TSchemedPipe>();
        Evaluator_->Execute(fragment, pipe->GetWriter())
            .Subscribe(BIND([pipe] (TError error) {
                if (!error.IsOK()) {
                    pipe->Fail(error);
                }
            }));
        return pipe->GetReader();
    }

    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& split,
        TPlanContextPtr context) override
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return DoGetChunkReader(split, std::move(context));

            case EObjectType::Tablet:
                return DoGetTabletReader(split, std::move(context));

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                    ~FormatEnum(TypeFromId(objectId)).Quote());
        }
    }

private:
    TQueryAgentConfigPtr Config_;
    TBootstrap* Bootstrap_;
    
    IExecutorPtr Coordinator_;
    IExecutorPtr Evaluator_;


    ISchemedReaderPtr DoGetChunkReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto futureReader = BIND(&TQueryExecutor::DoControlGetChunkReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run(split, std::move(context));
        return New<TLazySchemedReader>(std::move(futureReader));
    }

    ISchemedReaderPtr DoControlGetChunkReader(
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
            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateReplicationReader(
                Config_->ChunkReader,
                std::move(blockCache),
                std::move(masterChannel),
                std::move(nodeDirectory),
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        return CreateSchemedChunkReader(
            Config_->ChunkReader,
            std::move(chunkReader),
            lowerLimit,
            upperLimit,
            timestamp);
    }


    ISchemedReaderPtr DoGetTabletReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        try {
            auto tabletId = FromProto<TTabletId>(split.chunk_id());
            auto tabletCellController = Bootstrap_->GetTabletCellController();
            auto slot = tabletCellController->FindSlotByTabletId(tabletId);
            if (!slot) {
                ThrowNoSuchTablet(tabletId);
            }

            auto invoker = slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Read);
            if (!invoker) {
                ThrowNoSuchTablet(tabletId);
            }

            auto futureReader = BIND(&TQueryExecutor::DoAutomatonGetTabletReader, MakeStrong(this))
                .Guarded()
                .AsyncVia(invoker)
                .Run(tabletId, slot, split, std::move(context));

            if (futureReader.IsCanceled()) {
                ThrowNoSuchTablet(tabletId);
            }

            return New<TLazySchemedReader>(futureReader);
        } catch (const std::exception& ex) {
            auto futureReader = MakeFuture(TErrorOr<ISchemedReaderPtr>(ex));
            return New<TLazySchemedReader>(futureReader);
        }
    }

    ISchemedReaderPtr DoAutomatonGetTabletReader(
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


    void ThrowNoSuchTablet(const TTabletId& tabletId)
    {
        THROW_ERROR_EXCEPTION("Tablet %s is not known",
            ~ToString(tabletId));
    }

};

IExecutorPtr CreateQueryExecutor(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

