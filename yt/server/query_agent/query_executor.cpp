#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_chunk_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/pipe.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/api/client.h>

#include <server/data_node/block_store.h>

#include <server/tablet_node/tablet_slot_manager.h>
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
using namespace NVersionedTableClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTabletNode;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TLazySchemafulReader
    : public ISchemafulReader
{
public:
    explicit TLazySchemafulReader(TFuture<TErrorOr<ISchemafulReaderPtr>> futureUnderlyingReader)
        : FutureUnderlyingReader_(std::move(futureUnderlyingReader))
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return FutureUnderlyingReader_.Apply(
            BIND(&TLazySchemafulReader::DoOpen, MakeStrong(this), schema));
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
    TFuture<TErrorOr<ISchemafulReaderPtr>> FutureUnderlyingReader_;

    ISchemafulReaderPtr UnderlyingReader_;


    TAsyncError DoOpen(const TTableSchema& schema, TErrorOr<ISchemafulReaderPtr> readerOrError)
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
    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragment& fragment,
        ISchemafulWriterPtr writer) override
    {
        return Coordinator_->Execute(fragment, std::move(writer));
    }


    // ICoordinateCallbacks implementation.
    virtual bool CanSplit(const TDataSplit& split) override
    {
        auto objectId = GetObjectIdFromDataSplit(split);
        auto type = TypeFromId(objectId);
        return type == EObjectType::Tablet;
    }

    virtual TFuture<TErrorOr<TDataSplits>> SplitFurther(
        const TDataSplit& split,
        TPlanContextPtr context) override
    {
        try {
            auto tabletId = GetObjectIdFromDataSplit(split);
            YCHECK(TypeFromId(tabletId) == EObjectType::Tablet);

            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletDescriptor = tabletSlotManager->FindTabletDescriptor(tabletId);
            if (!tabletDescriptor) {
                ThrowNoSuchTablet(tabletId);
            }

            auto lowerBound = GetLowerBoundFromDataSplit(split);
            auto upperBound = GetUpperBoundFromDataSplit(split);
            auto keyColumns = GetKeyColumnsFromDataSplit(split);
            auto schema = GetTableSchemaFromDataSplit(split);

            // Run binary search to find the relevant partitions.
            auto startIt = std::upper_bound(
                tabletDescriptor->SplitKeys.begin(),
                tabletDescriptor->SplitKeys.end(),
                lowerBound,
                [] (const TOwningKey& lhs, const TOwningKey& rhs) {
                    return lhs < rhs;
                }) - 1;

            std::vector<TDataSplit> subsplits;
            for (auto it = startIt; it != tabletDescriptor->SplitKeys.end(); ++it) {
                const auto& splitKey = *it;
                auto nextSplitKey = (it + 1 == tabletDescriptor->SplitKeys.end()) ? MaxKey() : *(it + 1);
                if (upperBound <= splitKey)
                    break;

                TDataSplit subsplit;
                SetObjectId(&subsplit, tabletId);
                SetKeyColumns(&subsplit, keyColumns);
                SetTableSchema(&subsplit, schema);
                SetLowerBound(&subsplit, std::max(lowerBound, splitKey));
                SetUpperBound(&subsplit, std::min(upperBound, nextSplitKey));
                SetTimestamp(&subsplit, context->GetTimestamp());
                subsplits.push_back(std::move(subsplit));
            }

            return MakeFuture(TErrorOr<TDataSplits>(std::move(subsplits)));
        } catch (const std::exception& ex) {
            return MakeFuture(TErrorOr<TDataSplits>(ex));
        }
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

    virtual std::pair<ISchemafulReaderPtr, TFuture<TErrorOr<TQueryStatistics>>> Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& /*collocatedSplit*/) override
    {
        auto pipe = New<TSchemafulPipe>();
        TFuture<TErrorOr<TQueryStatistics>> result = Evaluator_->Execute(fragment, pipe->GetWriter());
        result.Subscribe(BIND([pipe] (TErrorOr<TQueryStatistics> result) {
            if (!result.IsOK()) {
                pipe->Fail(result);
            }
        }));

        return std::make_pair(pipe->GetReader(), result);
    }

    virtual ISchemafulReaderPtr GetReader(
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


    ISchemafulReaderPtr DoGetChunkReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto futureReader = BIND(&TQueryExecutor::DoControlGetChunkReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run(split, std::move(context));
        return New<TLazySchemafulReader>(std::move(futureReader));
    }

    ISchemafulReaderPtr DoControlGetChunkReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto chunkId = FromProto<TChunkId>(split.chunk_id());
        auto lowerBound = FromProto<TReadLimit>(split.lower_limit());
        auto upperBound = FromProto<TReadLimit>(split.upper_limit());
        auto timestamp = GetTimestampFromDataSplit(split);

        auto chunkReader = CreateLocalChunkReader(Bootstrap_, chunkId);
        if (chunkReader) {
            LOG_DEBUG("Creating local reader for chunk split (ChunkId: %s, LowerBound: {%s}, UpperBound: {%s}, Timestamp: %" PRId64 ")",
                ~ToString(chunkId),
                ~ToString(lowerBound),
                ~ToString(upperBound),
                timestamp);
        } else {
            LOG_DEBUG("Creating remote reader for chunk split (ChunkId: %s, LowerBound: {%s}, UpperBound: {%s}, Timestamp: %" PRId64 ")",
                ~ToString(chunkId),
                ~ToString(lowerBound),
                ~ToString(upperBound),
                timestamp);

            auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
            auto masterChannel = Bootstrap_->GetMasterClient()->GetMasterChannel();
            auto nodeDirectory = context->GetNodeDirectory();
            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateReplicationReader(
                Config_->Reader,
                std::move(blockCache),
                std::move(masterChannel),
                std::move(nodeDirectory),
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        return CreateSchemafulChunkReader(
            Config_->Reader,
            std::move(chunkReader),
            split.chunk_meta(),
            lowerBound,
            upperBound,
            timestamp);
    }


    ISchemafulReaderPtr DoGetTabletReader(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        try {
            auto tabletId = FromProto<TTabletId>(split.chunk_id());

            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletDescriptor = tabletSlotManager->FindTabletDescriptor(tabletId);
            if (!tabletDescriptor) {
                ThrowNoSuchTablet(tabletId);
            }

            const auto& slot = tabletDescriptor->Slot;
            auto invoker = slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Read);
            auto futureReader = BIND(&TQueryExecutor::DoAutomatonGetTabletReader, MakeStrong(this))
                .Guarded()
                .AsyncVia(invoker)
                .Run(tabletId, slot, split, std::move(context));

            return New<TLazySchemafulReader>(futureReader);
        } catch (const std::exception& ex) {
            auto futureReader = MakeFuture(TErrorOr<ISchemafulReaderPtr>(ex));
            return New<TLazySchemafulReader>(futureReader);
        }
    }

    ISchemafulReaderPtr DoAutomatonGetTabletReader(
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

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        auto timestamp = GetTimestampFromDataSplit(split);

        LOG_DEBUG("Creating reader for tablet split (TabletId: %s, CellId: %s, LowerBound: {%s}, UpperBound: {%s}, Timestamp: % " PRId64 ")",
            ~ToString(tabletId),
            ~ToString(slot->GetCellGuid()),
            ~ToString(lowerBound),
            ~ToString(upperBound),
            timestamp);

        return CreateSchemafulTabletReader(
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

