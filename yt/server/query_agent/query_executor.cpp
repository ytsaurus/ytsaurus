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
#include <server/tablet_node/config.h>

#include <server/hydra/hydra_manager.h>

#include <server/data_node/local_chunk_reader.h>
#include <server/data_node/chunk_registry.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

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
            // NB: Don't bound concurrency here.
            // Doing otherwise may lead to deadlock and makes no sense since coordination
            // is cheap.
            Bootstrap_->GetQueryPoolInvoker(),
            this))
        , Evaluator_(CreateEvaluator(
            Bootstrap_->GetBoundedConcurrencyQueryInvoker(),
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

            auto endIt = std::lower_bound(
                tabletDescriptor->SplitKeys.begin(),
                tabletDescriptor->SplitKeys.end(),
                upperBound,
                [] (const TOwningKey& lhs, const TOwningKey& rhs) {
                    return lhs < rhs;
                });

            int totalSplitCount = std::distance(startIt, endIt);
            int adjustedSplitCount = std::min(totalSplitCount, Config_->MaxSubsplitsPerTablet);

            std::vector<TDataSplit> subsplits;
            for (int index = 0; index < adjustedSplitCount; ++index) {
                auto thisIt = startIt + index * totalSplitCount / adjustedSplitCount;
                auto nextIt = startIt + (index + 1) * totalSplitCount / adjustedSplitCount;

                const auto& thisKey = *thisIt;
                auto nextKey = (nextIt == tabletDescriptor->SplitKeys.end()) ? MaxKey() : *nextIt;

                TDataSplit subsplit;
                SetObjectId(&subsplit, tabletId);
                SetKeyColumns(&subsplit, keyColumns);
                SetTableSchema(&subsplit, schema);
                SetLowerBound(&subsplit, std::max(lowerBound, thisKey));
                SetUpperBound(&subsplit, std::min(upperBound, nextKey));
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
        auto result = Evaluator_->Execute(fragment, pipe->GetWriter());
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
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qv", 
                    TypeFromId(objectId));
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

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        NChunkClient::IReaderPtr chunkReader;
        if (chunk) {
            LOG_DEBUG("Creating local reader for chunk split (ChunkId: %v, LowerBound: {%v}, UpperBound: {%v}, Timestamp: %v)",
                chunkId,
                lowerBound,
                upperBound,
                timestamp);

            chunkReader = CreateLocalChunkReader(
                Bootstrap_,
                Bootstrap_->GetConfig()->TabletNode->ChunkReader,
                chunk);
        } else {
            LOG_DEBUG("Creating remote reader for chunk split (ChunkId: %v, LowerBound: {%v}, UpperBound: {%v}, Timestamp: %v)",
                chunkId,
                lowerBound,
                upperBound,
                timestamp);

            auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
            auto masterChannel = Bootstrap_->GetMasterClient()->GetMasterChannel();
            auto nodeDirectory = context->GetNodeDirectory();
            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateReplicationReader(
                Bootstrap_->GetConfig()->TabletNode->ChunkReader,
                std::move(blockCache),
                std::move(masterChannel),
                std::move(nodeDirectory),
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        return CreateSchemafulChunkReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
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
            THROW_ERROR_EXCEPTION("Cannot query tablet %v while cell is in %Qv state",
                tabletId,
                hydraManager->GetAutomatonState());
        }

        auto tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            ThrowNoSuchTablet(tabletId);
        }

        if (tablet->GetState() != NTabletNode::ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Cannot query tablet %v in %Qv state",
                tabletId,
                tablet->GetState());
        }

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        auto timestamp = GetTimestampFromDataSplit(split);

        LOG_DEBUG("Creating reader for tablet split (TabletId: %v, CellId: %v, LowerBound: {%v}, UpperBound: {%v}, Timestamp: %v)",
            tabletId,
            slot->GetCellGuid(),
            lowerBound,
            upperBound,
            timestamp);

        return CreateSchemafulTabletReader(
            tablet,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp);
    }


    void ThrowNoSuchTablet(const TTabletId& tabletId)
    {
        THROW_ERROR_EXCEPTION("Tablet %v is not known",
            tabletId);
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

