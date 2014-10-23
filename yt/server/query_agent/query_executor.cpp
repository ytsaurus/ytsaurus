#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/concurrency/scheduler.h>

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
#include <ytlib/new_table_client/schemaful_merging_reader.h>
#include <ytlib/new_table_client/pipe.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/private.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/query_statistics.h>

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

using namespace NConcurrency;
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
{
public:
    explicit TQueryExecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Evaluator_(New<TEvaluator>(Config_))
    { }

    TQueryStatistics DoExecute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        auto query = fragment->Query;

        auto groupedSplits = DoSplitAndRegroup(GetPrunedSplits(query, fragment->DataSplits), fragment->NodeDirectory);

        std::vector<TKeyRange> ranges = GetRanges(groupedSplits);

        TConstQueryPtr topquery;
        std::vector<TConstQueryPtr> subqueries;

        std::tie(topquery, subqueries) = CoordinateQuery(query, ranges);

        auto Logger = BuildLogger(fragment->Query);

        std::vector<ISchemafulReaderPtr> splitReaders;
        std::vector<TFuture<TErrorOr<TQueryStatistics>>> subqueriesStatistics;

        for (size_t subqueryIndex = 0; subqueryIndex < subqueries.size(); ++subqueryIndex) {
            if (!groupedSplits[subqueryIndex].empty()) {
                LOG_DEBUG("Delegating subfragment (SubfragmentId: %v)",
                    subqueries[subqueryIndex]->GetId());

                ISchemafulReaderPtr reader;
                TFuture<TErrorOr<TQueryStatistics>> statistics;

                std::vector<ISchemafulReaderPtr> bottomSplitReaders;
                for (const auto& dataSplit : groupedSplits[subqueryIndex]) {
                    bottomSplitReaders.push_back(GetReader(dataSplit, fragment->NodeDirectory));
                }
                auto mergingReader = CreateSchemafulMergingReader(bottomSplitReaders);

                auto pipe = New<TSchemafulPipe>();
                
                auto result = BIND(&TEvaluator::Run, Evaluator_)
                    .Guarded()
                    .AsyncVia(Bootstrap_->GetBoundedConcurrencyQueryPoolInvoker())
                    .Run(subqueries[subqueryIndex], mergingReader, pipe->GetWriter());

                result.Subscribe(BIND([pipe] (TErrorOr<TQueryStatistics> result) {
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                    }
                }));

                splitReaders.push_back(pipe->GetReader());
                subqueriesStatistics.push_back(result);
            }                    
        }

        auto mergingReader = CreateSchemafulMergingReader(splitReaders);

        auto asyncResultOrError = BIND(&TEvaluator::Run, Evaluator_)
            .Guarded()
            .AsyncVia(Bootstrap_->GetBoundedConcurrencyQueryPoolInvoker())
            .Run(topquery, std::move(mergingReader), std::move(writer));

        auto queryStatistics = WaitFor(asyncResultOrError).ValueOrThrow();

        for (auto const& subqueryStatistics : subqueriesStatistics) {
            queryStatistics += subqueryStatistics.Get().ValueOrThrow();
        }
        
        return queryStatistics; 
    }

    // IExecutor implementation.
    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
        return BIND(&TQueryExecutor::DoExecute, MakeStrong(this))
            .Guarded()
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(fragment, std::move(writer));
    }

    TDataSplits SplitFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto tabletId = GetObjectIdFromDataSplit(split);
        YCHECK(TypeFromId(tabletId) == EObjectType::Tablet);

        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = tabletSlotManager->GetTabletSnapshotOrThrow(tabletId);

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        auto keyColumns = GetKeyColumnsFromDataSplit(split);
        auto schema = GetTableSchemaFromDataSplit(split);
        auto timestamp = GetTimestampFromDataSplit(split);

        // Build the initial set of split keys by copying samples.
        // NB: splitKeys[0] < lowerBound is possible.
        auto splitKeys = BuildSplitKeys(tabletSnapshot, lowerBound, upperBound);

        // Cap the number of splits.
        int totalSplitCount = static_cast<int>(splitKeys.size());
        int cappedSplitCount = std::min(totalSplitCount, Config_->MaxSubsplitsPerTablet);

        std::vector<TDataSplit> subsplits;
        for (int index = 0; index < cappedSplitCount; ++index) {
            auto thisIt = splitKeys.begin() + index * totalSplitCount / cappedSplitCount;
            auto nextIt = splitKeys.begin() + (index + 1) * totalSplitCount / cappedSplitCount;

            const auto& thisKey = *thisIt;
            const auto& nextKey = (nextIt == splitKeys.end()) ? MaxKey() : *nextIt;

            TDataSplit subsplit;
            SetObjectId(&subsplit, tabletId);
            SetKeyColumns(&subsplit, keyColumns);
            SetTableSchema(&subsplit, schema);
            SetLowerBound(&subsplit, std::max(lowerBound, thisKey));
            SetUpperBound(&subsplit, std::min(upperBound, nextKey));
            SetTimestamp(&subsplit, timestamp);
            subsplits.push_back(std::move(subsplit));
        }

        return subsplits;
    }

    static std::vector<TOwningKey> BuildSplitKeys(
        TTabletSnapshotPtr tabletSnapshot,
        const TOwningKey& lowerBound,
        const TOwningKey& upperBound)
    {
        // Run binary search to find the relevant partitions.
        const auto& partitions = tabletSnapshot->Partitions;
        YCHECK(lowerBound >= partitions[0]->SampleKeys[0]);
        auto startPartitionIt = std::upper_bound(
            partitions.begin(),
            partitions.end(),
            lowerBound,
            [] (const TOwningKey& lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->SampleKeys[0];
            }) - 1;

        // Construct split keys by copying sample keys.
        std::vector<TOwningKey> result;
        for (auto partitionIt = startPartitionIt; partitionIt != partitions.end(); ++partitionIt) {
            const auto& partition = *partitionIt;
            const auto& sampleKeys = partition->SampleKeys;
            // Run binary search to find the relevant sections of the partition.
            auto startSampleIt = partitionIt == startPartitionIt
                ? std::upper_bound(
                    sampleKeys.begin(),
                    sampleKeys.end(),
                    lowerBound,
                    [] (const TOwningKey& lhs, const TOwningKey& rhs) {
                        return lhs < rhs;
                    }) - 1
                : sampleKeys.begin();

            for (auto sampleIt = startSampleIt; sampleIt != sampleKeys.end(); ++sampleIt) {
                const auto& sampleKey = *sampleIt;
                if (sampleKey >= upperBound) {
                    return result;
                }
                result.push_back(sampleKey);
            }
        }
        return result;
    }

    virtual TGroupedDataSplits Regroup(
        const TDataSplits& splits,
        TNodeDirectoryPtr nodeDirectory)
    {
        std::map<TGuid, TDataSplits> groups;
        TGroupedDataSplits result;

        for (const auto& split : splits) {
            auto tabletId = GetObjectIdFromDataSplit(split);
            if (TypeFromId(tabletId) == EObjectType::Tablet) {
                groups[tabletId].push_back(split);
            } else {
                result.emplace_back(1, split);
            }
        }

        result.reserve(result.size() + groups.size());
        for (const auto& group : groups) {
            result.emplace_back(std::move(group.second));
        }

        return result;
    }

    TGroupedDataSplits DoSplitAndRegroup(
        const TDataSplits& splits,
        TNodeDirectoryPtr nodeDirectory)
    {
        TDataSplits allSplits;
        for (const auto& split : splits) {
            auto objectId = GetObjectIdFromDataSplit(split);
            auto type = TypeFromId(objectId);

            if (type != EObjectType::Tablet) {
                allSplits.push_back(split);
                continue;
            }

            TDataSplits newSplits = SplitFurther(split, nodeDirectory);

            LOG_DEBUG(
                "Got %v splits for input %v",
                newSplits.size(),
                objectId);

            allSplits.insert(allSplits.end(), newSplits.begin(), newSplits.end());
        }

        LOG_DEBUG("Regrouping %v splits", allSplits.size());

        return Regroup(allSplits, nodeDirectory);
    }

    ISchemafulReaderPtr GetReader(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return DoGetChunkReader(split, std::move(nodeDirectory));

            case EObjectType::Tablet:
                return DoGetTabletReader(split, std::move(nodeDirectory));

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

private:
    TQueryAgentConfigPtr Config_;
    TBootstrap* Bootstrap_;
    
    TEvaluatorPtr Evaluator_;

    ISchemafulReaderPtr DoGetChunkReader(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto futureReader = BIND(&TQueryExecutor::DoControlGetChunkReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run(split, std::move(nodeDirectory));
        return New<TLazySchemafulReader>(std::move(futureReader));
    }

    ISchemafulReaderPtr DoControlGetChunkReader(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
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

            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateReplicationReader(
                Bootstrap_->GetConfig()->TabletNode->ChunkReader,
                Bootstrap_->GetBlockStore()->GetCompressedBlockCache(),
                Bootstrap_->GetMasterClient()->GetMasterChannel(),
                nodeDirectory,
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        return CreateSchemafulChunkReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            std::move(chunkReader),
            Bootstrap_->GetUncompressedBlockCache(),
            split.chunk_meta(),
            lowerBound,
            upperBound,
            timestamp);
    }


    ISchemafulReaderPtr DoGetTabletReader(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        try {
            auto tabletId = FromProto<TTabletId>(split.chunk_id());

            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = tabletSlotManager->GetTabletSnapshotOrThrow(tabletId);

            auto lowerBound = GetLowerBoundFromDataSplit(split);
            auto upperBound = GetUpperBoundFromDataSplit(split);
            auto timestamp = GetTimestampFromDataSplit(split);

            LOG_DEBUG("Creating reader for tablet split (TabletId: %v, CellId: %v, LowerBound: {%v}, UpperBound: {%v}, Timestamp: %v)",
                tabletId,
                tabletSnapshot->Slot->GetCellId(),
                lowerBound,
                upperBound,
                timestamp);

            return CreateSchemafulTabletReader(
                Bootstrap_->GetQueryPoolInvoker(),
                std::move(tabletSnapshot),
                std::move(lowerBound),
                std::move(upperBound),
                timestamp);
        } catch (const std::exception& ex) {
            auto futureReader = MakeFuture(TErrorOr<ISchemafulReaderPtr>(ex));
            return New<TLazySchemafulReader>(futureReader);
        }
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

