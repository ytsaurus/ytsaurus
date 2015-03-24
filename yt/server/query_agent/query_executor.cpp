#include "stdafx.h"
#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/chunk_reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_chunk_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/unordered_schemaful_reader.h>
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
#include <ytlib/query_client/function_registry.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/api/client.h>

#include <server/data_node/block_store.h>

#include <server/tablet_node/slot_manager.h>
#include <server/tablet_node/tablet_manager.h>
#include <server/tablet_node/tablet_slot.h>
#include <server/tablet_node/tablet.h>
#include <server/tablet_node/tablet_reader.h>
#include <server/tablet_node/security_manager.h>
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
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TLazySchemafulReader
    : public ISchemafulReader
{
public:
    explicit TLazySchemafulReader(TFuture<ISchemafulReaderPtr> futureUnderlyingReader)
        : FutureUnderlyingReader_(std::move(futureUnderlyingReader))
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        return FutureUnderlyingReader_.Apply(
            BIND(&TLazySchemafulReader::DoOpen, MakeStrong(this), schema));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YASSERT(UnderlyingReader_);
        return UnderlyingReader_->Read(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        YASSERT(UnderlyingReader_);
        return UnderlyingReader_->GetReadyEvent();
    }

private:
    TFuture<ISchemafulReaderPtr> FutureUnderlyingReader_;

    ISchemafulReaderPtr UnderlyingReader_;


    TFuture<void> DoOpen(const TTableSchema& schema, const TErrorOr<ISchemafulReaderPtr>& readerOrError)
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

class TRemoteExecutor
    : public IExecutor
{
public:
    explicit TRemoteExecutor(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TFuture<TQueryStatistics> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
        auto executor = Bootstrap_->GetMasterClient()->GetQueryExecutor();
        return executor->Execute(fragment, std::move(writer));
    }

private:
    TBootstrap* const Bootstrap_;
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
        , RemoteExecutor_(New<TRemoteExecutor>(bootstrap))
    { }

    // IExecutor implementation.
    virtual TFuture<TQueryStatistics> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        return BIND(&TQueryExecutor::DoExecute, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(fragment, std::move(writer), maybeUser);
    }

private:
    const TQueryAgentConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const IExecutorPtr RemoteExecutor_;


    TQueryStatistics DoExecute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto timestamp = fragment->Timestamp;

        auto nodeDirectory = fragment->NodeDirectory;
        auto Logger = BuildLogger(fragment->Query);

        LOG_DEBUG("Splitting %v sources", fragment->DataSources.size());

        auto splits = Split(fragment->DataSources, nodeDirectory, Logger, fragment->VerboseLogging);
        int splitCount = splits.size();
        int splitOffset = 0;
        std::vector<TDataSources> groupedSplits;

        LOG_DEBUG("Grouping %v splits", splits.size());

        for (int queryIndex = 1; queryIndex <= Config_->MaxSubqueries; ++queryIndex) {
            int nextSplitOffset = queryIndex * splitCount / Config_->MaxSubqueries;
            if (splitOffset != nextSplitOffset) {
                groupedSplits.emplace_back(splits.begin() + splitOffset, splits.begin() + nextSplitOffset);
                splitOffset = nextSplitOffset;
            }
        }

        LOG_DEBUG("Grouped into %v groups", groupedSplits.size());
        auto ranges = GetRanges(groupedSplits);

        Stroka rangesString;
        for (const auto& range : ranges) {
            rangesString += Format("[%v .. %v]", range.first, range.second);
        }

        LOG_DEBUG("Got ranges for groups %v", rangesString);

        auto functionRegistry = CreateFunctionRegistry(Bootstrap_->GetMasterClient());

        return CoordinateAndExecute(
            fragment,
            writer,
            false,
            ranges,
            [&] (const TConstQueryPtr& subquery, int index) {
                std::vector<ISchemafulReaderPtr> bottomSplitReaders;
                for (const auto& dataSplit : groupedSplits[index]) {
                    bottomSplitReaders.push_back(GetReader(dataSplit, timestamp, nodeDirectory));
                }
                auto mergingReader = CreateUnorderedSchemafulReader(bottomSplitReaders);

                auto pipe = New<TSchemafulPipe>();

                LOG_DEBUG("Evaluating subquery (SubqueryId: %v)", subquery->Id);

                auto asyncStatistics = BIND(&TEvaluator::RunWithExecutor, Evaluator_)
                    .AsyncVia(Bootstrap_->GetBoundedConcurrencyQueryPoolInvoker())
                    .Run(subquery, mergingReader, pipe->GetWriter(), [&] (const TQueryPtr& subquery, ISchemafulWriterPtr writer) -> TQueryStatistics {
                        LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                        auto planFragment = New<TPlanFragment>();
                        planFragment->NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
                        planFragment->Timestamp = fragment->Timestamp;
                        planFragment->DataSources.push_back(fragment->ForeignDataSource);
                        planFragment->Query = subquery;
                        planFragment->VerboseLogging = fragment->VerboseLogging;

                        auto subqueryResult = RemoteExecutor_->Execute(planFragment, writer);

                        return WaitFor(subqueryResult)
                            .ValueOrThrow();
                    },
                    functionRegistry);

                asyncStatistics.Subscribe(BIND([=] (const TErrorOr<TQueryStatistics>& result) {
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                        LOG_DEBUG(result, "Failed evaluating subquery (SubqueryId: %v)", subquery->Id);
                    }
                }));

                return std::make_pair(pipe->GetReader(), asyncStatistics);
            },
            [&] (const TConstQueryPtr& topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                LOG_DEBUG("Evaluating topquery (TopqueryId: %v)", topQuery->Id);

                auto asyncQueryStatisticsOrError = BIND(&TEvaluator::Run, Evaluator_)
                    .AsyncVia(Bootstrap_->GetBoundedConcurrencyQueryPoolInvoker())
                    .Run(topQuery, std::move(reader), std::move(writer), functionRegistry);

                auto result = WaitFor(asyncQueryStatisticsOrError);
                LOG_DEBUG(result, "Finished evaluating topquery (TopqueryId: %v)", topQuery->Id);
                return result.ValueOrThrow();
            });
    }

    TDataSources Split(
        const TDataSources& splits,
        TNodeDirectoryPtr nodeDirectory,
        const NLogging::TLogger& Logger,
        bool verboseLogging)
    {
        yhash_map<TGuid, std::vector<TKeyRange>> rangesByTablet;
        TDataSources allSplits;
        for (const auto& split : splits) {
            auto objectId = split.Id;
            auto type = TypeFromId(objectId);

            if (type == EObjectType::Tablet) {
                rangesByTablet[objectId].push_back(split.Range);
            } else {
                allSplits.push_back(split);
            }
        }

        auto securityManager = Bootstrap_->GetSecurityManager();

        for (auto& tabletIdRange : rangesByTablet) {
            auto tabletId = tabletIdRange.first;
            auto& keyRanges = tabletIdRange.second;

            YCHECK(!keyRanges.empty());

            std::sort(keyRanges.begin(), keyRanges.end(), [] (const TKeyRange& lhs, const TKeyRange& rhs) {
                return lhs.first < rhs.first;
            });

            auto slotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

            securityManager->ValidatePermission(tabletSnapshot, NYTree::EPermission::Read);

            int lastIndex = 0;
            std::vector<std::pair<TOwningKey, TOwningKey>> resultRanges;
            for (int index = 1; index < keyRanges.size(); ++index) {
                auto lowerBound = keyRanges[index].first;
                auto upperBound = keyRanges[index - 1].second;

                int totalSampleCount, partitionCount;
                std::tie(totalSampleCount, partitionCount) = GetBoundSampleKeys(tabletSnapshot, upperBound, lowerBound);

                if (totalSampleCount != 0 || partitionCount != 0) {
                    resultRanges.emplace_back(keyRanges[lastIndex].first, upperBound);

                    LOG_DEBUG_IF(verboseLogging, "Merging %v ranges into %v", 
                        index - lastIndex,
                        Format("[%v .. %v]", keyRanges[lastIndex].first, upperBound));

                    lastIndex = index;
                }
            }

            resultRanges.emplace_back(keyRanges[lastIndex].first, keyRanges.back().second);

            int totalSampleCount = 0;
            int totalPartitionCount = 0;
            for (const auto& range : resultRanges) {
                int sampleCount, partitionCount;
                std::tie(sampleCount, partitionCount) = GetBoundSampleKeys(tabletSnapshot, range.first, range.second);
                totalSampleCount += sampleCount;
                totalPartitionCount += partitionCount;
            }

            int freeSlotCount = std::max(0, Config_->MaxSubsplitsPerTablet - totalPartitionCount);
            int cappedSampleCount = std::min(freeSlotCount, totalSampleCount);

            int nextSampleIndex = 1;
            int currentSampleCount = 1;
            for (const auto& range : resultRanges) {
                auto splitKeys = BuildSplitKeys(
                    tabletSnapshot,
                    range.first,
                    range.second,
                    nextSampleIndex,
                    currentSampleCount,
                    totalSampleCount,
                    cappedSampleCount);

                for (int splitKeyIndex = 0; splitKeyIndex < splitKeys.size(); ++splitKeyIndex) {
                    const auto& thisKey = splitKeys[splitKeyIndex];
                    const auto& nextKey = (splitKeyIndex == splitKeys.size() - 1)
                        ? MaxKey()
                        : splitKeys[splitKeyIndex + 1];
                    TDataSource subsource{tabletId, TKeyRange(std::max(range.first, thisKey), std::min(range.second, nextKey))};

                    allSplits.push_back(std::move(subsource));
                }
            }
        }

        return allSplits;
    }

    std::pair<int, int> GetBoundSampleKeys(
        TTabletSnapshotPtr tabletSnapshot,
        const TOwningKey& lowerBound,
        const TOwningKey& upperBound)
    {
        auto findStartSample = [&] (const std::vector<TOwningKey>& sampleKeys) {
            return std::upper_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                lowerBound);
        };
        auto findEndSample = [&] (const std::vector<TOwningKey>& sampleKeys) {
            return std::lower_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                upperBound);
        };

        // Run binary search to find the relevant partitions.
        const auto& partitions = tabletSnapshot->Partitions;
        YCHECK(!partitions.empty());
        YCHECK(lowerBound >= partitions[0]->PivotKey);
        auto startPartitionIt = std::upper_bound(
            partitions.begin(),
            partitions.end(),
            lowerBound,
            [] (const TOwningKey& lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, const TOwningKey& rhs) {
                return lhs->PivotKey < rhs;
            });
        int partitionCount = std::distance(startPartitionIt, endPartitionIt) - 1;

        int totalSampleCount = 0;
        for (auto partitionIt = startPartitionIt; partitionIt != endPartitionIt; ++partitionIt) {
            const auto& partition = *partitionIt;
            const auto& sampleKeys = partition->SampleKeys->Keys;
            auto startSampleIt = partitionIt == startPartitionIt && !sampleKeys.empty()
                ? findStartSample(sampleKeys)
                : sampleKeys.begin();
            auto endSampleIt = partitionIt + 1 == endPartitionIt
                ? findEndSample(sampleKeys)
                : sampleKeys.end();

            totalSampleCount += std::distance(startSampleIt, endSampleIt);
        }

        return std::make_pair(totalSampleCount, partitionCount);
    }

    std::vector<TOwningKey> BuildSplitKeys(
        TTabletSnapshotPtr tabletSnapshot,
        const TOwningKey& lowerBound,
        const TOwningKey& upperBound,
        int& nextSampleIndex,
        int& currentSampleCount,
        int totalSampleCount,
        int cappedSampleCount)
    {
        auto findStartSample = [&] (const std::vector<TOwningKey>& sampleKeys) {
            return std::upper_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                lowerBound);
        };
        auto findEndSample = [&] (const std::vector<TOwningKey>& sampleKeys) {
            return std::lower_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                upperBound);
        };

        // Run binary search to find the relevant partitions.
        const auto& partitions = tabletSnapshot->Partitions;
        YCHECK(lowerBound >= partitions[0]->PivotKey);
        auto startPartitionIt = std::upper_bound(
            partitions.begin(),
            partitions.end(),
            lowerBound,
            [] (const TOwningKey& lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, const TOwningKey& rhs) {
                return lhs->PivotKey < rhs;
            });
        int partitionCount = std::distance(startPartitionIt, endPartitionIt);

        int nextSampleCount = cappedSampleCount != 0
            ? nextSampleIndex * totalSampleCount / cappedSampleCount
            : 0;

        // Fill results with pivotKeys and up to cappedSampleCount sampleKeys.
        std::vector<TOwningKey> result;
        result.reserve(partitionCount + cappedSampleCount);
        for (auto partitionIt = startPartitionIt; partitionIt != endPartitionIt; ++partitionIt) {
            const auto& partition = *partitionIt;
            const auto& sampleKeys = partition->SampleKeys->Keys;
            auto startSampleIt = partitionIt == startPartitionIt && !sampleKeys.empty()
                ? findStartSample(sampleKeys)
                : sampleKeys.begin();
            auto endSampleIt = partitionIt == endPartitionIt - 1
                ? findEndSample(sampleKeys)
                : sampleKeys.end();

            result.push_back(partition->PivotKey);

            if (cappedSampleCount == 0) {
                continue;
            }

            for (auto sampleIt = startSampleIt; sampleIt < endSampleIt;) {
                if (currentSampleCount == nextSampleCount) {
                    ++nextSampleIndex;
                    nextSampleCount = nextSampleIndex * totalSampleCount / cappedSampleCount;
                    result.push_back(*sampleIt);
                }
                int samplesLeft = static_cast<int>(std::distance(sampleIt, endSampleIt));
                int step = std::min(samplesLeft, nextSampleCount - currentSampleCount);
                YCHECK(step > 0);
                sampleIt += step;
                currentSampleCount += step;
            }
        }
        return result;
    }

    ISchemafulReaderPtr GetReader(
        const TDataSource& source,
        TTimestamp timestamp,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto objectId = source.Id;
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(source, timestamp, std::move(nodeDirectory));

            case EObjectType::Tablet:
                return GetTabletReader(source, timestamp, std::move(nodeDirectory));

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetChunkReader(
        const TDataSource& source,
        TTimestamp timestamp,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto futureReader = BIND(&TQueryExecutor::GetChunkReaderControl, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run(source, timestamp, std::move(nodeDirectory));
        return New<TLazySchemafulReader>(std::move(futureReader));
    }

    ISchemafulReaderPtr GetChunkReaderControl(
        const TDataSource& source,
        TTimestamp timestamp,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto chunkId = source.Id;
        auto lowerBound = source.Range.first;
        auto upperBound = source.Range.second;

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        NChunkClient::IChunkReaderPtr chunkReader;
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
                Bootstrap_->GetMasterClient()->GetMasterChannel(NApi::EMasterChannelKind::LeaderOrFollower),
                nodeDirectory,
                Bootstrap_->GetLocalDescriptor(),
                chunkId);
        }

        auto chunkMeta = WaitFor(chunkReader->GetMeta()).ValueOrThrow();

        TReadLimit lowerReadLimit;
        lowerReadLimit.SetKey(lowerBound);

        TReadLimit upperReadLimit;
        upperReadLimit.SetKey(upperBound);

        return CreateSchemafulChunkReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            std::move(chunkReader),
            Bootstrap_->GetUncompressedBlockCache(),
            chunkMeta,
            lowerReadLimit,
            upperReadLimit,
            timestamp);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TDataSource& source,
        TTimestamp timestamp,
        TNodeDirectoryPtr nodeDirectory)
    {
        try {
            auto tabletId = source.Id;

            auto slotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

            auto securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(tabletSnapshot, NYTree::EPermission::Read);

            auto lowerBound = source.Range.first;
            auto upperBound = source.Range.second;

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

