#include "query_executor.h"
#include "private.h"
#include "config.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/chunk.h>
#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/local_chunk_reader.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/security_manager.h>
#include <yt/server/tablet_node/slot_manager.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/tablet_reader.h>
#include <yt/server/tablet_node/tablet_slot.h>

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/coordinator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/plan_fragment.h>
#include <yt/ytlib/query_client/plan_helpers.h>
#include <yt/ytlib/query_client/private.h>
#include <yt/ytlib/query_client/query_statistics.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/pipe.h>
#include <yt/ytlib/table_client/schemaful_chunk_reader.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/string.h>

namespace NYT {
namespace NQueryAgent {

using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTabletNode;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TColumnFilter GetColumnFilter(const TTableSchema& desiredSchema, const TTableSchema& tabletSchema)
{
    // Infer column filter.
    TColumnFilter columnFilter;
    columnFilter.All = false;
    for (const auto& column : desiredSchema.Columns()) {
        const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name);
        if (tabletColumn.Type != column.Type) {
            THROW_ERROR_EXCEPTION("Mismatched type of column %Qv in schema: expected %Qlv, found %Qlv",
                column.Name,
                tabletColumn.Type,
                column.Type);
        }
        columnFilter.Indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
    }

    return columnFilter;
}

struct TDataSourceFormatter
{
    void operator()(TStringBuilder* builder, const NQueryClient::TDataRange& source) const
    {
        builder->AppendFormat("[%v .. %v]",
            source.Range.first,
            source.Range.second);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TQueryExecutorBufferTag
{ };

class TQueryExecutor
    : public ISubexecutor
{
public:
    TQueryExecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , FunctionImplCache_(CreateFunctionImplCache(
            config->FunctionImplCache,
            bootstrap->GetMasterClient()))
        , Bootstrap_(bootstrap)
        , Evaluator_(New<TEvaluator>(Config_))
        , ColumnEvaluatorCache_(Bootstrap_->GetMasterClient()->GetConnection()->GetColumnEvaluatorCache())
    { }

    // IExecutor implementation.
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TQueryOptions& options) override
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        auto execute = query->IsOrdered()
            ? &TQueryExecutor::DoExecuteOrdered
            : &TQueryExecutor::DoExecuteUnordered;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(
                std::move(query),
                std::move(externalCGInfo),
                std::move(dataSources),
                options,
                std::move(writer),
                maybeUser);
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    typedef std::function<ISchemafulReaderPtr()> TSubreaderCreator;

    TQueryStatistics DoCoordinateAndExecute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        const TQueryOptions& options,
        ISchemafulWriterPtr writer,
        const std::vector<TRefiner>& refiners,
        const std::vector<TSubreaderCreator>& subreaderCreators)
    {
        auto Logger = BuildLogger(query);

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        NApi::TClientOptions clientOptions;
        if (maybeUser) {
            clientOptions.User = maybeUser.Get();
        }

        auto client = Bootstrap_->GetMasterClient()->GetConnection()
            ->CreateClient(clientOptions);

        auto remoteExecutor = client->GetQueryExecutor();

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), BuiltinFunctionCG.Get());
        MergeFrom(aggregateGenerators.Get(), BuiltinAggregateCG.Get());
        FetchImplementations(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_);

        return CoordinateAndExecute(
            query,
            writer,
            refiners,
            [&] (TConstQueryPtr subquery, int index) {
                auto mergingReader = subreaderCreators[index]();

                auto pipe = New<TSchemafulPipe>();

                LOG_DEBUG("Evaluating subquery (SubqueryId: %v)", subquery->Id);

                auto asyncSubqueryResults = std::make_shared<std::vector<TFuture<TQueryStatistics>>>();

                auto foreignExecuteCallback = [
                    asyncSubqueryResults,
                    externalCGInfo,
                    options,
                    remoteExecutor,
                    Logger
                ] (
                    const TQueryPtr& subquery,
                    TDataRanges dataRanges,
                    ISchemafulWriterPtr writer)
                {
                    LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                    TQueryOptions subqueryOptions;
                    subqueryOptions.Timestamp = options.Timestamp;
                    subqueryOptions.VerboseLogging = options.VerboseLogging;

                    asyncSubqueryResults->push_back(remoteExecutor->Execute(
                        subquery,
                        externalCGInfo,
                        std::move(dataRanges),
                        writer,
                        subqueryOptions));
                };

                auto asyncStatistics = BIND(&TEvaluator::RunWithExecutor, Evaluator_)
                    .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
                    .Run(subquery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignExecuteCallback,
                        functionGenerators,
                        aggregateGenerators,
                        options.EnableCodeCache);

                asyncStatistics.Apply(BIND([=] (const TErrorOr<TQueryStatistics>& result) -> TErrorOr<TQueryStatistics>{
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                        LOG_DEBUG(result, "Failed evaluating subquery (SubqueryId: %v)", subquery->Id);
                        return result;
                    } else {
                        TQueryStatistics statistics = result.Value();

                        for (const auto& asyncSubqueryResult : *asyncSubqueryResults) {
                            auto subqueryStatistics = WaitFor(asyncSubqueryResult)
                                .ValueOrThrow();

                            LOG_DEBUG("Remote subquery statistics %v", subqueryStatistics);
                            statistics += subqueryStatistics;
                        }

                        return statistics;
                    }
                }));

                return std::make_pair(pipe->GetReader(), asyncStatistics);
            },
            [&] (TConstQueryPtr topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                LOG_DEBUG("Evaluating top query (TopQueryId: %v)", topQuery->Id);
                auto result = Evaluator_->Run(
                    topQuery,
                    std::move(reader),
                    std::move(writer),
                    functionGenerators,
                    aggregateGenerators,
                    options.EnableCodeCache);
                LOG_DEBUG("Finished evaluating top query (TopQueryId: %v)", topQuery->Id);
                return result;
            });
    }

    TQueryStatistics DoExecuteUnordered(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        const TQueryOptions& options,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto Logger = BuildLogger(query);

        LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        std::vector<TDataRanges> rangesByTablePart;
        std::vector<TDataKeys> keysByTablePart;

        auto keySize = query->KeyColumnsCount;

        for (const auto& source : dataSources) {
            TRowRanges rowRanges;
            std::vector<TRow> keys;

            for (const auto& range : source.Ranges) {
                auto lowerBound = range.first;
                auto upperBound = range.second;

                if (source.LookupSupported &&
                    keySize == lowerBound.GetCount() &&
                    keySize + 1 == upperBound.GetCount() &&
                    upperBound[keySize].Type == EValueType::Max &&
                    CompareRows(lowerBound.Begin(), lowerBound.End(), upperBound.Begin(), upperBound.Begin() + keySize) == 0)
                {
                    keys.push_back(lowerBound);
                } else {
                    rowRanges.push_back(range);
                }
            }

            if (!rowRanges.empty()) {
                TDataRanges item;
                item.Id = source.Id;
                item.Ranges = MakeSharedRange(std::move(rowRanges), source.Ranges.GetHolder());
                item.LookupSupported = source.LookupSupported;
                rangesByTablePart.emplace_back(std::move(item));
            }
            if (!keys.empty()) {
                TDataKeys item;
                item.Id = source.Id;
                item.Keys = MakeSharedRange(std::move(keys), source.Ranges.GetHolder());
                keysByTablePart.emplace_back(std::move(item));
            }
        }

        LOG_DEBUG("Splitting sources");

        auto splits = Split(std::move(rangesByTablePart), Logger, options.VerboseLogging);
        int splitCount = splits.Size();
        int splitOffset = 0;
        std::vector<TSharedRange<TDataRange>> groupedSplits;

        LOG_DEBUG("Grouping %v splits", splitCount);

        auto maxSubqueries = std::min(options.MaxSubqueries, Config_->MaxSubqueries);

        for (int queryIndex = 1; queryIndex <= maxSubqueries; ++queryIndex) {
            int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            if (splitOffset != nextSplitOffset) {
                std::vector<TDataRange> subsplit(splits.begin() + splitOffset, splits.begin() + nextSplitOffset);
                groupedSplits.emplace_back(MakeSharedRange(std::move(subsplit), splits.GetHolder()));
                splitOffset = nextSplitOffset;
            }
        }

        LOG_DEBUG("Got %v split groups", groupedSplits.size());

        auto columnEvaluator = ColumnEvaluatorCache_->Find(query->TableSchema);

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (auto& groupedSplit : groupedSplits) {
            std::vector<TRowRange> keyRanges;
            for (const auto& dataRange : groupedSplit) {
                keyRanges.push_back(dataRange.Range);
            }

            refiners.push_back([MOVE(keyRanges)] (
                TConstExpressionPtr expr,
                const TKeyColumns& keyColumns)
            {
                return EliminatePredicate(keyRanges, expr, keyColumns);
            });
            subreaderCreators.push_back([&, MOVE(groupedSplit)] () {
                if (options.VerboseLogging) {
                    LOG_DEBUG("Generating reader for ranges %v",
                        MakeFormattableRange(groupedSplit, TDataSourceFormatter()));
                } else {
                    LOG_DEBUG("Generating reader for %v ranges",
                        groupedSplit.Size());
                }

                auto bottomSplitReaderGenerator = [
                    Logger,
                    query,
                    MOVE(groupedSplit),
                    options,
                    index = 0,
                    this,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedSplit.Size()) {
                        return nullptr;
                    }

                    const auto& group = groupedSplit[index++];
                    return GetReader(
                        query->TableSchema,
                        group.Id,
                        group.Range,
                        options);
                };

                return CreateUnorderedSchemafulReader(
                    std::move(bottomSplitReaderGenerator),
                    Config_->MaxBottomReaderConcurrency);
            });
        }

        for (auto& keySource : keysByTablePart) {
            const auto& tablePartId = keySource.Id;
            auto& keys = keySource.Keys;

            refiners.push_back([&] (TConstExpressionPtr expr, const TKeyColumns& keyColumns) {
                return EliminatePredicate(keys, expr, keyColumns);
            });
            subreaderCreators.push_back([&, MOVE(keys)] () {
                ValidateReadTimestamp(options.Timestamp);

                std::function<ISchemafulReaderPtr()> bottomSplitReaderGenerator;

                switch (TypeFromId(tablePartId)) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk: {
                        return GetChunkReader(
                            query->TableSchema,
                            tablePartId,
                            keys,
                            options);
                    }

                    case EObjectType::Tablet: {
                        return GetTabletReader(
                            query->TableSchema,
                            tablePartId,
                            keys,
                            options);
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                            TypeFromId(tablePartId));
                }
            });
        }

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    TQueryStatistics DoExecuteOrdered(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        const TQueryOptions& options,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto Logger = BuildLogger(query);

        auto splits = Split(std::move(dataSources), Logger, options.VerboseLogging);

        LOG_DEBUG("Sorting %v splits", splits.Size());

        std::sort(splits.Begin(), splits.End(), [] (const TDataRange& lhs, const TDataRange& rhs) {
            return lhs.Range.first < rhs.Range.first;
        });

        if (options.VerboseLogging) {
            LOG_DEBUG("Got ranges for groups %v",
                MakeFormattableRange(splits, TDataSourceFormatter()));
        } else {
            LOG_DEBUG("Got ranges for %v groups",
                splits.Size());
        }

        auto columnEvaluator = ColumnEvaluatorCache_->Find(
            query->TableSchema);

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (const auto& dataSplit : splits) {
            refiners.push_back([&] (TConstExpressionPtr expr, const TKeyColumns& keyColumns) {
                return EliminatePredicate(MakeRange(&dataSplit.Range, 1), expr, keyColumns);
            });
            subreaderCreators.push_back([&] () {
                return GetReader(query->TableSchema, dataSplit.Id, dataSplit.Range, options);
            });
        }

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    // TODO(lukyan): Use mutable shared range
    TSharedMutableRange<TDataRange> Split(
        std::vector<TDataRanges> rangesByTablePart,
        const NLogging::TLogger& Logger,
        bool verboseLogging)
    {
        auto rowBuffer = New<TRowBuffer>(TQueryExecutorBufferTag());
        std::vector<TDataRange> allSplits;

        for (auto& tablePartIdRange : rangesByTablePart) {
            auto tablePartId = tablePartIdRange.Id;
            auto& keyRanges = tablePartIdRange.Ranges;

            if (TypeFromId(tablePartId) != EObjectType::Tablet) {
                for (const auto& range : keyRanges) {
                    allSplits.push_back(TDataRange{tablePartId, TRowRange(
                        rowBuffer->Capture(range.first),
                        rowBuffer->Capture(range.second)
                    )});
                }
                continue;
            }

            YCHECK(!keyRanges.Empty());

            YCHECK(std::is_sorted(
                keyRanges.Begin(),
                keyRanges.End(),
                [] (const TRowRange& lhs, const TRowRange& rhs) {
                    return lhs.first < rhs.first;
                }));

            auto slotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tablePartId);

            std::vector<TRowRange> resultRanges;
            int lastIndex = 0;

            auto addRange = [&] (int count, TUnversionedRow lowerBound, TUnversionedRow upperBound) {
                LOG_DEBUG_IF(verboseLogging, "Merging %v ranges into [%v .. %v]",
                    count,
                    lowerBound,
                    upperBound);
                resultRanges.emplace_back(lowerBound, upperBound);
            };

            for (int index = 1; index < keyRanges.Size(); ++index) {
                auto lowerBound = keyRanges[index].first;
                auto upperBound = keyRanges[index - 1].second;

                int totalSampleCount, partitionCount;
                std::tie(totalSampleCount, partitionCount) = GetBoundSampleKeys(tabletSnapshot, upperBound, lowerBound);
                YCHECK(partitionCount > 0);

                if (totalSampleCount != 0 || partitionCount != 1) {
                    addRange(index - lastIndex, keyRanges[lastIndex].first, upperBound);
                    lastIndex = index;
                }
            }

            addRange(
                keyRanges.Size() - lastIndex,
                keyRanges[lastIndex].first,
                keyRanges[keyRanges.Size() - 1].second);

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
                    allSplits.push_back({tablePartId, TRowRange(
                        rowBuffer->Capture(std::max(range.first, thisKey.Get())),
                        rowBuffer->Capture(std::min(range.second, nextKey.Get()))
                    )});
                }
            }
        }

        return MakeSharedMutableRange(std::move(allSplits), rowBuffer);
    }

    std::pair<int, int> GetBoundSampleKeys(
        const TTabletSnapshotPtr& tabletSnapshot,
        TKey lowerBound,
        TKey upperBound)
    {
        YCHECK(lowerBound <= upperBound);

        if (!tabletSnapshot->TableSchema.IsSorted()) {
            return std::make_pair(0, 1);
        }

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
        const auto& partitions = tabletSnapshot->PartitionList;
        YCHECK(!partitions.empty());
        YCHECK(lowerBound >= partitions[0]->PivotKey);
        auto startPartitionIt = std::upper_bound(
            partitions.begin(),
            partitions.end(),
            lowerBound,
            [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, TRow rhs) {
                return lhs->PivotKey < rhs;
            });
        int partitionCount = std::distance(startPartitionIt, endPartitionIt);

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
        const TTabletSnapshotPtr& tabletSnapshot,
        TRow lowerBound,
        TRow upperBound,
        int& nextSampleIndex,
        int& currentSampleCount,
        int totalSampleCount,
        int cappedSampleCount)
    {
        if (!tabletSnapshot->TableSchema.IsSorted()) {
            return {TOwningKey(lowerBound)};
        }

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
        const auto& partitions = tabletSnapshot->PartitionList;
        YCHECK(lowerBound >= partitions[0]->PivotKey);
        auto startPartitionIt = std::upper_bound(
            partitions.begin(),
            partitions.end(),
            lowerBound,
            [] (TRow lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, TRow rhs) {
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
        const TTableSchema& schema,
        const TObjectId& objectId,
        const TRowRange& range,
        const TQueryOptions& options)
    {
        ValidateReadTimestamp(options.Timestamp);

        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(schema, objectId, range, options);

            case EObjectType::Tablet:
                return GetTabletReader(schema, objectId, range, options);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetReader(
        const TTableSchema& schema,
        const TObjectId& objectId,
        const TSharedRange<TRow>& keys,
        const TQueryOptions& options)
    {
        ValidateReadTimestamp(options.Timestamp);

        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(schema,  objectId, keys, options);

            case EObjectType::Tablet:
                return GetTabletReader(schema, objectId, keys, options);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const TObjectId& chunkId,
        const TRowRange& range,
        const TQueryOptions& options)
    {
        std::vector<TReadRange> readRanges;
        TReadLimit lowerReadLimit;
        TReadLimit upperReadLimit;
        lowerReadLimit.SetKey(TOwningKey(range.first));
        upperReadLimit.SetKey(TOwningKey(range.second));
        readRanges.emplace_back(std::move(lowerReadLimit), std::move(upperReadLimit));
        return GetChunkReader(schema, chunkId, std::move(readRanges), options);
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const TChunkId& chunkId,
        const TSharedRange<TRow>& keys,
        const TQueryOptions& options)
    {
        std::vector<TReadRange> readRanges;
        TUnversionedOwningRowBuilder builder;
        for (const auto& key : keys) {
            TReadLimit lowerReadLimit;
            lowerReadLimit.SetKey(TOwningKey(key));

            TReadLimit upperReadLimit;
            for (const auto& value : key) {
                builder.AddValue(value);
            }
            builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            upperReadLimit.SetKey(builder.FinishRow());

            readRanges.emplace_back(std::move(lowerReadLimit), std::move(upperReadLimit));
        }

        return GetChunkReader(schema, chunkId, readRanges, options);
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const TChunkId& chunkId,
        std::vector<TReadRange> readRanges,
        const TQueryOptions& options)
    {
        auto blockCache = Bootstrap_->GetBlockCache();
        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        auto config = CloneYsonSerializable(Bootstrap_->GetConfig()->TabletNode->TabletManager->ChunkReader);
        config->WorkloadDescriptor = options.WorkloadDescriptor;

        NChunkClient::IChunkReaderPtr chunkReader;
        if (chunk && !chunk->IsRemoveScheduled()) {
            LOG_DEBUG("Creating local reader for chunk split (ChunkId: %v, Timestamp: %v)",
                chunkId,
                options.Timestamp);

            chunkReader = CreateLocalChunkReader(
                config,
                chunk,
                Bootstrap_->GetChunkBlockManager(),
                blockCache);
        } else {
            LOG_DEBUG("Creating remote reader for chunk split (ChunkId: %v, Timestamp: %v)",
                chunkId,
                options.Timestamp);

            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            chunkReader = CreateRemoteReader(
                chunkId,
                config,
                New<TRemoteReaderOptions>(),
                Bootstrap_->GetMasterClient(),
                Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
                Bootstrap_->GetBlockCache(),
                GetUnlimitedThrottler());
        }

        auto asyncChunkMeta = chunkReader->GetMeta(config->WorkloadDescriptor);
        auto chunkMeta = WaitFor(asyncChunkMeta)
            .ValueOrThrow();

        return CreateSchemafulChunkReader(
            std::move(config),
            std::move(chunkReader),
            Bootstrap_->GetBlockCache(),
            schema,
            chunkMeta,
            std::move(readRanges),
            options.Timestamp);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTableSchema& schema,
        const TObjectId& tabletId,
        const TRowRange& range,
        const TQueryOptions& options)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
        slotManager->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            options.Timestamp);

        TOwningKey lowerBound(range.first);
        TOwningKey upperBound(range.second);

        auto columnFilter = GetColumnFilter(schema, tabletSnapshot->QuerySchema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            options.Timestamp,
            options.WorkloadDescriptor);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTableSchema& schema,
        const TTabletId& tabletId,
        const TSharedRange<TRow>& keys,
        const TQueryOptions& options)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
        slotManager->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            options.Timestamp);

        auto columnFilter = GetColumnFilter(schema, tabletSnapshot->QuerySchema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            keys,
            options.Timestamp,
            options.WorkloadDescriptor,
            Config_->MaxBottomReaderConcurrency);
    }
};

ISubexecutorPtr CreateQueryExecutor(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

