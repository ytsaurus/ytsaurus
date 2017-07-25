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

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>

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
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_helpers.h>
#include <yt/ytlib/query_client/private.h>
#include <yt/ytlib/query_client/query_statistics.h>
#include <yt/ytlib/query_client/executor.h>

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
#include <yt/core/misc/collection_helpers.h>

namespace NYT {
namespace NQueryAgent {

using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTabletNode;
using namespace NDataNode;
using namespace NCellNode;

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

struct TQuerySubexecutorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TTabletSnapshotCache
{
public:
    explicit TTabletSnapshotCache(TSlotManagerPtr slotManager)
        : SlotManager_(std::move(slotManager))
    { }

    void ValidateAndRegisterTabletSnapshot(
        const TTabletId& tabletId,
        const i64 mountRevision,
        const TTimestamp timestamp)
    {
        auto tabletSnapshot = SlotManager_->GetTabletSnapshotOrThrow(tabletId);

        tabletSnapshot->ValidateMountRevision(mountRevision);

        SlotManager_->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            timestamp);

        Map_.insert(std::make_pair(tabletId, tabletSnapshot));
    }

    TTabletSnapshotPtr GetCachedTabletSnapshot(const TTabletId& tabletId)
    {
        auto it = Map_.find(tabletId);
        YCHECK(it != Map_.end());
        return it->second;
    }

private:
    const TSlotManagerPtr SlotManager_;
    yhash<TTabletId, TTabletSnapshotPtr> Map_;

};

////////////////////////////////////////////////////////////////////////////////

class TQueryExecution
    : public TIntrinsicRefCounted
{
public:
    TQueryExecution(
        TQueryAgentConfigPtr config,
        TFunctionImplCachePtr functionImplCache,
        TBootstrap* const bootstrap,
        const TEvaluatorPtr evaluator,
        TConstQueryPtr query,
        const TQueryOptions& options)
        : Config_(std::move(config))
        , FunctionImplCache_(std::move(functionImplCache))
        , Bootstrap_(bootstrap)
        , Evaluator_(std::move(evaluator))
        , Query_(std::move(query))
        , Options_(std::move(options))
        , Logger(MakeQueryLogger(Query_))
        , TabletSnapshots_(bootstrap->GetTabletSlotManager())
    { }

    TFuture<TQueryStatistics> Execute(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer)
    {
        for (const auto& source : dataSources) {
            if (TypeFromId(source.Id) == EObjectType::Tablet) {
                TabletSnapshots_.ValidateAndRegisterTabletSnapshot(
                    source.Id,
                    source.MountRevision,
                    Options_.Timestamp);
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        auto execute = Query_->IsOrdered()
            ? &TQueryExecution::DoExecuteOrdered
            : &TQueryExecution::DoExecuteUnordered;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(
                std::move(externalCGInfo),
                std::move(dataSources),
                std::move(writer),
                maybeUser);
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;

    const TConstQueryPtr Query_;
    const TQueryOptions Options_;

    const NLogging::TLogger Logger;

    TTabletSnapshotCache TabletSnapshots_;

    typedef std::function<ISchemafulReaderPtr()> TSubreaderCreator;

    TQueryStatistics DoCoordinateAndExecute(
        TConstExternalCGInfoPtr externalCGInfo,
        ISchemafulWriterPtr writer,
        const std::vector<TRefiner>& refiners,
        const std::vector<TSubreaderCreator>& subreaderCreators)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        NApi::TClientOptions clientOptions;
        if (maybeUser) {
            clientOptions.User = maybeUser.Get();
        }

        auto client = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->CreateNativeClient(clientOptions);

        auto remoteExecutor = CreateQueryExecutor(
            client->GetNativeConnection(),
            client->GetChannelFactory(),
            FunctionImplCache_);

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *BuiltinFunctionCG);
        MergeFrom(aggregateGenerators.Get(), *BuiltinAggregateCG);
        FetchImplementations(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_);

        return CoordinateAndExecute(
            Query_,
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
                    remoteExecutor,
                    this,
                    this_ = MakeStrong(this)
                ] (
                    const TQueryPtr& subquery,
                    TDataRanges dataRanges,
                    ISchemafulWriterPtr writer)
                {
                    LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                    auto asyncResult = remoteExecutor->Execute(
                        subquery,
                        externalCGInfo,
                        std::move(dataRanges),
                        writer,
                        Options_);

                    asyncSubqueryResults->push_back(asyncResult);

                    return asyncResult;
                };

                auto asyncStatistics = BIND(&TEvaluator::RunWithExecutor, Evaluator_)
                    .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
                    .Run(subquery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignExecuteCallback,
                        functionGenerators,
                        aggregateGenerators,
                        Options_.EnableCodeCache);

                asyncStatistics.Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TQueryStatistics>& result) -> TErrorOr<TQueryStatistics>{
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
                    Options_.EnableCodeCache);
                LOG_DEBUG("Finished evaluating top query (TopQueryId: %v)", topQuery->Id);
                return result;
            });
    }

    TQueryStatistics DoExecuteUnordered(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TNullable<TString>& maybeUser)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        std::vector<TDataRanges> rangesByTablePart;
        std::vector<TDataKeys> keysByTablePart;

        auto rowBuffer = New<TRowBuffer>(TQuerySubexecutorBufferTag());

        auto keySize = Query_->OriginalSchema.GetKeyColumnCount();
        size_t rangesCount = 0;
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

            for (const auto& key : source.Keys) {
                auto rowSize = key.GetCount();
                if (source.LookupSupported &&
                    keySize == key.GetCount())
                {
                    keys.push_back(key);
                } else {
                    auto lowerBound = key;

                    auto upperBound = rowBuffer->Allocate(rowSize + 1);
                    for (int column = 0; column < rowSize; ++column) {
                        upperBound[column] = lowerBound[column];
                    }

                    upperBound[rowSize] = MakeUnversionedSentinelValue(EValueType::Max);
                    rowRanges.emplace_back(lowerBound, upperBound);
                }
            }

            if (!rowRanges.empty()) {
                rangesCount += rowRanges.size();
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

        LOG_DEBUG("Splitting %v ranges", rangesCount);

        auto splits = Split(std::move(rangesByTablePart), rowBuffer);
        int splitCount = splits.Size();
        int splitOffset = 0;
        std::vector<TSharedRange<TDataRange>> groupedSplits;

        LOG_DEBUG("Grouping %v splits", splitCount);

        auto maxSubqueries = std::min(Options_.MaxSubqueries, Config_->MaxSubqueries);

        for (int queryIndex = 1; queryIndex <= maxSubqueries; ++queryIndex) {
            int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            if (splitOffset != nextSplitOffset) {
                std::vector<TDataRange> subsplit(splits.begin() + splitOffset, splits.begin() + nextSplitOffset);
                groupedSplits.emplace_back(MakeSharedRange(std::move(subsplit), splits.GetHolder()));
                splitOffset = nextSplitOffset;
            }
        }

        LOG_DEBUG("Got %v split groups", groupedSplits.size());

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (auto& groupedSplit : groupedSplits) {
            std::vector<TRowRange> keyRanges;
            for (const auto& dataRange : groupedSplit) {
                keyRanges.push_back(dataRange.Range);
            }

            refiners.push_back([MOVE(keyRanges), inferRanges = Query_->InferRanges] (
                TConstExpressionPtr expr,
                const TKeyColumns& keyColumns)
            {
                if (inferRanges) {
                    return EliminatePredicate(keyRanges, expr, keyColumns);
                } else {
                    return expr;
                }
            });
            subreaderCreators.push_back([&, MOVE(groupedSplit)] () {
                if (Options_.VerboseLogging) {
                    LOG_DEBUG("Generating reader for ranges %v",
                        MakeFormattableRange(groupedSplit, TDataSourceFormatter()));
                } else {
                    LOG_DEBUG("Generating reader for %v ranges",
                        groupedSplit.Size());
                }

                auto bottomSplitReaderGenerator = [
                    MOVE(groupedSplit),
                    index = 0,
                    this,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedSplit.Size()) {
                        return nullptr;
                    }

                    const auto& group = groupedSplit[index++];
                    return GetReader(group.Id, group.Range);
                };

                return CreatePrefetchingOrderedSchemafulReader(std::move(bottomSplitReaderGenerator));
            });
        }

        for (auto& keySource : keysByTablePart) {
            const auto& tablePartId = keySource.Id;
            auto& keys = keySource.Keys;

            refiners.push_back([&, inferRanges = Query_->InferRanges] (
                TConstExpressionPtr expr, const
                TKeyColumns& keyColumns)
            {
                if (inferRanges) {
                    return EliminatePredicate(keys, expr, keyColumns);
                } else {
                    return expr;
                }
            });
            subreaderCreators.push_back([&, MOVE(keys)] () {
                switch (TypeFromId(tablePartId)) {
                    case EObjectType::Tablet:
                        return GetTabletReader(tablePartId, keys);

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                            TypeFromId(tablePartId));
                }
            });
        }

        return DoCoordinateAndExecute(
            externalCGInfo,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    TQueryStatistics DoExecuteOrdered(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TNullable<TString>& maybeUser)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto rowBuffer = New<TRowBuffer>(TQuerySubexecutorBufferTag());
        std::vector<TDataRanges> rangesByTablePart;

        for (const auto& source : dataSources) {
            TRowRanges rowRanges;

            for (const auto& range : source.Ranges) {
                rowRanges.push_back(range);
            }

            for (const auto& key : source.Keys) {
                auto rowSize = key.GetCount();
                auto lowerBound = key;

                auto upperBound = rowBuffer->Allocate(rowSize + 1);
                for (int column = 0; column < rowSize; ++column) {
                    upperBound[column] = lowerBound[column];
                }

                upperBound[rowSize] = MakeUnversionedSentinelValue(EValueType::Max);
                rowRanges.emplace_back(lowerBound, upperBound);

            }

            TDataRanges item;
            item.Id = source.Id;
            item.Ranges = MakeSharedRange(std::move(rowRanges), source.Ranges.GetHolder());
            item.LookupSupported = source.LookupSupported;
            rangesByTablePart.emplace_back(std::move(item));
        }

        auto splits = Split(std::move(rangesByTablePart), rowBuffer);

        LOG_DEBUG("Sorting %v splits", splits.Size());

        std::sort(splits.Begin(), splits.End(), [] (const TDataRange& lhs, const TDataRange& rhs) {
            return lhs.Range.first < rhs.Range.first;
        });

        if (Options_.VerboseLogging) {
            LOG_DEBUG("Got ranges for groups %v",
                MakeFormattableRange(splits, TDataSourceFormatter()));
        } else {
            LOG_DEBUG("Got ranges for %v groups",
                splits.Size());
        }

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (const auto& dataSplit : splits) {
            refiners.push_back([&, inferRanges = Query_->InferRanges] (
                TConstExpressionPtr expr,
                const TKeyColumns& keyColumns)
            {
                if (inferRanges) {
                    return EliminatePredicate(MakeRange(&dataSplit.Range, 1), expr, keyColumns);
                } else {
                    return expr;
                }
            });
            subreaderCreators.push_back([&] () {
                return GetReader(dataSplit.Id, dataSplit.Range);
            });
        }

        return DoCoordinateAndExecute(
            externalCGInfo,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    // TODO(lukyan): Use mutable shared range
    TSharedMutableRange<TDataRange> Split(std::vector<TDataRanges> rangesByTablePart, TRowBufferPtr rowBuffer)
    {
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

            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tablePartId);

            std::vector<TRowRange> resultRanges;
            int lastIndex = 0;

            auto addRange = [&] (int count, TUnversionedRow lowerBound, TUnversionedRow upperBound) {
                LOG_DEBUG_IF(Options_.VerboseLogging, "Merging %v ranges into [%v .. %v]",
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

        if (!tabletSnapshot->PhysicalSchema.IsSorted()) {
            return std::make_pair(0, 1);
        }

        auto findStartSample = [&] (const TSharedRange<TKey>& sampleKeys) {
            return std::upper_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                lowerBound);
        };
        auto findEndSample = [&] (const TSharedRange<TKey>& sampleKeys) {
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
            auto startSampleIt = partitionIt == startPartitionIt && !sampleKeys.Empty()
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
        if (!tabletSnapshot->PhysicalSchema.IsSorted()) {
            return {TOwningKey(lowerBound)};
        }

        auto findStartSample = [&] (const TSharedRange<TKey>& sampleKeys) {
            return std::upper_bound(
                sampleKeys.begin(),
                sampleKeys.end(),
                lowerBound);
        };
        auto findEndSample = [&] (const TSharedRange<TKey>& sampleKeys) {
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
            auto startSampleIt = partitionIt == startPartitionIt && !sampleKeys.Empty()
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
                    result.push_back(TOwningKey(*sampleIt));
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
        const TObjectId& objectId,
        const TRowRange& range)
    {
        switch (TypeFromId(objectId)) {
            case EObjectType::Tablet:
                return GetTabletReader(objectId, range);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetReader(
        const TObjectId& objectId,
        const TSharedRange<TRow>& keys)
    {
        switch (TypeFromId(objectId)) {
            case EObjectType::Tablet:
                return GetTabletReader(objectId, keys);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetTabletReader(
        const TObjectId& tabletId,
        const TRowRange& range)
    {
        TOwningKey lowerBound(range.first);
        TOwningKey upperBound(range.second);

        auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);
        auto columnFilter = GetColumnFilter(Query_->GetReadSchema(), tabletSnapshot->QuerySchema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            Options_.Timestamp,
            Options_.WorkloadDescriptor);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTabletId& tabletId,
        const TSharedRange<TRow>& keys)
    {
        auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);
        auto columnFilter = GetColumnFilter(Query_->GetReadSchema(), tabletSnapshot->QuerySchema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            keys,
            Options_.Timestamp,
            Options_.WorkloadDescriptor);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TQuerySubexecutor
    : public ISubexecutor
{
public:
    TQuerySubexecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , FunctionImplCache_(CreateFunctionImplCache(
            config->FunctionImplCache,
            bootstrap->GetMasterClient()))
        , Bootstrap_(bootstrap)
        , Evaluator_(New<TEvaluator>(Config_))
        , ColumnEvaluatorCache_(Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetColumnEvaluatorCache())
    { }

    // ISubexecutor implementation.
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TQueryOptions& options) override
    {
        ValidateReadTimestamp(options.Timestamp);

        auto execution = New<TQueryExecution>(
            Config_,
            FunctionImplCache_,
            Bootstrap_,
            Evaluator_,
            std::move(query),
            options);

        return execution->Execute(
            std::move(externalCGInfo),
            std::move(dataSources),
            std::move(writer));
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
};

ISubexecutorPtr CreateQuerySubexecutor(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TQuerySubexecutor>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

