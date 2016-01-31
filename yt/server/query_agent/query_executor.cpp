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
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/coordinator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/function_registry.h>
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

void RowRangeFormatter(TStringBuilder* builder, const NQueryClient::TRowRange& range)
{
    builder->AppendFormat("[%v .. %v]",
        range.first,
        range.second);
}

void DataSourceFormatter(TStringBuilder* builder, const NQueryClient::TDataRange& source)
{
    builder->AppendFormat("[%v .. %v]",
        source.Range.first,
        source.Range.second);
}

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public ISubExecutor
{
public:
    explicit TQueryExecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Evaluator_(New<TEvaluator>(Config_))
        , FunctionRegistry_(Bootstrap_->GetMasterClient()->GetConnection()->GetFunctionRegistry())
        , ColumnEvaluatorCache_(Bootstrap_->GetMasterClient()->GetConnection()->GetColumnEvaluatorCache())
    { }

    // IExecutor implementation.
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        TQueryOptions options) override
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        auto execute = query->IsOrdered()
            ? &TQueryExecutor::DoExecuteOrdered
            : &TQueryExecutor::DoExecute;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(std::move(query), std::move(dataSources), std::move(options), std::move(writer), maybeUser);
    }

private:
    const TQueryAgentConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const IFunctionRegistryPtr FunctionRegistry_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    typedef std::function<ISchemafulReaderPtr()> TSubreaderCreator;

    TQueryStatistics DoCoordinateAndExecute(
        TConstQueryPtr query,
        TQueryOptions options,
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

        auto remoteExecutor = Bootstrap_->GetMasterClient()->GetConnection()
            ->CreateClient(clientOptions)->GetQueryExecutor();

        return CoordinateAndExecute(
            query,
            writer,
            refiners,
            [&] (TConstQueryPtr subquery, int index) {
                auto mergingReader = subreaderCreators[index]();

                auto pipe = New<TSchemafulPipe>();

                LOG_DEBUG("Evaluating subquery (SubqueryId: %v)", subquery->Id);

                auto foreignExecuteCallback = [options, remoteExecutor, Logger] (
                    const TQueryPtr& subquery,
                    TGuid dataId,
                    TRowBufferPtr buffer,
                    TRowRanges ranges,
                    ISchemafulWriterPtr writer) -> TFuture<TQueryStatistics>
                {
                    LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                    TQueryOptions subqueryOptions;
                    subqueryOptions.Timestamp = options.Timestamp;
                    subqueryOptions.VerboseLogging = options.VerboseLogging;

                    TDataRanges dataSource{
                        dataId,
                        MakeSharedRange(std::move(ranges), std::move(buffer))
                    };

                    return remoteExecutor->Execute(
                        subquery,
                        std::move(dataSource),
                        writer,
                        subqueryOptions);
                };

                auto asyncStatistics = BIND(&TEvaluator::RunWithExecutor, Evaluator_)
                    .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
                    .Run(
                        subquery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignExecuteCallback,
                        FunctionRegistry_,
                        options.EnableCodeCache);

                asyncStatistics.Subscribe(BIND([=] (const TErrorOr<TQueryStatistics>& result) {
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                        LOG_DEBUG(result, "Failed evaluating subquery (SubqueryId: %v)", subquery->Id);
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
                    FunctionRegistry_,
                    options.EnableCodeCache);
                LOG_DEBUG("Finished evaluating top query (TopQueryId: %v)", topQuery->Id);
                return result;
            },
            FunctionRegistry_);
    }

    TQueryStatistics DoExecute(
        TConstQueryPtr query,
        std::vector<TDataRanges> dataSources,
        TQueryOptions options,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto Logger = BuildLogger(query);

        LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        std::vector<std::pair<TGuid, TSharedRange<TRowRange>>> rangesByTablePart;
        std::vector<std::pair<TGuid, TSharedRange<TRow>>> keysByTablePart;

        auto keySize = query->KeyColumnsCount;

        for (const auto& source : dataSources) {
            TRowRanges rowRanges;
            std::vector<TRow> keys;

            for (const auto& range : source.Ranges) {
                auto lowerBound = range.first;
                auto upperBound = range.second;

                if (keySize == lowerBound.GetCount() &&
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
                rangesByTablePart.emplace_back(
                    source.Id,
                    MakeSharedRange(std::move(rowRanges), source.Ranges.GetHolder()));
            }
            if (!keys.empty()) {
                keysByTablePart.emplace_back(
                    source.Id,
                    MakeSharedRange(std::move(keys), source.Ranges.GetHolder()));
            }
        }

        LOG_DEBUG("Splitting sources");

        auto rowBuffer = New<TRowBuffer>();
        auto splits = Split(std::move(rangesByTablePart), rowBuffer, Logger, options.VerboseLogging);
        int splitCount = splits.size();
        int splitOffset = 0;
        std::vector<std::vector<TDataRange>> groupedSplits;

        LOG_DEBUG("Grouping %v splits", splitCount);

        auto maxSubqueries = std::min(options.MaxSubqueries, Config_->MaxSubqueries);

        for (int queryIndex = 1; queryIndex <= maxSubqueries; ++queryIndex) {
            int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            if (splitOffset != nextSplitOffset) {
                groupedSplits.emplace_back(splits.begin() + splitOffset, splits.begin() + nextSplitOffset);
                splitOffset = nextSplitOffset;
            }
        }

        LOG_DEBUG("Got %v split groups", groupedSplits.size());

        auto columnEvaluator = ColumnEvaluatorCache_->Find(
            query->TableSchema,
            query->KeyColumnsCount);

        auto timestamp = options.Timestamp;

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (auto& groupedSplit : groupedSplits) {
            refiners.push_back([&, range = GetRange(groupedSplit)] (TConstExpressionPtr expr, const TTableSchema& schema, const
            TKeyColumns& keyColumns) {
                return RefinePredicate(range, expr, schema, keyColumns, columnEvaluator);
            });
            subreaderCreators.push_back([&, MOVE(groupedSplit)] () {
                if (options.VerboseLogging) {
                    LOG_DEBUG("Generating reader for ranges %v",
                        JoinToString(groupedSplit, DataSourceFormatter));
                } else {
                    LOG_DEBUG("Generating reader for %v ranges", groupedSplit.size());
                }

                auto bottomSplitReaderGenerator = [
                    Logger,
                    query,
                    MOVE(groupedSplit),
                    timestamp,
                    index = 0,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedSplit.size()) {
                        return nullptr;
                    } else {

                        const auto& group = groupedSplit[index++];

                        auto result =  this_->GetReader(
                            query->TableSchema,
                            group.Id,
                            group.Range,
                            timestamp);

                        return result;
                    }
                };

                return CreateUnorderedSchemafulReader(
                    std::move(bottomSplitReaderGenerator),
                    Config_->MaxBottomReaderConcurrency);
            });
        }

        for (auto& keySource : keysByTablePart) {
            const auto& tablePartId = keySource.first;
            auto& keys = keySource.second;

            refiners.push_back([&] (TConstExpressionPtr expr, const TTableSchema& schema, const TKeyColumns& keyColumns) {
                return RefinePredicate(keys, expr, keyColumns);
            });

            subreaderCreators.push_back([&] () {
                ValidateReadTimestamp(timestamp);

                std::function<ISchemafulReaderPtr()> bottomSplitReaderGenerator;

                switch (TypeFromId(tablePartId)) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk: {
                        return GetChunkReader(
                            query->TableSchema,
                            tablePartId,
                            keys,
                            timestamp);
                    }

                    case EObjectType::Tablet: {
                        LOG_DEBUG("Grouping %v lookup keys by parition", keys.Size());
                        auto groupedKeys = GroupKeysByPartition(tablePartId, std::move(keys));
                        LOG_DEBUG("Grouped lookup keys into %v paritions", groupedKeys.size());

                        for (const auto& keys : groupedKeys) {
                            if (options.VerboseLogging) {
                                LOG_DEBUG("Generating lookup reader for keys %v",
                                    keys.second);
                            } else {
                                LOG_DEBUG("Generating lookup reader for %v keys",
                                    keys.second.Size());
                            }
                        }

                        auto slotManager = Bootstrap_->GetTabletSlotManager();
                        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tablePartId);

                        slotManager->ValidateTabletAccess(
                            tabletSnapshot,
                            NYTree::EPermission::Read,
                            timestamp);

                        bottomSplitReaderGenerator = [
                            Logger,
                            MOVE(tabletSnapshot),
                            query,
                            MOVE(groupedKeys),
                            tablePartId,
                            timestamp,
                            index = 0,
                            this_ = MakeStrong(this)
                        ] () mutable -> ISchemafulReaderPtr {
                            if (index == groupedKeys.size()) {
                                return nullptr;
                            } else {
                                const auto& group = groupedKeys[index++];

                                auto columnFilter = GetColumnFilter(query->TableSchema, tabletSnapshot->Schema);
                                auto result = CreateSchemafulTabletReader(
                                    std::move(tabletSnapshot),
                                    columnFilter,
                                    group.first,
                                    group.second,
                                    timestamp);

                                return result;
                            }
                        };

                        return CreateUnorderedSchemafulReader(
                            std::move(bottomSplitReaderGenerator),
                            Config_->MaxBottomReaderConcurrency);
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                            TypeFromId(tablePartId));
                }
            });
        }

        return DoCoordinateAndExecute(
            query,
            options,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    TQueryStatistics DoExecuteOrdered(
        TConstQueryPtr query,
        std::vector<TDataRanges> dataSources,
        TQueryOptions options,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto Logger = BuildLogger(query);

        std::vector<std::pair<TGuid, TSharedRange<TRowRange>>> rangesByTablePart;
        for (const auto& source : dataSources) {
            rangesByTablePart.emplace_back(source.Id, source.Ranges);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto splits = Split(std::move(rangesByTablePart), rowBuffer, Logger, options.VerboseLogging);

        LOG_DEBUG("Sorting %v splits", splits.size());

        std::sort(splits.begin(), splits.end(), [] (const TDataRange & lhs, const TDataRange & rhs) {
            return lhs.Range.first < rhs.Range.first;
        });

        if (options.VerboseLogging) {
            LOG_DEBUG("Got ranges for groups %v",
                JoinToString(splits, DataSourceFormatter));
        } else {
            LOG_DEBUG("Got ranges for %v groups", splits.size());
        }

        auto columnEvaluator = ColumnEvaluatorCache_->Find(
            query->TableSchema,
            query->KeyColumnsCount);

        auto timestamp = options.Timestamp;

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (const auto& dataSplit : splits) {
            refiners.push_back([&] (TConstExpressionPtr expr, const TTableSchema& schema, const TKeyColumns& keyColumns) {
                return RefinePredicate(dataSplit.Range, expr, schema, keyColumns, columnEvaluator);
            });
            subreaderCreators.push_back([&] () {
                return GetReader(query->TableSchema, dataSplit.Id, dataSplit.Range, timestamp);
            });
        }

        return DoCoordinateAndExecute(
            query,
            options,
            std::move(writer),
            refiners,
            subreaderCreators);
    }

    std::vector<TDataRange> Split(
        std::vector<std::pair<TGuid, TSharedRange<TRowRange>>> rangesByTablePart,
        TRowBufferPtr rowBuffer,
        const NLogging::TLogger& Logger,
        bool verboseLogging)
    {
        std::vector<TDataRange> allSplits;

        auto securityManager = Bootstrap_->GetSecurityManager();

        for (auto& tablePartIdRange : rangesByTablePart) {
            auto tablePartId = tablePartIdRange.first;
            auto& keyRanges = tablePartIdRange.second;

            if (TypeFromId(tablePartId) != EObjectType::Tablet) {
                for (const auto& range : keyRanges) {
                    allSplits.push_back(TDataRange{tablePartId, range});
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

        return allSplits;
    }

    std::vector<std::pair<TPartitionSnapshotPtr, TSharedRange<TRow>>> GroupKeysByPartition(
        const NObjectClient::TObjectId& objectId,
        TSharedRange<TRow> keys)
    {
        std::vector<std::pair<TPartitionSnapshotPtr, TSharedRange<TRow>>> result;
        // TODO(lukyan): YCHECK(sorted)

        YCHECK(TypeFromId(objectId) == EObjectType::Tablet);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(objectId);
        const auto& partitions = tabletSnapshot->Partitions;

        auto currentPartition = partitions.begin();
        auto currentIt = begin(keys);
        while (currentIt != end(keys)) {
            auto nextPartition = std::upper_bound(
                currentPartition,
                partitions.end(),
                *currentIt,
                [] (TRow lhs, const TPartitionSnapshotPtr& rhs) {
                    return lhs < rhs->PivotKey;
                });

            auto nextIt = nextPartition != partitions.end()
                ? std::lower_bound(currentIt, end(keys), (*nextPartition)->PivotKey)
                : end(keys);

            // TODO(babenko): fixme, data ownership?
            TPartitionSnapshotPtr ptr = *currentPartition;
            result.emplace_back(
                ptr,
                MakeSharedRange(MakeRange<TRow>(currentIt, nextIt), keys.GetHolder()));

            currentIt = nextIt;
            currentPartition = nextPartition;
        }

        return result;
    }

    std::pair<int, int> GetBoundSampleKeys(
        TTabletSnapshotPtr tabletSnapshot,
        TRow lowerBound,
        TRow upperBound)
    {
        YCHECK(lowerBound <= upperBound);

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
        TRow lowerBound,
        TRow upperBound,
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
        const NObjectClient::TObjectId& objectId,
        const TRowRange& range,
        TTimestamp timestamp)
    {
        ValidateReadTimestamp(timestamp);

        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(schema, objectId, range, timestamp);

            case EObjectType::Tablet:
                return GetTabletReader(schema, objectId, range, timestamp);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetReader(
        const TTableSchema& schema,
        const NObjectClient::TObjectId& objectId,
        const TSharedRange<TRow>& keys,
        TTimestamp timestamp)
    {
        ValidateReadTimestamp(timestamp);

        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(schema,  objectId, keys, timestamp);

            case EObjectType::Tablet:
                return GetTabletReader(schema, objectId, keys, timestamp);

            default:
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(objectId));
        }
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const NObjectClient::TObjectId& chunkId,
        const TRowRange& range,
        TTimestamp timestamp)
    {
        std::vector<TReadRange> readRanges;
        TReadLimit lowerReadLimit;
        TReadLimit upperReadLimit;
        lowerReadLimit.SetKey(TOwningKey(range.first));
        upperReadLimit.SetKey(TOwningKey(range.second));
        readRanges.emplace_back(std::move(lowerReadLimit), std::move(upperReadLimit));
        return GetChunkReader(schema, chunkId, std::move(readRanges), timestamp);
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const TChunkId& chunkId,
        const TSharedRange<TRow>& keys,
        TTimestamp timestamp)
    {
        std::vector<TReadRange> readRanges;
        TUnversionedOwningRowBuilder builder;
        for (const auto& key : keys) {
            TReadLimit lowerReadLimit;
            lowerReadLimit.SetKey(TOwningKey(key));

            TReadLimit upperReadLimit;
            for (int index = 0; index < key.GetCount(); ++index) {
                builder.AddValue(key[index]);
            }
            builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
            upperReadLimit.SetKey(builder.FinishRow());

            readRanges.emplace_back(std::move(lowerReadLimit), std::move(upperReadLimit));
        }

        return GetChunkReader(schema, chunkId, readRanges, timestamp);
    }

    ISchemafulReaderPtr GetChunkReader(
        const TTableSchema& schema,
        const TChunkId& chunkId,
        std::vector<TReadRange> readRanges,
        TTimestamp timestamp)
    {
        auto blockCache = Bootstrap_->GetBlockCache();
        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        NChunkClient::IChunkReaderPtr chunkReader;
        if (chunk && !chunk->IsRemoveScheduled()) {
            LOG_DEBUG("Creating local reader for chunk split (ChunkId: %v, Timestamp: %v)",
                chunkId,
                timestamp);

            chunkReader = CreateLocalChunkReader(
                Bootstrap_,
                Bootstrap_->GetConfig()->TabletNode->ChunkReader,
                chunk,
                blockCache);
        } else {
            LOG_DEBUG("Creating remote reader for chunk split (ChunkId: %v, Timestamp: %v)",
                chunkId,
                timestamp);

            // TODO(babenko): seed replicas?
            // TODO(babenko): throttler?
            auto options = New<TRemoteReaderOptions>();
            chunkReader = CreateReplicationReader(
                Bootstrap_->GetConfig()->TabletNode->ChunkReader,
                options,
                Bootstrap_->GetMasterClient(),
                New<TNodeDirectory>(),
                Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
                chunkId,
                TChunkReplicaList(),
                Bootstrap_->GetBlockCache());
        }

        auto chunkMeta = WaitFor(chunkReader->GetMeta()).ValueOrThrow();

        return CreateSchemafulChunkReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            std::move(chunkReader),
            Bootstrap_->GetBlockCache(),
            schema,
            chunkMeta,
            std::move(readRanges),
            timestamp);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTableSchema& schema,
        const NObjectClient::TObjectId& tabletId,
        const TRowRange& range,
        TTimestamp timestamp)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
        slotManager->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            timestamp);

        TOwningKey lowerBound(range.first);
        TOwningKey upperBound(range.second);

        auto columnFilter = GetColumnFilter(schema, tabletSnapshot->Schema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp);
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTableSchema& schema,
        const TTabletId& tabletId,
        const TSharedRange<TRow>& keys,
        TTimestamp timestamp)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
        slotManager->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            timestamp);

        auto columnFilter = GetColumnFilter(schema, tabletSnapshot->Schema);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            keys,
            timestamp,
            Config_->MaxBottomReaderConcurrency);
    }

};

ISubExecutorPtr CreateQueryExecutor(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

