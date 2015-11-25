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

#include <yt/core/misc/common.h>
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

Stroka RowRangeFormatter(const NQueryClient::TRowRange& range)
{
    return Format("[%v .. %v]", range.first, range.second);
}

Stroka DataSourceFormatter(const NQueryClient::TDataSource& source)
{
    return Format("[%v .. %v]", source.Range.first, source.Range.second);
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
        TPlanSubFragmentPtr fragment,
        ISchemafulWriterPtr writer) override
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        auto execute = fragment->Query->IsOrdered()
            ? &TQueryExecutor::DoExecuteOrdered
            : &TQueryExecutor::DoExecute;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
            .Run(fragment, std::move(writer), maybeUser);
    }

private:
    const TQueryAgentConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const IFunctionRegistryPtr FunctionRegistry_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    typedef std::function<ISchemafulReaderPtr()> TSubreaderCreator;

    TQueryStatistics DoCoordinateAndExecute(
        TPlanSubFragmentPtr fragment,
        ISchemafulWriterPtr writer,
        bool isOrdered,
        const std::vector<TRefiner>& refiners,
        const std::vector<TSubreaderCreator>& subreaderCreators)
    {
        auto Logger = BuildLogger(fragment->Query);

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto maybeUser = securityManager->GetAuthenticatedUser();

        NApi::TClientOptions clientOptions;
        if (maybeUser) {
            clientOptions.User = maybeUser.Get();
        }

        auto remoteExecutor = Bootstrap_->GetMasterClient()->GetConnection()
            ->CreateClient(clientOptions)->GetQueryExecutor();

        return CoordinateAndExecute(
            fragment->Query,
            writer,
            refiners,
            isOrdered,
            [&] (TConstQueryPtr subquery, int index) {
                auto mergingReader = subreaderCreators[index]();

                auto pipe = New<TSchemafulPipe>();

                LOG_DEBUG("Evaluating subquery (SubqueryId: %v)", subquery->Id);

                auto foreignExecuteCallback = [fragment, remoteExecutor, Logger] (
                    const TQueryPtr& subquery,
                    TGuid dataId,
                    TRowBufferPtr buffer,
                    TRowRanges ranges,
                    ISchemafulWriterPtr writer) -> TQueryStatistics
                {
                    LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                    TQueryOptions subqueryOptions;
                    subqueryOptions.Timestamp = fragment->Timestamp;
                    subqueryOptions.VerboseLogging = fragment->Options.VerboseLogging;

                    TDataSource2 dataSource;
                    dataSource.Id = dataId;
                    dataSource.Ranges = MakeSharedRange(std::move(ranges), std::move(buffer));

                    auto subqueryResult = remoteExecutor->Execute(
                        subquery,
                        dataSource,
                        subqueryOptions,
                        writer);

                    return WaitFor(subqueryResult)
                        .ValueOrThrow();
                };

                auto asyncStatistics = BIND(&TEvaluator::RunWithExecutor, Evaluator_)
                    .AsyncVia(Bootstrap_->GetQueryPoolInvoker())
                    .Run(
                        subquery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignExecuteCallback,
                        FunctionRegistry_,
                        ColumnEvaluatorCache_,
                        fragment->Options.EnableCodeCache);

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
                auto result = Evaluator_->Run(topQuery, std::move(reader), std::move(writer), FunctionRegistry_,
                                              fragment->Options.EnableCodeCache);
                LOG_DEBUG("Finished evaluating top query (TopQueryId: %v)", topQuery->Id);
                return result;
            },
            FunctionRegistry_);
    }

    TQueryStatistics DoExecute(
        TPlanSubFragmentPtr fragment,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto timestamp = fragment->Timestamp;

        auto Logger = BuildLogger(fragment->Query);

        TDataSources rangeSources;

        std::map<NObjectClient::TObjectId, std::vector<TRow>> keySources;

        LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        for (const auto& source : fragment->DataSources) {
            auto lowerBound = source.Range.first;
            auto upperBound = source.Range.second;

            auto keySize = fragment->Query->KeyColumnsCount;

            if (keySize == lowerBound.GetCount()  &&
                keySize + 1 == upperBound.GetCount() &&
                upperBound[keySize].Type == EValueType::Max &&
                CompareRows(lowerBound.Begin(), lowerBound.End(), upperBound.Begin(), upperBound.Begin() + keySize) == 0)
            {
                keySources[source.Id].push_back(lowerBound);
            } else {
                rangeSources.push_back(source);
            }
        }

        LOG_DEBUG("Splitting %v sources", rangeSources.size());

        auto rowBuffer = New<TRowBuffer>();
        auto splits = Split(rangeSources, rowBuffer, true, Logger, fragment->Options.VerboseLogging);
        int splitCount = splits.size();
        int splitOffset = 0;
        std::vector<TDataSources> groupedSplits;

        LOG_DEBUG("Grouping %v splits", splitCount);

        auto maxSubqueries = std::min(fragment->Options.MaxSubqueries, Config_->MaxSubqueries);

        for (int queryIndex = 1; queryIndex <= maxSubqueries; ++queryIndex) {
            int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            if (splitOffset != nextSplitOffset) {
                groupedSplits.emplace_back(splits.begin() + splitOffset, splits.begin() + nextSplitOffset);
                splitOffset = nextSplitOffset;
            }
        }

        LOG_DEBUG("Got %v split groups", groupedSplits.size());

        auto ranges = GetRanges(groupedSplits);

        if (fragment->Options.VerboseLogging) {
            LOG_DEBUG("Got ranges for groups %v",
                JoinToString(ranges, RowRangeFormatter));
        } else {
            LOG_DEBUG("Got ranges for %v groups", ranges.size());
        }

        auto columnEvaluator = ColumnEvaluatorCache_->Find(
            fragment->Query->TableSchema,
            fragment->Query->KeyColumnsCount);

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (const auto& groupedSplit : groupedSplits) {
            refiners.push_back([&] (TConstExpressionPtr expr, const TTableSchema& schema, const TKeyColumns& keyColumns) {
                return RefinePredicate(GetRange(groupedSplit), expr, schema, keyColumns, columnEvaluator);
            });
            subreaderCreators.push_back([&] () {
                if (fragment->Options.VerboseLogging) {
                    LOG_DEBUG("Creating reader for ranges %v",
                        JoinToString(groupedSplit, DataSourceFormatter));
                } else {
                    LOG_DEBUG("Creating reader for %v ranges", groupedSplit.size());
                }

                auto bottomSplitReaderGenerator = [
                    fragment,
                    groupedSplit,
                    timestamp,
                    index = 0,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedSplit.size()) {
                        return nullptr;
                    } else {
                        return this_->GetReader(
                            fragment->Query->TableSchema,
                            groupedSplit[index++],
                      	    timestamp);
                    }
                };

                return CreateUnorderedSchemafulReader(
                    bottomSplitReaderGenerator,
                    Config_->MaxBottomReaderConcurrency);
            });
        }

        for (const auto& keySource : keySources) {
            refiners.push_back([&] (TConstExpressionPtr expr, const TTableSchema& schema, const TKeyColumns& keyColumns) {
                return RefinePredicate(keySource.second, expr, keyColumns);
            });
            subreaderCreators.push_back([&] () {
                std::vector<ISchemafulReaderPtr> bottomSplitReaders;

                LOG_DEBUG("Grouping %v lookup keys by parition", keySource.second.size());
                auto groupedKeys = GroupKeysByPartition(keySource.first, keySource.second);
                LOG_DEBUG("Grouped lookup keys into %v paritions", groupedKeys.size());

                for (const auto& keys : groupedKeys) {
                    if (fragment->Options.VerboseLogging) {
                        LOG_DEBUG("Creating lookup reader for keys %v",
                            JoinToString(keys));
                    } else {
                        LOG_DEBUG("Creating lookup reader for %v keys",
                            keys.Size());
                    }

                    bottomSplitReaders.push_back(GetReader(
                        fragment->Query->TableSchema,
                        keySource.first,
                        keys,
                        timestamp));
                }

                auto bottomSplitReaderGenerator = [
                    fragment,
                    groupedKeys,
                    object = keySource.first,
                    timestamp,
                    index = 0,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedKeys.size()) {
                        return nullptr;
                    } else {
                        return this_->GetReader(
                            fragment->Query->TableSchema,
                            object,
                            groupedKeys[index++],
                            timestamp);
                    }
                };

                return CreateUnorderedSchemafulReader(
                    bottomSplitReaderGenerator,
                    Config_->MaxBottomReaderConcurrency);
            });
        }

        return DoCoordinateAndExecute(
            fragment,
            std::move(writer),
            false,
            refiners,
            subreaderCreators);
    }

    TQueryStatistics DoExecuteOrdered(
        TPlanSubFragmentPtr fragment,
        ISchemafulWriterPtr writer,
        const TNullable<Stroka>& maybeUser)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, maybeUser);

        auto timestamp = fragment->Timestamp;

        auto Logger = BuildLogger(fragment->Query);

        auto rowBuffer = New<TRowBuffer>();
        auto splits = Split(fragment->DataSources, rowBuffer, true, Logger, fragment->Options.VerboseLogging);

        LOG_DEBUG("Sorting %v splits", splits.size());

        std::sort(splits.begin(), splits.end(), [] (const TDataSource& lhs, const TDataSource& rhs) {
            return lhs.Range.first < rhs.Range.first;
        });

        if (fragment->Options.VerboseLogging) {
            LOG_DEBUG("Got ranges for groups %v",
                JoinToString(splits, DataSourceFormatter));
        } else {
            LOG_DEBUG("Got ranges for %v groups", splits.size());
        }

        auto columnEvaluator = ColumnEvaluatorCache_->Find(
            fragment->Query->TableSchema,
            fragment->Query->KeyColumnsCount);

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;

        for (const auto& dataSplit : splits) {
            refiners.push_back([&] (TConstExpressionPtr expr, const TTableSchema& schema, const TKeyColumns& keyColumns) {
                return RefinePredicate(dataSplit.Range, expr, schema, keyColumns, columnEvaluator);
            });
            subreaderCreators.push_back([&] () {
                return GetReader(fragment->Query->TableSchema, dataSplit, timestamp);
            });
        }

        return DoCoordinateAndExecute(
            fragment,
            std::move(writer),
            true,
            refiners,
            subreaderCreators);
    }


    TDataSources Split(
        const TDataSources& splits,
        TRowBufferPtr rowBuffer,
        bool mergeRanges,
        const NLogging::TLogger& Logger,
        bool verboseLogging)
    {
        yhash_map<TGuid, TRowRanges> rangesByTablet;
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

            std::sort(keyRanges.begin(), keyRanges.end(), [] (const TRowRange& lhs, const TRowRange& rhs) {
                return lhs.first < rhs.first;
            });

            auto slotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

            securityManager->ValidatePermission(tabletSnapshot, NYTree::EPermission::Read);

            std::vector<TRowRange> resultRanges;
            if (mergeRanges) {
                int lastIndex = 0;

                auto addRange = [&] (int count, TUnversionedRow lowerBound, TUnversionedRow upperBound) {
                    LOG_DEBUG_IF(verboseLogging, "Merging %v ranges into [%v .. %v]",
                        count,
                        lowerBound,
                        upperBound);
                    resultRanges.emplace_back(lowerBound, upperBound);
                };

                for (int index = 1; index < keyRanges.size(); ++index) {
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

                addRange(keyRanges.size() - lastIndex, keyRanges[lastIndex].first, keyRanges.back().second);
            } else {
                resultRanges = keyRanges;
            }

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
                    allSplits.push_back({tabletId, TRowRange(
                        rowBuffer->Capture(std::max(range.first, thisKey.Get())),
                        rowBuffer->Capture(std::min(range.second, nextKey.Get()))
                    )});
                }
            }
        }

        return allSplits;
    }

    std::vector<TSharedRange<TRow>> GroupKeysByPartition(
        const NObjectClient::TObjectId& objectId,
        std::vector<TRow> keys)
    {
        std::vector<TSharedRange<TRow>> result;
        std::sort(keys.begin(), keys.end());

        if (TypeFromId(objectId) == EObjectType::Tablet) {
            auto slotManager = Bootstrap_->GetTabletSlotManager();
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(objectId);
            const auto& partitions = tabletSnapshot->Partitions;

            // Group keys by partitions.
            auto addRange = [&] (std::vector<TRow>::iterator begin, std::vector<TRow>::iterator end) {
                std::vector<TRow> selectedKeys(begin, end);
                // TODO(babenko): fixme, data ownership?
                result.emplace_back(MakeSharedRange(std::move(selectedKeys)));
            };

            auto currentIt = keys.begin();
            while (currentIt != keys.end()) {
                auto nextPartition = std::upper_bound(
                    partitions.begin(),
                    partitions.end(),
                    *currentIt,
                    [] (TRow lhs, const TPartitionSnapshotPtr& rhs) {
                        return lhs < rhs->PivotKey.Get();
                    });

                if (nextPartition == partitions.end()) {
                    addRange(currentIt, keys.end());
                    break;
                }

                auto nextIt = std::lower_bound(currentIt, keys.end(), (*nextPartition)->PivotKey.Get());
                addRange(currentIt, nextIt);
                currentIt = nextIt;
            }
        } else {
            result.emplace_back(MakeSharedRange(std::move(keys)));
        }

        return result;
    }

    std::pair<int, int> GetBoundSampleKeys(
        TTabletSnapshotPtr tabletSnapshot,
        const TRow& lowerBound,
        const TRow& upperBound)
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
            [] (const TRow& lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey.Get();
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, const TRow& rhs) {
                return lhs->PivotKey.Get() < rhs;
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
        const TRow& lowerBound,
        const TRow& upperBound,
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
            [] (const TRow& lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey.Get();
            }) - 1;
        auto endPartitionIt = std::lower_bound(
            startPartitionIt,
            partitions.end(),
            upperBound,
            [] (const TPartitionSnapshotPtr& lhs, const TRow& rhs) {
                return lhs->PivotKey.Get() < rhs;
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
        const TDataSource& source,
        TTimestamp timestamp)
    {
        ValidateReadTimestamp(timestamp);

        const auto& objectId = source.Id;
        switch (TypeFromId(objectId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetChunkReader(schema, source, timestamp);

            case EObjectType::Tablet:
                return GetTabletReader(schema, source, timestamp);

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
        const TDataSource& source,
        TTimestamp timestamp)
    {
        std::vector<TReadRange> readRanges;
        TReadLimit lowerReadLimit;
        TReadLimit upperReadLimit;
        lowerReadLimit.SetKey(TOwningKey(source.Range.first));
        upperReadLimit.SetKey(TOwningKey(source.Range.second));
        readRanges.emplace_back(std::move(lowerReadLimit), std::move(upperReadLimit));
        return GetChunkReader(schema, source.Id, std::move(readRanges), timestamp);
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

        return WaitFor(CreateSchemafulChunkReader(
            Bootstrap_->GetConfig()->TabletNode->ChunkReader,
            std::move(chunkReader),
            Bootstrap_->GetBlockCache(),
            schema,
            chunkMeta,
            std::move(readRanges),
            timestamp))
            .ValueOrThrow();
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTableSchema& schema,
        const TDataSource& source,
        TTimestamp timestamp)
    {
        const auto& tabletId = source.Id;

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(tabletSnapshot, NYTree::EPermission::Read);

        TOwningKey lowerBound(source.Range.first);
        TOwningKey upperBound(source.Range.second);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            schema,
            lowerBound,
            upperBound,
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

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(tabletSnapshot, NYTree::EPermission::Read);

        return CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            schema,
            keys,
            timestamp);
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

