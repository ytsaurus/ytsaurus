
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>
#include <yt/yt/library/query/engine_api/new_range_inferrer.h>
#include <yt/yt/library/query/engine_api/shuffling_reader.h>

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/writer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Coordinator");

////////////////////////////////////////////////////////////////////////////////

std::pair<TConstFrontQueryPtr, TConstQueryPtr> GetDistributedQueryPattern(const TConstQueryPtr& query)
{
    auto bottomQuery = New<TQuery>();

    bottomQuery->Schema.Original = query->Schema.Original;
    bottomQuery->Schema.Mapping = query->Schema.Mapping;

    bottomQuery->JoinClauses = query->JoinClauses;
    bottomQuery->OrderClause = query->OrderClause;
    bottomQuery->HavingClause = query->HavingClause;
    bottomQuery->GroupClause = query->GroupClause;
    bottomQuery->Offset = 0;
    bottomQuery->Limit = query->Offset + query->Limit;
    bottomQuery->UseDisjointGroupBy = query->UseDisjointGroupBy;
    bottomQuery->InferRanges = query->InferRanges;
    bottomQuery->IsFinal = false;
    bottomQuery->WhereClause = query->WhereClause;
    bottomQuery->IsReverseScan = false;

    auto frontQuery = New<TFrontQuery>();

    frontQuery->GroupClause = query->GroupClause;
    frontQuery->HavingClause = query->HavingClause;
    // When the scan is reversed, tablets are read in descending key order so
    // the merged stream is already sorted correctly. The front query only
    // needs to apply OFFSET/LIMIT without re-sorting.
    frontQuery->OrderClause = query->IsReverseScan ? nullptr : query->OrderClause;
    frontQuery->Offset = query->Offset;
    frontQuery->Limit = query->Limit;
    frontQuery->IsFinal = query->IsFinal;
    frontQuery->ProjectClause = query->ProjectClause;
    frontQuery->Schema = bottomQuery->GetTableSchema();
    frontQuery->IsReverseScan = query->IsReverseScan;

    return {frontQuery, bottomQuery};
}

TSharedRange<TRowRange> GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchemaPtr& tableSchema,
    const TKeyColumns& keyColumns,
    TObjectId tableId,
    const TSharedRange<TRowRange>& /*ranges*/,
    const TRowBufferPtr& /*rowBuffer*/,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    bool forceLightRangeInference,
    TGuid queryId)
{
    auto Logger = MakeQueryLogger(queryId);

    YT_LOG_DEBUG("Inferring ranges from predicate (ForceLightRangeInference: %v)",
        forceLightRangeInference);

    TSharedRange<TRowRange> result;

    if (options.NewRangeInference) {
        result = CreateNewRangeInferrer(
            predicate,
            tableSchema,
            keyColumns,
            evaluatorCache,
            GetBuiltinConstraintExtractors(),
            options,
            memoryChunkProvider,
            forceLightRangeInference);
    } else {
        result = CreateRangeInferrer(
            predicate,
            tableSchema,
            keyColumns,
            evaluatorCache,
            rangeExtractors,
            options);
    }

    auto keyRangeFormatter = [] (const TRowRange& range) {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    for (const auto& range : result) {
        YT_LOG_DEBUG_IF(options.VerboseLogging, "Inferred range (TableId: %v, Range: %v)",
            tableId,
            keyRangeFormatter(range));
    }

    return result;
}

TSharedRange<TRowRange> GetPrunedRanges(
    const TConstQueryPtr& query,
    TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    return GetPrunedRanges(
        query->WhereClause,
        query->Schema.Original,
        query->GetKeyColumns(),
        tableId,
        ranges,
        rowBuffer,
        evaluatorCache,
        rangeExtractors,
        options,
        memoryChunkProvider,
        query->ForceLightRangeInference,
        query->Id);
}

////////////////////////////////////////////////////////////////////////////////

int GetCommonPrefixLength(TUnversionedValueRange lhs, TUnversionedValueRange rhs)
{
    int limit = std::min(std::ssize(lhs), std::ssize(rhs));

    for (int index = 0; index < limit; ++index) {
        if (lhs[index] != rhs[index]) {
            return index;
        }
    }

    return limit;
}

int GetLongestCommonPrimaryKeyPrefixLength(TRange<TRowRange> ranges)
{
    auto prefix = std::optional<TUnversionedValueRange>();

    auto updatePrefix = [&] (TUnversionedRow row) {
        if (prefix == std::nullopt) {
            prefix = TUnversionedValueRange(row.Begin(), GetSignificantWidth(row));
        } else {
            i64 commonPrefixLength = GetCommonPrefixLength(
                *prefix,
                TUnversionedValueRange(row.Begin(), row.End()));

            prefix = prefix->Slice(0, commonPrefixLength);
        }
    };

    for (const auto& [begin, end] : ranges) {
        updatePrefix(begin);
        updatePrefix(end);
    }

    if (prefix.has_value()) {
        return std::ssize(*prefix);
    }

    return 0;
}

std::pair<TDataSource, TConstQueryPtr> InferRanges(
    const IColumnEvaluatorCachePtr& columnEvaluatorCache,
    TConstQueryPtr query,
    const TDataSource& dataSource,
    const TQueryOptions& options,
    TRowBufferPtr rowBuffer,
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    const NLogging::TLogger& Logger)
{
    auto tableId = dataSource.ObjectId;
    auto ranges = dataSource.Ranges;
    auto keys = dataSource.Keys;

    TConstQueryPtr resultQuery;

    // TODO(lukyan): Infer ranges if no initial ranges or keys?
    if (!keys && query->InferRanges) {
        ranges = GetPrunedRanges(
            query,
            tableId,
            ranges,
            rowBuffer,
            columnEvaluatorCache,
            GetBuiltinRangeExtractors(),
            options,
            memoryChunkProvider);

        YT_LOG_DEBUG("Ranges are inferred (RangeCount: %v, TableId: %v)",
            ranges.Size(),
            tableId);

        auto newQuery = New<TQuery>(*query);

        if (query->WhereClause && !ranges.Empty()) {
            newQuery->WhereClause = EliminatePredicate(
                ranges,
                query->WhereClause,
                query->GetKeyColumns());
        }

        if (auto* orderClause = newQuery->OrderClause.Get()) {
            auto fixedKeyPrefix = GetLongestCommonPrimaryKeyPrefixLength(ranges);
            if (CanOmitOrderBy(fixedKeyPrefix, orderClause->OrderItems, newQuery->GetKeyColumns())) {
                YT_LOG_DEBUG("Omitting ORDER BY clause (FixedKeyPrefix: %v)", fixedKeyPrefix);

                newQuery->OrderClause.Reset();
            }
        }

        resultQuery = newQuery;
    } else {
        resultQuery = query;
    }

    TDataSource inferredDataSource{
        .ObjectId = tableId,
        .Ranges = ranges,
        .Keys = keys
    };

    return {inferredDataSource, resultQuery};
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveReaderGenerator
{
public:
    TAdaptiveReaderGenerator(
        std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
        const TSubplanFutureHoldersPtr& subplanHolders)
        : GetNextReader_(getNextReader)
        , SubplanHolders_(subplanHolders)
    { }

    ISchemafulUnversionedReaderPtr Next()
    {
        if (PrefetchWindow_.empty()) {
            for (i64 i = 0; i < PrefetchWindowSize_; ++i) {
                if (auto nextReader = GetNextReader_()) {
                    PrefetchWindow_.push(nextReader);
                }
            }
            PrefetchWindowSize_ *= PrefetchWindowGrowthFactor;
        }

        if (PrefetchWindow_.empty()) {
            return nullptr;
        }

        auto result = PrefetchWindow_.front();
        PrefetchWindow_.pop();
        return result;
    }

private:
    static constexpr i64 PrefetchWindowGrowthFactor = 2;

    const std::function<ISchemafulUnversionedReaderPtr()> GetNextReader_;
    const TSubplanFutureHoldersPtr SubplanHolders_;

    std::queue<ISchemafulUnversionedReaderPtr> PrefetchWindow_;
    i64 PrefetchWindowSize_ = 1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

ISchemafulUnversionedReaderPtr CreateAdaptiveOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
    const TSubplanFutureHoldersPtr& subplanHolders,
    i64 /*offset*/,
    i64 /*limit*/,
    bool useAdaptiveOrderedSchemafulReader)
{
    if (!useAdaptiveOrderedSchemafulReader) {
        return CreateOrderedSchemafulReader(std::move(getNextReader));
    }

    YT_LOG_DEBUG("Use adaptive ordered schemaful reader");

    auto generator = NDetail::TAdaptiveReaderGenerator(getNextReader, subplanHolders);
    auto readerGenerator = [generator = std::move(generator)] () mutable -> ISchemafulUnversionedReaderPtr {
        return generator.Next();
    };
    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics CoordinateAndExecute(
    EScanOrder scanOrder,
    bool prefetch,
    int splitCount,
    i64 offset,
    i64 limit,
    bool useAdaptiveOrderedSchemafulReader,
    TSubQueryEvaluator evaluateSubQuery,
    TTopQueryEvaluator evaluateTopQuery,
    TSubplanFutureHoldersPtr subplanHolders)
{
    if (!subplanHolders) {
        subplanHolders = New<TSubplanFutureHolders>();
    }

    auto responseFeatureFlags = NewPromise<TFeatureFlags>();

    if (splitCount == 0) {
        // If the filtering predicate is false, we will not send subplans to nodes.
        // Therefore, we will not create any reader, so we can choose any kind of feature flags here.
        responseFeatureFlags.Set(MostFreshFeatureFlags());
    }

    auto subqueryReaderCreator = [&] () mutable -> ISchemafulUnversionedReaderPtr {
        auto evaluateResult = evaluateSubQuery();
        if (evaluateResult.Reader) {
            subplanHolders->push_back(evaluateResult.Statistics);

            // One single feature flags response is enough, ignore others.
            responseFeatureFlags.TrySetFrom(evaluateResult.ResponseFeatureFlags);
        }
        return evaluateResult.Reader;
    };

    YT_LOG_DEBUG("Creating reader (ScanOrder: %v, Prefetch: %v, SplitCount: %v, Offset: %v, Limit: %v, UseAdaptiveOrderedSchemafulReader: %v)",
        scanOrder,
        prefetch,
        splitCount,
        offset,
        limit,
        useAdaptiveOrderedSchemafulReader);

    // TODO: Use separate condition for prefetch after protocol update
    ISchemafulUnversionedReaderPtr topReader;
    switch (scanOrder) {
        case EScanOrder::Ordered:
        case EScanOrder::Reversed:
            topReader = prefetch
                ? CreateFullPrefetchingOrderedSchemafulReader(std::move(subqueryReaderCreator))
                : CreateAdaptiveOrderedSchemafulReader(std::move(subqueryReaderCreator), subplanHolders, offset, limit, useAdaptiveOrderedSchemafulReader);
            break;
        case EScanOrder::Unordered:
            topReader = CreateUnorderedSchemafulReader(std::move(subqueryReaderCreator), /*concurrency*/ splitCount);
            break;
    }

    auto queryStatistics = evaluateTopQuery(std::move(topReader), responseFeatureFlags);

    for (int index = 0; index < std::ssize(*subplanHolders); ++index) {
        auto subqueryStatisticsOrError = WaitForFast((*subplanHolders)[index].Get());
        if (subqueryStatisticsOrError.IsOK()) {
            auto subqueryStatistics = std::move(subqueryStatisticsOrError).ValueOrThrow();
            queryStatistics.AddInnerStatistics(std::move(subqueryStatistics));
        } else {
            queryStatistics.AddInnerStatistics({});
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics CoordinateAndExecuteWithShuffle(
    int splitCount,
    int groupKeyPrefix,
    TSubQueryEvaluator evaluateSubQuery,
    TMiddleQueryEvaluator evaluateMiddleQuery,
    TTopQueryEvaluator evaluateTopQuery,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    if (splitCount == 0) {
        return {};
    }

    constexpr int ParallelizationFactor = 8;
    int destinationCount = std::min(ParallelizationFactor, splitCount);

    std::vector<TFuture<TQueryStatistics>> bottomSubqueryStatistics;
    std::vector<TFuture<TFeatureFlags>> subqueryFeatureFlags;
    std::vector<ISchemafulUnversionedReaderPtr> readers;
    readers.reserve(splitCount);
    bottomSubqueryStatistics.reserve(splitCount);
    subqueryFeatureFlags.reserve(splitCount);
    for (int index = 0; index < splitCount; ++index) {
        auto bottomResult = evaluateSubQuery();
        bottomSubqueryStatistics.push_back(std::move(bottomResult.Statistics));
        subqueryFeatureFlags.push_back(bottomResult.ResponseFeatureFlags);
        readers.push_back(bottomResult.Reader);
    }

    auto anyFeatureFlag = AnySucceeded(subqueryFeatureFlags, {.CancelInputOnShortcut = false});

    auto [shuffledReaders, shuffleFutures] = ShuffleByPrefixHash(
        readers,
        groupKeyPrefix,
        destinationCount,
        GetCurrentInvoker(),
        memoryChunkProvider);

    readers.clear();
    readers.reserve(shuffledReaders.size());
    std::vector<TFuture<TQueryStatistics>> middleSubqueryStatistics;
    middleSubqueryStatistics.reserve(shuffledReaders.size());
    for (const auto& reader : shuffledReaders) {
        auto middleResult = evaluateMiddleQuery(reader, anyFeatureFlag);
        readers.push_back(std::move(middleResult.Reader));
        middleSubqueryStatistics.push_back(std::move(middleResult.Statistics));
    }

    auto getNextReader = [readers = std::move(readers), index = 0] () mutable {
        if (index >= std::ssize(readers)) {
            return ISchemafulUnversionedReaderPtr();
        }
        return readers[index++];
    };
    auto finalReader = CreateUnorderedSchemafulReader(getNextReader, destinationCount);

    // TODO(sabdenovch): No clue if MostFreshFeatureFlags would be more or less correct.
    // Middle stage is executed locally, MostFreshFeatureFlags makes sense.
    // But the data itself comes from the bottom subqueries, just shuffled and merged a little more.
    // Input might have traits of different code versions.
    auto statisticsFuture = BIND(evaluateTopQuery, finalReader, anyFeatureFlag)
        .AsyncVia(GetCurrentInvoker())
        .Run();

    shuffleFutures.push_back(statisticsFuture.AsVoid());
    for (const auto& future : middleSubqueryStatistics) {
        shuffleFutures.push_back(future.AsVoid());
    }
    for (const auto& future : bottomSubqueryStatistics) {
        shuffleFutures.push_back(future.AsVoid());
    }

    WaitFor(AllSucceeded(shuffleFutures))
        .ThrowOnError();

    auto statistics = WaitForFast(statisticsFuture)
        .ValueOrThrow();

    statistics.InnerStatistics = WaitForFast(AllSucceeded(middleSubqueryStatistics))
        .ValueOrThrow();

    statistics.InnerStatistics.front().InnerStatistics = WaitForFast(AllSucceeded(bottomSubqueryStatistics))
        .ValueOrThrow();

    statistics.GroupedRowCount.SetTotal(std::accumulate(
        statistics.InnerStatistics.begin(),
        statistics.InnerStatistics.end(),
        static_cast<i64>(0),
        [] (i64 groupedRowCount, const TQueryStatistics& innerStatistics) {
            return groupedRowCount + innerStatistics.GroupedRowCount.GetTotal();
        }));

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
