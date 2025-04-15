#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>
#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/helpers.h>

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

    auto frontQuery = New<TFrontQuery>();

    frontQuery->GroupClause = query->GroupClause;
    frontQuery->HavingClause = query->HavingClause;
    frontQuery->OrderClause = query->OrderClause;
    frontQuery->Offset = query->Offset;
    frontQuery->Limit = query->Limit;
    frontQuery->IsFinal = query->IsFinal;
    frontQuery->ProjectClause = query->ProjectClause;
    frontQuery->Schema = bottomQuery->GetTableSchema();

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

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSubplanHolders)

struct TSubplanHolders final
    : public std::vector<TFutureHolder<TQueryStatistics>> // Use TFutureHolder to prevent leaking subqueries.
{ };

DEFINE_REFCOUNTED_TYPE(TSubplanHolders)

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveReaderGenerator
{
public:
    TAdaptiveReaderGenerator(
        std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
        const TSubplanHoldersPtr& subplanHolders,
        i64 offset,
        i64 limit)
        : GetNextReader_(getNextReader)
        , SubplanHolders_(subplanHolders)
        , Offset_(offset)
        , Limit_(limit)
    {
        YT_VERIFY(Limit_ != UnorderedReadHint && Limit_ != OrderedReadWithPrefetchHint);
    }

    ISchemafulUnversionedReaderPtr Next()
    {
        if (EstimateProcessedRowCount() < (Limit_ + Offset_) * FullPrefetchThreshold) {
            return GetNextReader_();
        }

        while (auto nextReader = GetNextReader_()) {
            FullPrefetch_.push_back(nextReader);
        }

        if (FullPrefetchIndex_ == std::ssize(FullPrefetch_)) {
            return nullptr;
        }

        return FullPrefetch_[FullPrefetchIndex_++];
    }

private:
    // We will switch to a full parallel prefetch after we have read `(OFFSET + LIMIT) * FullPrefetchThreshold` rows,
    // because the predicate turned out to be too selective.
    static constexpr i64 FullPrefetchThreshold = 3;

    const std::function<ISchemafulUnversionedReaderPtr()> GetNextReader_;
    const TSubplanHoldersPtr SubplanHolders_;
    const i64 Offset_ = 0;
    const i64 Limit_ = 0;

    std::vector<ISchemafulUnversionedReaderPtr> FullPrefetch_;
    i64 FullPrefetchIndex_ = 0;

    i64 EstimateProcessedRowCount()
    {
        i64 rowCount = 0;

        for (auto& subplan : *SubplanHolders_) {
            if (!subplan->IsSet()) {
                continue;
            }

            auto statisticsOrError = WaitForFast(subplan.Get());
            if (!statisticsOrError.IsOK()) {
                continue;
            }

            rowCount += statisticsOrError.Value().RowsRead;
        }

        return rowCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

// If the query text uses `LIMIT` without `ORDER BY`, we will first try to read the minimum required number of rows.
// If the filtering predicate turns out to be too selective, we switch to full prefetching.
ISchemafulUnversionedReaderPtr CreateAdaptiveOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
    const NDetail::TSubplanHoldersPtr& subplanHolders,
    i64 offset,
    i64 limit,
    bool useAdaptiveOrderedSchemafulReader)
{
    if (!useAdaptiveOrderedSchemafulReader) {
        return CreateOrderedSchemafulReader(std::move(getNextReader));
    }

    auto generator = NDetail::TAdaptiveReaderGenerator(getNextReader, subplanHolders, offset, limit);
    auto readerGenerator = [generator = std::move(generator)] () mutable -> ISchemafulUnversionedReaderPtr {
        return generator.Next();
    };
    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics CoordinateAndExecute(
    bool ordered,
    bool prefetch,
    int splitCount,
    i64 offset,
    i64 limit,
    bool useAdaptiveOrderedSchemafulReader,
    TSubQueryEvaluator evaluateSubQuery,
    TTopQueryEvaluator evaluateTopQuery)
{
    std::vector<ISchemafulUnversionedReaderPtr> splitReaders;

    auto subplanHolders = New<NDetail::TSubplanHolders>();

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

    // TODO: Use separate condition for prefetch after protocol update
    auto topReader = ordered
        ? (prefetch
            ? CreateFullPrefetchingOrderedSchemafulReader(std::move(subqueryReaderCreator))
            : CreateAdaptiveOrderedSchemafulReader(std::move(subqueryReaderCreator), subplanHolders, offset, limit, useAdaptiveOrderedSchemafulReader))
        : CreateUnorderedSchemafulReader(std::move(subqueryReaderCreator), /*concurrency*/ splitCount);

    auto queryStatistics = evaluateTopQuery(std::move(topReader), responseFeatureFlags);

    for (int index = 0; index < std::ssize(*subplanHolders); ++index) {
        auto subqueryStatisticsOrError = WaitForFast((*subplanHolders)[index].Get());
        if (subqueryStatisticsOrError.IsOK()) {
            auto subqueryStatistics = std::move(subqueryStatisticsOrError).ValueOrThrow();
            queryStatistics.AddInnerStatistics(std::move(subqueryStatistics));
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
