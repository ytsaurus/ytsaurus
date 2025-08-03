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
        const TSubplanHoldersPtr& subplanHolders)
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
    const TSubplanHoldersPtr SubplanHolders_;

    std::queue<ISchemafulUnversionedReaderPtr> PrefetchWindow_;
    i64 PrefetchWindowSize_ = 1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

ISchemafulUnversionedReaderPtr CreateAdaptiveOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
    const NDetail::TSubplanHoldersPtr& subplanHolders,
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

    YT_LOG_DEBUG("Creating reader (Ordered: %v, Prefetch: %v, SplitCount: %v, Offset: %v, Limit: %v, UseAdaptiveOrderedSchemafulReader: %v)",
        ordered,
        prefetch,
        splitCount,
        offset,
        limit,
        useAdaptiveOrderedSchemafulReader);

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
        } else {
            queryStatistics.AddInnerStatistics({});
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
