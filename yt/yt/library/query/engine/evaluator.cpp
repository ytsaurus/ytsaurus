
#include "folding_profiler.h"
#include "cg_cache.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/helpers.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

#include <utility>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;

using NCodegen::EExecutionBackend;
using NCodegen::EOptimizationLevel;

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public IEvaluator
{
public:
    TEvaluator(
        const TExecutorConfigPtr& /*config*/,
        const NProfiling::TProfiler& /*profiler*/)
    { }

    TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulUnversionedReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const std::vector<IJoinProfilerPtr>& joinProfilers,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const NWebAssembly::TModuleBytecode& sdk,
        const IMemoryChunkProviderPtr& memoryChunkProvider,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        TFuture<TFeatureFlags> responseFeatureFlags) override
    {
        CheckQueryOptions(query, options);

        auto queryFingerprint = InferName(query, {.OmitValues = true});

        NTracing::TChildTraceContextGuard guard("QueryClient.Evaluate");
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("fragment_id", query->Id);
            traceContext->AddTag("query_fingerprint", queryFingerprint);
        });

        auto Logger = MakeQueryLogger(query);

        YT_LOG_DEBUG("Executing query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, ExecutionBackend: %v, OptimizationLevel: %v)",
            queryFingerprint,
            *query->GetReadSchema(),
            *query->GetTableSchema(),
            options.ExecutionBackend,
            options.OptimizationLevel);

        TExecutionStatistics statistics;
        NProfiling::TWallTimer wallTime;
        NProfiling::TFiberWallTimer syncTime;

        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Finalizing evaluation");
        });

        // TODO(dtorilov): Catch here WAVM::Runtime::Exception*.

        try {
            TCGVariables fragmentParams;
            auto queryInstance = Codegen(
                query,
                fragmentParams,
                joinProfilers,
                functionProfilers,
                aggregateProfilers,
                sdk,
                statistics,
                options.EnableCodeCache,
                options.UseCanonicalNullRelations,
                options.ExecutionBackend,
                options.OptimizationLevel,
                options.AllowUnorderedGroupByWithLimit,
                options.MaxJoinBatchSize);

            // NB: Function contexts need to be destroyed before queryInstance since it hosts destructors.
            auto finalizer = Finally([&] {
                fragmentParams.Clear();
            });

            auto executionContext = TExecutionContext{
                .Reader = reader,
                .Writer = writer,
                .Statistics = &statistics,
                .InputRowLimit = options.InputRowLimit,
                .OutputRowLimit = options.OutputRowLimit,
                .GroupRowLimit = options.OutputRowLimit,
                .JoinRowLimit = options.OutputRowLimit,
                .RowsetProcessingBatchSize = options.RowsetProcessingBatchSize,
                .WriteRowsetSize = options.WriteRowsetSize,
                .MaxJoinBatchSize = options.MaxJoinBatchSize,
                .Offset = query->Offset,
                .Limit = query->Limit,
                .Ordered = query->IsOrdered(options.AllowUnorderedGroupByWithLimit),
                .IsMerge = dynamic_cast<const TFrontQuery*>(query.Get()) != nullptr,
                .MemoryChunkProvider = memoryChunkProvider,
                .RequestFeatureFlags = &requestFeatureFlags,
                .ResponseFeatureFlags = responseFeatureFlags,
            };

            YT_LOG_DEBUG("Evaluating query");

            queryInstance.SetDeadline(options.Deadline);

            queryInstance.Run(
                fragmentParams.GetLiteralValues(),
                fragmentParams.GetOpaqueData(),
                fragmentParams.GetOpaqueDataSizes(),
                &executionContext);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Query evaluation failed");
            THROW_ERROR_EXCEPTION("Query evaluation failed") << ex;
        }

        statistics.SyncTime = syncTime.GetElapsedTime();
        statistics.AsyncTime = wallTime.GetElapsedTime() - statistics.SyncTime;
        statistics.ExecuteTime =
            statistics.SyncTime - statistics.ReadTime - statistics.WriteTime - statistics.CodegenTime;

        auto queryStatistics = TQueryStatistics::FromExecutionStatistics(
            statistics,
            options.StatisticsAggregation);

        YT_LOG_DEBUG("Query statistics (%v)", queryStatistics);

        return queryStatistics;
    }

private:
    TCGQueryInstance Codegen(
        TConstBaseQueryPtr query,
        TCGVariables& variables,
        const std::vector<IJoinProfilerPtr>& joinProfilers,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const NWebAssembly::TModuleBytecode& sdk,
        TExecutionStatistics& statistics,
        bool enableCodeCache,
        bool useCanonicalNullRelations,
        EExecutionBackend executionBackend,
        NCodegen::EOptimizationLevel optimizationLevel,
        bool allowUnorderedGroupByWithLimit,
        i64 maxJoinBatchSize)
    {
        llvm::FoldingSetNodeID id;

        auto makeCodegenQuery = Profile(
            query,
            &id,
            &variables,
            joinProfilers,
            useCanonicalNullRelations,
            executionBackend,
            optimizationLevel,
            functionProfilers,
            aggregateProfilers,
            sdk,
            allowUnorderedGroupByWithLimit,
            maxJoinBatchSize);

        auto Logger = MakeQueryLogger(query);

        // See condition in folding_profiler.cpp.
        bool considerLimit = query->IsOrdered(allowUnorderedGroupByWithLimit) && !query->GroupClause;

        auto queryFingerprint = InferName(query, TInferNameOptions{
            .OmitValues = true,
            .OmitOffsetAndLimit = !considerLimit,
        });

        // Query fingerprints can differ when folding ids are equal in the following case:
        // WHERE predicate is split into multiple predicates which are evaluated before join and after it.
        // Example:
        // from [a] a join [b] b where a.k and b.k
        // from [a] a join [b] b where b.k and a.k
        auto cachedQueryImage = TCodegenCacheSingleton::Compile(enableCodeCache, id, queryFingerprint, makeCodegenQuery, &statistics.CodegenTime, Logger);

        NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
        return cachedQueryImage->Image.Instantiate();
    }

    static void CheckQueryOptions(const TConstBaseQueryPtr& query, const TQueryBaseOptions& options)
    {
        THROW_ERROR_EXCEPTION_IF(options.InputRowLimit < 0, "Negative input row limit is forbidden");
        THROW_ERROR_EXCEPTION_IF(options.OutputRowLimit < 0, "Negative output row limit is forbidden");

        if (query->Offset < 0) {
            THROW_ERROR_EXCEPTION("Negative OFFSET is forbidden")
                << TErrorAttribute("offset", query->Offset);
        }

        if (query->Limit < 0) {
            THROW_ERROR_EXCEPTION("Negative LIMIT is forbidden")
                << TErrorAttribute("limit", query->Limit);
        }

        if (query->Offset + query->Limit < 0) {
            THROW_ERROR_EXCEPTION("Negative OFFSET + LIMIT is forbidden")
                << TErrorAttribute("offset_limit_sum", query->Offset + query->Limit);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IEvaluatorPtr CreateEvaluator(TExecutorConfigPtr config, const NProfiling::TProfiler& profiler)
{
    return New<TEvaluator>(std::move(config), profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
