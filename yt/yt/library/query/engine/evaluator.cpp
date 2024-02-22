
#include "folding_profiler.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TCachedCGQueryImage
    : public TAsyncCacheValueBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
{
    const TString Fingerprint;
    const TCGQueryImage Image;

    TCachedCGQueryImage(
        const llvm::FoldingSetNodeID& id,
        TString fingerprint,
        TCGQueryImage image)
        : TAsyncCacheValueBase(id)
        , Fingerprint(std::move(fingerprint))
        , Image(std::move(image))
    { }
};

using TCachedCGQueryImagePtr = TIntrusivePtr<TCachedCGQueryImage>;

class TEvaluator
    : public TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
    , public IEvaluator
{
public:
    TEvaluator(
        const TExecutorConfigPtr& config,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(config->CGCache, profiler.WithPrefix("/cg_cache"))
    { }

    TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulUnversionedReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const IMemoryChunkProviderPtr& memoryChunkProvider,
        const TQueryBaseOptions& options) override
    {
        auto queryFingerprint = InferName(query, {.OmitValues = true});

        NTracing::TChildTraceContextGuard guard("QueryClient.Evaluate");
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("fragment_id", query->Id);
            traceContext->AddTag("query_fingerprint", queryFingerprint);
        });

        auto Logger = MakeQueryLogger(query);

        YT_LOG_DEBUG("Executing query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, ExecutionBackend: %v)",
            queryFingerprint,
            *query->GetReadSchema(),
            *query->GetTableSchema(),
            options.ExecutionBackend);

        TQueryStatistics statistics;
        NProfiling::TWallTimer wallTime;
        NProfiling::TFiberWallTimer syncTime;

        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Finalizing evaluation");
        });

        try {
            TCGVariables fragmentParams;
            auto queryInstance = Codegen(
                query,
                fragmentParams,
                joinProfiler,
                functionProfilers,
                aggregateProfilers,
                statistics,
                options.EnableCodeCache,
                options.UseCanonicalNullRelations,
                options.ExecutionBackend);

            // NB: Function contexts need to be destroyed before queryInstance since it hosts destructors.
            auto finalizer = Finally([&] () {
                fragmentParams.Clear();
            });

            TExecutionContext executionContext;
            executionContext.Reader = reader;
            executionContext.Writer = writer;
            executionContext.Statistics = &statistics;
            executionContext.InputRowLimit = options.InputRowLimit;
            executionContext.OutputRowLimit = options.OutputRowLimit;
            executionContext.GroupRowLimit = options.OutputRowLimit;
            executionContext.JoinRowLimit = options.OutputRowLimit;
            executionContext.Offset = query->Offset;
            executionContext.Limit = query->Limit;
            executionContext.Ordered = query->IsOrdered();
            executionContext.IsMerge = dynamic_cast<const TFrontQuery*>(query.Get());
            executionContext.MemoryChunkProvider = memoryChunkProvider;

            YT_LOG_DEBUG("Evaluating query");

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

        YT_LOG_DEBUG("Query statistics (%v)", statistics);

        // TODO(prime): place these into trace log
        //    TRACE_ANNOTATION("rows_read", statistics.RowsRead);
        //    TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
        //    TRACE_ANNOTATION("sync_time", statistics.SyncTime);
        //    TRACE_ANNOTATION("async_time", statistics.AsyncTime);
        //    TRACE_ANNOTATION("execute_time", statistics.ExecuteTime);
        //    TRACE_ANNOTATION("read_time", statistics.ReadTime);
        //    TRACE_ANNOTATION("write_time", statistics.WriteTime);
        //    TRACE_ANNOTATION("codegen_time", statistics.CodegenTime);
        //    TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
        //    TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

        return statistics;
    }

private:
    TCGQueryInstance Codegen(
        TConstBaseQueryPtr query,
        TCGVariables& variables,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        TQueryStatistics& statistics,
        bool enableCodeCache,
        bool useCanonicalNullRelations,
        EExecutionBackend executionBackend)
    {
        llvm::FoldingSetNodeID id;

        auto makeCodegenQuery = Profile(
            query,
            &id,
            &variables,
            joinProfiler,
            useCanonicalNullRelations,
            executionBackend,
            functionProfilers,
            aggregateProfilers);

        auto Logger = MakeQueryLogger(query);

        // See condition in folding_profiler.cpp.
        bool considerLimit = query->IsOrdered() && !query->GroupClause;

        auto queryFingerprint = InferName(query, TInferNameOptions{true, true, true, !considerLimit});
        auto compileWithLogging = [&] {
            NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
            YT_LOG_DEBUG("Started compiling fragment");
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
            auto image = makeCodegenQuery();
            auto cachedImage = New<TCachedCGQueryImage>(id, queryFingerprint, std::move(image));
            YT_LOG_DEBUG("Finished compiling fragment");
            return cachedImage;
        };

        TCachedCGQueryImagePtr cachedQueryImage;
        if (enableCodeCache) {
            auto cookie = BeginInsert(id);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Codegen cache miss: generating query evaluator");

                try {
                    cookie.EndInsert(compileWithLogging());
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(ex, "Failed to compile a query fragment");
                    cookie.Cancel(TError(ex).Wrap("Failed to compile a query fragment"));
                }
            }

            cachedQueryImage = WaitForFast(cookie.GetValue())
                .ValueOrThrow();

            // Query fingerprints can differ when folding ids are equal in the following case:
            // WHERE predicate is split into multiple predicates which are evaluated before join and after it.
            // Example:
            // from [a] a join [b] b where a.k and b.k
            // from [a] a join [b] b where b.k and a.k
        } else {
            YT_LOG_DEBUG("Codegen cache disabled");

            cachedQueryImage = compileWithLogging();
        }

        NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
        return cachedQueryImage->Image.Instantiate();
    }
};

////////////////////////////////////////////////////////////////////////////////

IEvaluatorPtr CreateEvaluator(TExecutorConfigPtr config, const NProfiling::TProfiler& profiler)
{
    return New<TEvaluator>(std::move(config), profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
