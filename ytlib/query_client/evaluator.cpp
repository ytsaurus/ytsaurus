#include "evaluator.h"
#include "private.h"
#include "config.h"
#include "evaluation_helpers.h"
#include "folding_profiler.h"
#include "helpers.h"
#include "query.h"
#include "query_statistics.h"

#include <yt/ytlib/table_client/schemaful_writer.h>

#include <yt/core/misc/async_cache.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/finally.h>

#include <llvm/ADT/FoldingSet.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCachedCGQuery
    : public TAsyncCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGQuery>
{
public:
    TCachedCGQuery(const llvm::FoldingSetNodeID& id, TCGQueryCallback&& function)
        : TAsyncCacheValueBase(id)
        , Function_(std::move(function))
    { }

    TCGQueryCallback GetQueryCallback()
    {
        return Function_;
    }

private:
    TCGQueryCallback Function_;
};

typedef TIntrusivePtr<TCachedCGQuery> TCachedCGQueryPtr;

class TEvaluator::TImpl
    : public TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQuery>
{
public:
    TImpl(
        TExecutorConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(
            config->CGCache,
            profiler.GetPathPrefix() ? NProfiling::TProfiler(profiler.GetPathPrefix() + "/cg_cache") : NProfiling::TProfiler())
    { }

    TQueryStatistics Run(
        TConstBaseQueryPtr query,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TJoinSubqueryProfiler joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const TQueryBaseOptions& options)
    {
        TRACE_CHILD("QueryClient", "Evaluate") {
            TRACE_ANNOTATION("fragment_id", query->Id);
            auto queryFingerprint = InferName(query, true);
            TRACE_ANNOTATION("query_fingerprint", queryFingerprint);

            auto Logger = MakeQueryLogger(query);

            LOG_DEBUG("Executing query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v)",
                queryFingerprint,
                query->GetReadSchema(),
                query->GetTableSchema());

            TQueryStatistics statistics;
            NProfiling::TWallTimer wallTime;
            NProfiling::TCpuTimer syncTime;

            auto finalLogger = Finally([&] () {
                LOG_DEBUG("Finalizing evaluation");
            });

            try {
                TCGVariables fragmentParams;
                auto cgQuery = Codegen(
                    query,
                    fragmentParams,
                    joinProfiler,
                    functionProfilers,
                    aggregateProfilers,
                    statistics,
                    options.EnableCodeCache,
                    options.UseMultijoin);

                auto finalizer = Finally([&] () {
                    fragmentParams.Clear();
                });

                LOG_DEBUG("Evaluating plan fragment");

                // NB: function contexts need to be destroyed before cgQuery since it hosts destructors.
                TExecutionContext executionContext;
                executionContext.Reader = reader;
                executionContext.Writer = writer;
                executionContext.Statistics = &statistics;
                executionContext.InputRowLimit = options.InputRowLimit;
                executionContext.OutputRowLimit = options.OutputRowLimit;
                executionContext.GroupRowLimit = options.OutputRowLimit;
                executionContext.JoinRowLimit = options.OutputRowLimit;
                executionContext.Limit = query->Limit;
                executionContext.IsOrdered = query->IsOrdered();

                LOG_DEBUG("Evaluating query");

                CallCGQueryPtr(
                    cgQuery,
                    fragmentParams.GetLiteralValues(),
                    fragmentParams.GetOpaqueData(),
                    &executionContext);

            } catch (const std::exception& ex) {
                LOG_DEBUG("Query evaluation failed");
                THROW_ERROR_EXCEPTION("Query evaluation failed") << ex;
            }

            statistics.SyncTime = syncTime.GetElapsedTime();
            statistics.AsyncTime = wallTime.GetElapsedTime() - statistics.SyncTime;
            statistics.ExecuteTime =
                statistics.SyncTime - statistics.ReadTime - statistics.WriteTime - statistics.CodegenTime;

            LOG_DEBUG("Query statistics (%v)", statistics);

            TRACE_ANNOTATION("rows_read", statistics.RowsRead);
            TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
            TRACE_ANNOTATION("sync_time", statistics.SyncTime);
            TRACE_ANNOTATION("async_time", statistics.AsyncTime);
            TRACE_ANNOTATION("execute_time", statistics.ExecuteTime);
            TRACE_ANNOTATION("read_time", statistics.ReadTime);
            TRACE_ANNOTATION("write_time", statistics.WriteTime);
            TRACE_ANNOTATION("codegen_time", statistics.CodegenTime);
            TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
            TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

            return statistics;
        }
    }

private:
    TCGQueryCallback Codegen(
        TConstBaseQueryPtr query,
        TCGVariables& variables,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        TQueryStatistics& statistics,
        bool enableCodeCache,
        bool useMultijoin)
    {
        llvm::FoldingSetNodeID id;

        auto makeCodegenQuery = Profile(
            query,
            &id,
            &variables,
            joinProfiler,
            useMultijoin,
            functionProfilers,
            aggregateProfilers);

        auto Logger = MakeQueryLogger(query);

        auto compileWithLogging = [&] () {
            TRACE_CHILD("QueryClient", "Compile") {
                LOG_DEBUG("Started compiling fragment");
                NProfiling::TCpuTimingGuard timingGuard(&statistics.CodegenTime);
                auto cgQuery = New<TCachedCGQuery>(id, makeCodegenQuery());
                LOG_DEBUG("Finished compiling fragment");
                return cgQuery;
            }
        };

        TCachedCGQueryPtr cgQuery;
        if (enableCodeCache) {
            auto cookie = BeginInsert(id);
            if (cookie.IsActive()) {
                LOG_DEBUG("Codegen cache miss: generating query evaluator");

                try {
                    cookie.EndInsert(compileWithLogging());
                } catch (const std::exception& ex) {
                    cookie.Cancel(TError(ex).Wrap("Failed to compile a query fragment"));
                }
            }

            cgQuery = WaitFor(cookie.GetValue())
                .ValueOrThrow();
        } else {
            LOG_DEBUG("Codegen cache disabled");

            cgQuery = compileWithLogging();
        }

        return cgQuery->GetQueryCallback();
    }

    static void CallCGQuery(
        const TCGQueryCallback& cgQuery,
        TValue* literals,
        void* const* opaqueValues,
        TExecutionContext* executionContext)
    {
        cgQuery(literals, opaqueValues, executionContext);
    }

    void(*volatile CallCGQueryPtr)(
        const TCGQueryCallback& cgQuery,
        TValue* literals,
        void* const* opaqueValues,
        TExecutionContext* executionContext) = CallCGQuery;

};

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator(
    TExecutorConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Impl_(New<TImpl>(
        std::move(config),
        profiler))
{ }

TQueryStatistics TEvaluator::Run(
    TConstBaseQueryPtr query,
    ISchemafulReaderPtr reader,
    ISchemafulWriterPtr writer,
    TJoinSubqueryProfiler joinProfiler,
    TConstFunctionProfilerMapPtr functionProfilers,
    TConstAggregateProfilerMapPtr aggregateProfilers,
    const TQueryBaseOptions& options)
{
    return Impl_->Run(
        std::move(query),
        std::move(reader),
        std::move(writer),
        std::move(joinProfiler),
        functionProfilers,
        aggregateProfilers,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
