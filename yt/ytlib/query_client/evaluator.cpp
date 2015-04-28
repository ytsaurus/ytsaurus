#include "stdafx.h"
#include "evaluator.h"

#include "helpers.h"
#include "private.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "config.h"

#ifdef YT_USE_LLVM
#include "evaluation_helpers.h"
#include "folding_profiler.h"
#endif

#include <ytlib/new_table_client/schemaful_writer.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/sync_cache.h>

#ifdef YT_USE_LLVM

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

#endif

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_LLVM

class TCachedCGQuery
    : public TSyncCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGQuery>
{
public:
    TCachedCGQuery(const llvm::FoldingSetNodeID& id, TCGQueryCallback&& function)
        : TSyncCacheValueBase(id)
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
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQuery>
{
public:
    explicit TImpl(TExecutorConfigPtr config)
        : TSyncSlruCacheBase(config->CGCache)
    { }

    TQueryStatistics Run(
        const TConstQueryPtr& query,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TExecuteQuery executeCallback,
        const IFunctionRegistryPtr functionRegistry)
    {
        TRACE_CHILD("QueryClient", "Evaluate") {
            TRACE_ANNOTATION("fragment_id", query->Id);

            auto Logger = BuildLogger(query);

            TQueryStatistics statistics;
            TDuration wallTime;

            try {
                NProfiling::TAggregatingTimingGuard timingGuard(&wallTime);

                TCGVariables fragmentParams;
                auto cgQuery = Codegen(query, fragmentParams, functionRegistry);

                LOG_DEBUG("Evaluating plan fragment");

                LOG_DEBUG("Opening writer");
                {
                    NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                    WaitFor(writer->Open(query->GetTableSchema()))
                        .ThrowOnError();
                }

                auto permanentBuffer = New<TRowBuffer>();
                auto outputBuffer = New<TRowBuffer>();
                auto intermediateBuffer = New<TRowBuffer>();

                std::vector<TRow> outputBatchRows;
                outputBatchRows.reserve(MaxRowsPerWrite);

                TExecutionContext executionContext;
                executionContext.Reader = reader;
                executionContext.Schema = &query->TableSchema;

                executionContext.LiteralRows = &fragmentParams.LiteralRows;
                executionContext.PermanentBuffer = permanentBuffer;
                executionContext.OutputBuffer = outputBuffer;
                executionContext.IntermediateBuffer = intermediateBuffer;
                executionContext.Writer = writer;
                executionContext.OutputBatchRows = &outputBatchRows;
                executionContext.Statistics = &statistics;
                executionContext.InputRowLimit = query->InputRowLimit;
                executionContext.OutputRowLimit = query->OutputRowLimit;
                executionContext.GroupRowLimit = query->OutputRowLimit;
                executionContext.JoinRowLimit = query->OutputRowLimit;
                executionContext.Limit = query->Limit;

                if (query->JoinClause) {
                    auto joinClause = query->JoinClause.Get();
                    YCHECK(executeCallback);
                    executionContext.JoinEvaluator = GetJoinEvaluator(
                        *joinClause,
                        query->WhereClause,
                        query->TableSchema,
                        executeCallback);
                }

                LOG_DEBUG("Evaluating query");
                CallCGQueryPtr(cgQuery, fragmentParams.ConstantsRowBuilder.GetRow(), &executionContext);

                LOG_DEBUG("Flushing writer");
                if (!outputBatchRows.empty()) {
                    bool shouldNotWait;
                    {
                        NProfiling::TAggregatingTimingGuard timingGuard(&statistics.WriteTime);
                        shouldNotWait = writer->Write(outputBatchRows);
                    }

                    if (!shouldNotWait) {
                        NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                        WaitFor(writer->GetReadyEvent())
                            .ThrowOnError();
                    }
                }

                LOG_DEBUG("Closing writer");
                {
                    NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                    WaitFor(writer->Close())
                        .ThrowOnError();
                }

                LOG_DEBUG("Finished evaluating plan fragment ("
                    "PermanentBufferCapacity: %v, "
                    "OutputBufferCapacity: %v, "
                    "IntermediateBufferCapacity: %v)",
                    permanentBuffer->GetCapacity(),
                    outputBuffer->GetCapacity(),
                    intermediateBuffer->GetCapacity());

                LOG_DEBUG("Query statistics (%v)", statistics);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Query evaluation failed") << ex;
            }

            statistics.SyncTime = wallTime - statistics.AsyncTime;
            statistics.ExecuteTime = statistics.SyncTime - statistics.ReadTime - statistics.WriteTime;

            TRACE_ANNOTATION("rows_read", statistics.RowsRead);
            TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
            TRACE_ANNOTATION("sync_time", statistics.SyncTime);
            TRACE_ANNOTATION("async_time", statistics.AsyncTime);
            TRACE_ANNOTATION("execute_time", statistics.ExecuteTime);
            TRACE_ANNOTATION("read_time", statistics.ReadTime);
            TRACE_ANNOTATION("write_time", statistics.WriteTime);
            TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
            TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

            return statistics;
        }
    }

private:
    TCGQueryCallback Codegen(
        const TConstQueryPtr& query,
        TCGVariables& variables,
        const IFunctionRegistryPtr functionRegistry)
    {
        llvm::FoldingSetNodeID id;

        auto makeCodegenQuery = Profile(query, &id, &variables, nullptr, functionRegistry);

        auto Logger = BuildLogger(query);

        auto cgQuery = Find(id);
        if (cgQuery) {
            LOG_TRACE("Codegen cache hit");
        } else {
            LOG_DEBUG("Codegen cache miss");
            try {
                TRACE_CHILD("QueryClient", "Compile") {
                    LOG_DEBUG("Started compiling fragment");
                    cgQuery = New<TCachedCGQuery>(id, makeCodegenQuery());
                    LOG_DEBUG("Finished compiling fragment");
                    TryInsert(cgQuery, &cgQuery);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to compile a query fragment")
                    << ex;
            }
        }

        return cgQuery->GetQueryCallback();
    }

    static void CallCGQuery(
        const TCGQueryCallback& cgQuery,
        TRow constants,
        TExecutionContext* executionContext)
    {
#ifndef NDEBUG
        int dummy;
        executionContext->StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif
        cgQuery(constants, executionContext);
    }

    void(*volatile CallCGQueryPtr)(
        const TCGQueryCallback& cgQuery,
        TRow constants,
        TExecutionContext* executionContext) = CallCGQuery;

};

#endif

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator(TExecutorConfigPtr config)
#ifdef YT_USE_LLVM
    : Impl_(New<TImpl>(std::move(config)))
#endif
{ }

TEvaluator::~TEvaluator()
{ }

TQueryStatistics TEvaluator::RunWithExecutor(
    const TConstQueryPtr& query,
    ISchemafulReaderPtr reader,
    ISchemafulWriterPtr writer,
    TExecuteQuery executeCallback,
    const IFunctionRegistryPtr functionRegistry)
{
#ifdef YT_USE_LLVM
    return Impl_->Run(query, std::move(reader), std::move(writer), executeCallback, functionRegistry);
#else
    THROW_ERROR_EXCEPTION("Query evaluation is not supported in this build");
#endif
}

TQueryStatistics TEvaluator::Run(
    const TConstQueryPtr& query,
    ISchemafulReaderPtr reader,
    ISchemafulWriterPtr writer,
    const IFunctionRegistryPtr functionRegistry)
{
    return RunWithExecutor(query, std::move(reader), std::move(writer), nullptr, functionRegistry);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
