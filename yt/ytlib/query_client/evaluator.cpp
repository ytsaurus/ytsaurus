#include "stdafx.h"
#include "evaluator.h"

#include "helpers.h"
#include "private.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "config.h"

#ifdef YT_USE_LLVM

#include "cg_fragment_compiler.h"
#include "cg_routines.h"

#endif

#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/query_client/plan_fragment.pb.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/sync_cache.h>

#include <core/logging/log.h>

#include <core/tracing/trace_context.h>

#ifdef YT_USE_LLVM

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

#endif

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_LLVM

// Folding profiler computes a strong structural hash used to cache query fragments.

DECLARE_ENUM(EFoldingObjectType,
    (ScanOp)
    (FilterOp)
    (GroupOp)
    (ProjectOp)

    (LiteralExpr)
    (ReferenceExpr)
    (FunctionExpr)
    (BinaryOpExpr)
    (InOpExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
);

class TFoldingProfiler
{
public:
    explicit TFoldingProfiler(
        llvm::FoldingSetNodeID& id,
        TCGBinding& binding,
        TCGVariables& variables)
        : Id_(id)
        , Binding_(binding)
        , Variables_(variables)
    { }

    void Profile(const TConstQueryPtr& query);
    void Profile(const TConstExpressionPtr& expr);
    void Profile(const TNamedItem& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

private:
    llvm::FoldingSetNodeID& Id_;
    TCGBinding& Binding_;
    TCGVariables& Variables_;

};

void TFoldingProfiler::Profile(const TConstQueryPtr& query)
{
    Id_.AddInteger(EFoldingObjectType::ScanOp);
    Profile(query->TableSchema);
    
    if (query->Predicate) {
        Id_.AddInteger(EFoldingObjectType::FilterOp);
        Profile(query->Predicate);
    }
     
    if (query->GroupClause) {
        Id_.AddInteger(EFoldingObjectType::GroupOp);

        for (const auto& groupItem : query->GroupClause->GroupItems) {
            Profile(groupItem);
        }

        for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
            Profile(aggregateItem);
        }
    }


    if (query->ProjectClause) {
        Id_.AddInteger(EFoldingObjectType::ProjectOp);

        for (const auto& projection : query->ProjectClause->Projections) {
            Profile(projection);
        }
    }
    
}

void TFoldingProfiler::Profile(const TConstExpressionPtr& expr)
{
    Id_.AddInteger(expr->Type);
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Id_.AddInteger(EFoldingObjectType::LiteralExpr);
        Id_.AddInteger(TValue(literalExpr->Value).Type);

        int index = Variables_.ConstantsRowBuilder.AddValue(TValue(literalExpr->Value));
        Binding_.NodeToConstantIndex[literalExpr] = index;
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Id_.AddInteger(EFoldingObjectType::ReferenceExpr);
        Id_.AddString(referenceExpr->ColumnName.c_str());
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Id_.AddInteger(EFoldingObjectType::FunctionExpr);
        Id_.AddString(functionExpr->FunctionName.c_str());

        for (const auto& argument : functionExpr->Arguments) {
            Profile(argument);
        }
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        Id_.AddInteger(EFoldingObjectType::BinaryOpExpr);
        Id_.AddInteger(binaryOp->Opcode);

        Profile(binaryOp->Lhs);
        Profile(binaryOp->Rhs);
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Id_.AddInteger(EFoldingObjectType::InOpExpr);

        for (const auto& argument : inOp->Arguments) {
            Profile(argument);
        }

        int index = Variables_.LiteralRows.size();
        Variables_.LiteralRows.push_back(inOp->Values);
        Binding_.NodeToRows[expr.Get()] = index;

    } else {
        YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema)
{
    Id_.AddInteger(EFoldingObjectType::TableSchema);
}

void TFoldingProfiler::Profile(const TNamedItem& namedExpression)
{
    Id_.AddInteger(EFoldingObjectType::NamedExpression);
    Id_.AddString(namedExpression.Name.c_str());

    Profile(namedExpression.Expression);
}

void TFoldingProfiler::Profile(const TAggregateItem& aggregateItem)
{
    Id_.AddInteger(EFoldingObjectType::AggregateItem);
    Id_.AddInteger(aggregateItem.AggregateFunction);
    Id_.AddString(aggregateItem.Name.c_str());

    Profile(aggregateItem.Expression);
}

struct TFoldingHasher
{
    size_t operator ()(const llvm::FoldingSetNodeID& id) const
    {
        return id.ComputeHash();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCachedCGQuery
    : public TSyncCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGQuery,
        TFoldingHasher>
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
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQuery, TFoldingHasher>
{
public:
    explicit TImpl(TExecutorConfigPtr config)
        : TSyncSlruCacheBase(config->CGCache)
    {
        Compiler_ = CreateFragmentCompiler();

        CallCGQueryPtr_ = &CallCGQuery;
    }

    TQueryStatistics Run(
        const TConstQueryPtr& query,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer)
    {
        TRACE_CHILD("QueryClient", "Evaluate") {
            TRACE_ANNOTATION("fragment_id", query->GetId());

            auto Logger = BuildLogger(query);

            TQueryStatistics statistics;
            TDuration wallTime;

            try {
                NProfiling::TAggregatingTimingGuard timingGuard(&wallTime);

                TCGVariables fragmentParams;
                auto cgQuery = Codegen(query, fragmentParams);

                LOG_DEBUG("Evaluating plan fragment");

                LOG_DEBUG("Opening writer");
                {
                    NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                    auto error = WaitFor(writer->Open(
                        query->GetTableSchema(),
                        query->GetKeyColumns()));
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                }

                LOG_DEBUG("Writer opened");

                TRowBuffer rowBuffer;

                struct TScratchMemoryPoolTag { };
                TChunkedMemoryPool scratchSpace { TScratchMemoryPoolTag() };

                std::vector<TRow> batch;
                batch.reserve(MaxRowsPerWrite);

                TExecutionContext executionContext;
                executionContext.Reader = reader.Get();
                executionContext.Schema = query->TableSchema;

                executionContext.DataSplitsArray = &fragmentParams.DataSplitsArray;
                executionContext.LiteralRows = &fragmentParams.LiteralRows;
                executionContext.RowBuffer = &rowBuffer;
                executionContext.ScratchSpace = &scratchSpace;
                executionContext.Writer = writer.Get();
                executionContext.Batch = &batch;
                executionContext.Statistics = &statistics;
                executionContext.InputRowLimit = query->GetInputRowLimit();
                executionContext.OutputRowLimit = query->GetOutputRowLimit();

                CallCGQueryPtr_(cgQuery, fragmentParams.ConstantsRowBuilder.GetRow(), &executionContext);

                LOG_DEBUG("Flushing writer");
                if (!batch.empty()) {
                    if (!writer->Write(batch)) {
                        NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                        auto error = WaitFor(writer->GetReadyEvent());
                        THROW_ERROR_EXCEPTION_IF_FAILED(error);
                    }
                }

                LOG_DEBUG("Closing writer");
                {
                    NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                    auto error = WaitFor(writer->Close());
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                }

                LOG_DEBUG("Finished evaluating plan fragment (RowBufferCapacity: %v, ScratchSpaceCapacity: %v)",
                    rowBuffer.GetCapacity(),
                    scratchSpace.GetCapacity());

            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to evaluate plan fragment") << ex;
            }

            statistics.SyncTime = wallTime - statistics.AsyncTime;

            TRACE_ANNOTATION("rows_read", statistics.RowsRead);
            TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
            TRACE_ANNOTATION("sync_time", statistics.SyncTime);
            TRACE_ANNOTATION("async_time", statistics.AsyncTime);
            TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
            TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

            return statistics;
        }        
    }

private:
    TCGQueryCallback Codegen(const TConstQueryPtr& query, TCGVariables& variables)
    {
        llvm::FoldingSetNodeID id;
        TCGBinding binding;

        TFoldingProfiler(id, binding, variables).Profile(query);
        auto Logger = BuildLogger(query);

        auto cgQuery = Find(id);
        if (!cgQuery) {
            LOG_DEBUG("Codegen cache miss");
            try {
                TRACE_CHILD("QueryClient", "Compile") {
                    LOG_DEBUG("Started compiling fragment");
                    cgQuery = New<TCachedCGQuery>(id, Compiler_(query, binding));
                    LOG_DEBUG("Finished compiling fragment");
                    TryInsert(cgQuery, &cgQuery);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to compile a fragment")
                    << ex;
            }
        } else {
            LOG_DEBUG("Codegen cache hit");
        }

        return cgQuery->GetQueryCallback();
    }

    static void CallCGQuery(
        const TCGQueryCallback& cgQuery,
        TRow constants,
        TExecutionContext* executionContext)
    {
#ifdef DEBUG
        int dummy;
        executionContext->StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif
        cgQuery.Run(constants, executionContext);
    }

    void(* volatile CallCGQueryPtr_)(
        const TCGQueryCallback& cgQuery,
        TRow constants,
        TExecutionContext* executionContext);

private:
    TCGFragmentCompiler Compiler_;
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

TQueryStatistics TEvaluator::Run(
    const TConstQueryPtr& query,
    ISchemafulReaderPtr reader,
    ISchemafulWriterPtr writer)
{
#ifdef YT_USE_LLVM
    return Impl_->Run(query, std::move(reader), std::move(writer));
#else
    THROW_ERROR_EXCEPTION("Query evaluation is not supported in this build");
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
