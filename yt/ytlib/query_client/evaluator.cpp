#include "stdafx.h"
#include "evaluator.h"

#include "helpers.h"
#include "private.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "cg_fragment.h"
#include "cg_fragment_compiler.h"
#include "cg_routines.h"
#include "config.h"

#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/query_client/plan_fragment.pb.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/sync_cache.h>

#include <core/logging/log.h>

#include <core/tracing/trace_context.h>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void InitializeLlvmImpl()
{
    YCHECK(llvm::llvm_is_multithreaded());
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();
}

void InitializeLlvm()
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &InitializeLlvmImpl);
}

////////////////////////////////////////////////////////////////////////////////
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

    void Profile(const TConstOperatorPtr& op);
    void Profile(const TConstExpressionPtr& expr);
    void Profile(const TNamedItem& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

private:
    llvm::FoldingSetNodeID& Id_;
    TCGBinding& Binding_;
    TCGVariables& Variables_;

};

void TFoldingProfiler::Profile(const TConstOperatorPtr& op)
{
    if (auto scanOp = op->As<TScanOperator>()) {
        Id_.AddInteger(EFoldingObjectType::ScanOp);

        auto tableSchema = scanOp->GetTableSchema();
            
        Profile(tableSchema);
        
        // TODO(lukyan): Remove this code
        auto dataSplits = scanOp->DataSplits;
        for (auto & dataSplit : dataSplits) {
            SetTableSchema(&dataSplit, tableSchema);
        }

        int index = Variables_.DataSplitsArray.size();
        Variables_.DataSplitsArray.push_back(dataSplits);
        Binding_.ScanOpToDataSplits[scanOp] = index;
    } else if (auto filterOp = op->As<TFilterOperator>()) {
        Id_.AddInteger(EFoldingObjectType::FilterOp);

        Profile(filterOp->Predicate);
        Profile(filterOp->Source);
    } else if (auto projectOp = op->As<TProjectOperator>()) {
        Id_.AddInteger(EFoldingObjectType::ProjectOp);

        for (const auto& projection : projectOp->Projections) {
            Profile(projection);
        }

        Profile(projectOp->Source);
    } else if (auto groupOp = op->As<TGroupOperator>()) {
        Id_.AddInteger(EFoldingObjectType::GroupOp);

        for (const auto& groupItem : groupOp->GroupItems) {
            Profile(groupItem);
        }

        for (const auto& aggregateItem : groupOp->AggregateItems) {
            Profile(aggregateItem);
        }

        Profile(groupOp->Source);
    } else {
        YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TConstExpressionPtr& expr)
{
    Id_.AddInteger(expr->Type);
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Id_.AddInteger(EFoldingObjectType::LiteralExpr);
        Id_.AddInteger(literalExpr->Index);
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

class TCachedCGFragment
    : public TSyncCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGFragment,
        TFoldingHasher>
    , public TCGFragment
{
public:
    explicit TCachedCGFragment(const llvm::FoldingSetNodeID& id)
        : TSyncCacheValueBase(id)
        , TCGFragment()
    { }

};

class TEvaluator::TImpl
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGFragment, TFoldingHasher>
{
public:
    explicit TImpl(TExecutorConfigPtr config)
        : TSyncSlruCacheBase(config->CGCache)
    {
        InitializeLlvm();
        RegisterCGRoutines();

        Compiler_ = CreateFragmentCompiler();

        CallCgFunctionPtr_ = &CallCgFunction;
    }

    TQueryStatistics Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        TRACE_CHILD("QueryClient", "Evaluate") {
            TRACE_ANNOTATION("fragment_id", fragment->GetId());

            auto Logger = BuildLogger(fragment);

            TQueryStatistics statistics;
            TDuration wallTime;

            try {
                NProfiling::TAggregatingTimingGuard timingGuard(&wallTime);

                TCGFunction cgFunction;
                TCGVariables fragmentParams;

                std::tie(cgFunction, fragmentParams) = Codegen(fragment);

                auto constants = fragment->Literals.Get();

                LOG_DEBUG("Evaluating plan fragment");

                LOG_DEBUG("Opening writer");
                {
                    NProfiling::TAggregatingTimingGuard timingGuard(&statistics.AsyncTime);
                    auto error = WaitFor(writer->Open(
                        fragment->Head->GetTableSchema(),
                        fragment->Head->GetKeyColumns()));
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                }

                LOG_DEBUG("Writer opened");

                TRowBuffer rowBuffer;
                TChunkedMemoryPool scratchSpace;
                std::vector<TRow> batch;
                batch.reserve(MaxRowsPerWrite);

                TExecutionContext executionContext;
                executionContext.Callbacks = callbacks;
                executionContext.NodeDirectory = fragment->NodeDirectory;
                executionContext.DataSplitsArray = &fragmentParams.DataSplitsArray;
                executionContext.RowBuffer = &rowBuffer;
                executionContext.ScratchSpace = &scratchSpace;
                executionContext.Writer = writer.Get();
                executionContext.Batch = &batch;
                executionContext.Statistics = &statistics;
                executionContext.InputRowLimit = fragment->GetInputRowLimit();
                executionContext.OutputRowLimit = fragment->GetOutputRowLimit();

                CallCgFunctionPtr_(cgFunction, constants, &executionContext);

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

                LOG_DEBUG("Finished evaluating plan fragment (RowBufferCapacity: %" PRISZT ", ScratchSpaceCapacity: %" PRISZT ")",
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
    std::pair<TCGFunction, TCGVariables> Codegen(const TPlanFragmentPtr& fragment)
    {
        llvm::FoldingSetNodeID id;
        TCGBinding binding;
        TCGVariables variables;

        TFoldingProfiler(id, binding, variables).Profile(fragment->Head);
        auto Logger = BuildLogger(fragment);

        auto cgFragment = Find(id);
        if (!cgFragment) {
            LOG_DEBUG("Codegen cache miss");
            try {
                TRACE_CHILD("QueryClient", "Compile") {
                    LOG_DEBUG("Started compiling fragment");
                    cgFragment = New<TCachedCGFragment>(id);
                    cgFragment->Embody(Compiler_(fragment, *cgFragment, binding));
                    cgFragment->GetCompiledBody();
                    LOG_DEBUG("Finished compiling fragment");
                    TryInsert(cgFragment);
                }
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to compile a fragment");
                throw;
            }
        } else {
            LOG_DEBUG("Codegen cache hit");
        }

        auto cgFunction = cgFragment->GetCompiledBody();
        YCHECK(cgFunction);

        return std::make_pair(cgFunction, std::move(variables));
    }

    static void CallCgFunction(
        TCGFunction cgFunction,
        TRow constants,
        TExecutionContext* executionContext)
    {
#ifdef DEBUG
        int dummy;
        executionContext->StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif
        cgFunction(constants, executionContext);
    }

    void(* volatile CallCgFunctionPtr_)(
        TCGFunction cgFunction,
        TRow constants,
        TExecutionContext* executionContext);

private:
    TCGFragmentCompiler Compiler_;

    virtual i64 GetWeight(TCachedCGFragment* /*fragment*/) const override
    {
        return 1;
    }

    static NLog::TLogger BuildLogger(const TPlanFragmentPtr& fragment)
    {
        NLog::TLogger result(QueryClientLogger);
        result.AddTag("FragmentId: %v", fragment->GetId());
        return result;
    }

};

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator(TExecutorConfigPtr config)
    : Impl_(New<TImpl>(std::move(config)))
{ }

TEvaluator::~TEvaluator()
{ }

TQueryStatistics TEvaluator::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragmentPtr& fragment,
    ISchemafulWriterPtr writer)
{
    return Impl_->Run(callbacks, fragment, std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
