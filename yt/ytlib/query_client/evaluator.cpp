#include "stdafx.h"
#include "evaluator.h"

#include "cg_fragment.h"
#include "cg_fragment_compiler.h"
#include "cg_routines.h"

#include "plan_fragment.h"
#include "plan_node.h"

#include "helpers.h"
#include "private.h"

#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <core/concurrency/fiber.h>

#include <core/misc/cache.h>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

static auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

void InitializeLlvmImpl()
{
    llvm::llvm_start_multithreaded();
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

    (IntegerLiteralExpr)
    (DoubleLiteralExpr)
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

    void Profile(const TOperator* op);
    void Profile(const TExpression* expr);
    void Profile(const TNamedExpression& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

private:
    llvm::FoldingSetNodeID& Id_;
    TCGBinding& Binding_;
    TCGVariables& Variables_;

};

void TFoldingProfiler::Profile(const TOperator* op)
{
    switch (op->GetKind()) {

        case EOperatorKind::Scan: {
            const auto* scanOp = op->As<TScanOperator>();
            Id_.AddInteger(EFoldingObjectType::ScanOp);

            auto tableSchema = scanOp->GetTableSchema();
            
            Profile(tableSchema);
            
            auto dataSplits = scanOp->DataSplits();
            for (auto & dataSplit : dataSplits) {
                SetTableSchema(&dataSplit, tableSchema);
            }

            int index = Variables_.DataSplitsArray.size();
            Variables_.DataSplitsArray.push_back(dataSplits);
            Binding_.ScanOpToDataSplits[scanOp] = index;

            break;
        }

        case EOperatorKind::Filter: {
            const auto* filterOp = op->As<TFilterOperator>();
            Id_.AddInteger(EFoldingObjectType::FilterOp);

            Profile(filterOp->GetPredicate());
            Profile(filterOp->GetSource());

            break;
        }

        case EOperatorKind::Project: {
            const auto* projectOp = op->As<TProjectOperator>();
            Id_.AddInteger(EFoldingObjectType::ProjectOp);

            for (const auto& projection : projectOp->Projections()) {
                Profile(projection);
            }

            Profile(projectOp->GetSource());

            break;
        }

        case EOperatorKind::Group: {
            const auto* groupOp = op->As<TGroupOperator>();
            Id_.AddInteger(EFoldingObjectType::GroupOp);

            for (const auto& groupItem : groupOp->GroupItems()) {
                Profile(groupItem);
            }

            for (const auto& aggregateItem : groupOp->AggregateItems()) {
                Profile(aggregateItem);
            }

            Profile(groupOp->GetSource());

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TExpression* expr)
{
    using NVersionedTableClient::MakeIntegerValue;
    using NVersionedTableClient::MakeDoubleValue;

    switch (expr->GetKind()) {

        case EExpressionKind::IntegerLiteral: {
            const auto* integerLiteralExpr = expr->As<TIntegerLiteralExpression>();
            Id_.AddInteger(EFoldingObjectType::IntegerLiteralExpr);

            int index = Variables_.ConstantArray.size();
            Variables_.ConstantArray.push_back(MakeIntegerValue<TValue>(integerLiteralExpr->GetValue()));
            Binding_.NodeToConstantIndex[expr] = index;

            break;
        }

        case EExpressionKind::DoubleLiteral: {
            const auto* doubleLiteralExpr = expr->As<TDoubleLiteralExpression>();
            Id_.AddInteger(EFoldingObjectType::DoubleLiteralExpr);

            int index = Variables_.ConstantArray.size();
            Variables_.ConstantArray.push_back(MakeIntegerValue<TValue>(doubleLiteralExpr->GetValue()));
            Binding_.NodeToConstantIndex[expr] = index;

            break;
        }

        case EExpressionKind::Reference: {
            const auto* referenceExpr = expr->As<TReferenceExpression>();
            Id_.AddInteger(EFoldingObjectType::ReferenceExpr);
            Id_.AddString(referenceExpr->GetColumnName().c_str());

            break;
        }

        case EExpressionKind::Function: {
            const auto* functionExpr = expr->As<TFunctionExpression>();
            Id_.AddInteger(EFoldingObjectType::FunctionExpr);
            Id_.AddString(functionExpr->GetFunctionName().c_str());

            for (const auto& argument : functionExpr->Arguments()) {
                Profile(argument);
            }

            break;
        }

        case EExpressionKind::BinaryOp: {
            const auto* binaryOp = expr->As<TBinaryOpExpression>();
            Id_.AddInteger(EFoldingObjectType::BinaryOpExpr);
            Id_.AddInteger(binaryOp->GetOpcode());

            Profile(binaryOp->GetLhs());
            Profile(binaryOp->GetRhs());

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema)
{
    Id_.AddInteger(EFoldingObjectType::TableSchema);
}

void TFoldingProfiler::Profile(const TNamedExpression& namedExpression)
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
    : public TCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGFragment,
        TFoldingHasher>
    , public TCGFragment
{
public:
    TCachedCGFragment(const llvm::FoldingSetNodeID& id)
        : TCacheValueBase(id)
        , TCGFragment()
    { }

};

class TEvaluator::TImpl
    : public TSizeLimitedCache<
        llvm::FoldingSetNodeID,
        TCachedCGFragment,
        TFoldingHasher>
{
public:
    TImpl(const int maxCacheSize)
        : TSizeLimitedCache(maxCacheSize)
    {
        InitializeLlvm();
        RegisterCGRoutines();

        Compiler_ = CreateFragmentCompiler();

        CallCodegenedFunctionPtr_ = &CallCodegenedFunction;
    }

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemafulWriterPtr writer)
    {
        auto result = Codegen(fragment);

        auto codegenedFunction = result.first;
        auto fragmentParams = result.second;

        // Make TRow from fragmentParams.ConstantArray.
        TChunkedMemoryPool memoryPool;
        auto constants = TRow::Allocate(&memoryPool, fragmentParams.ConstantArray.size());
        for (int i = 0; i < fragmentParams.ConstantArray.size(); ++i) {
            constants[i] = fragmentParams.ConstantArray[i];
        }

        try {
            LOG_DEBUG("Evaluating plan fragment (FragmentId: %s)",
                ~ToString(fragment.Id()));

            LOG_DEBUG("Opening writer (FragmentId: %s)",
                ~ToString(fragment.Id()));
            {
                auto error = WaitFor(writer->Open(
                    fragment.GetHead()->GetTableSchema(),
                    fragment.GetHead()->GetKeyColumns()));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            LOG_DEBUG("Writer opened (FragmentId: %s)",
                ~ToString(fragment.Id()));

            TRowBuffer rowBuffer;
            TChunkedMemoryPool scratchSpace;
            std::vector<TRow> batch;
            batch.reserve(MaxRowsPerWrite);

            TPassedFragmentParams passedFragmentParams;
            passedFragmentParams.Callbacks = callbacks;
            passedFragmentParams.Context = fragment.GetContext().Get();
            passedFragmentParams.DataSplitsArray = &fragmentParams.DataSplitsArray;
            passedFragmentParams.RowBuffer = &rowBuffer;
            passedFragmentParams.ScratchSpace = &scratchSpace;
            passedFragmentParams.Writer = writer.Get();
            passedFragmentParams.Batch = &batch;

            CallCodegenedFunctionPtr_(codegenedFunction, constants, &passedFragmentParams);

            LOG_DEBUG("Flushing writer (FragmentId: %s)",
                ~ToString(fragment.Id()));

            if (!batch.empty()) {
                if (!writer->Write(batch)) {
                    auto error = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                }
            }

            LOG_DEBUG("Closing writer (FragmentId: %s)",
                ~ToString(fragment.Id()));
            {
                auto error = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            LOG_DEBUG("Finished evaluating plan fragment (FragmentId: %s, RowBufferCapacity: %" PRISZT ", ScratchSpaceCapacity: %" PRISZT ")",
                ~ToString(fragment.Id()),
                rowBuffer.GetCapacity(),
                scratchSpace.GetCapacity());
        } catch (const std::exception& ex) {
            auto error = TError("Failed to evaluate plan fragment") << ex;
            LOG_ERROR(error);
            return error;
        }

        return TError();
    }

private:
    std::pair<TCodegenedFunction, TCGVariables> Codegen(const TPlanFragment& fragment)
    {
        llvm::FoldingSetNodeID id;
        TCGBinding binding;
        TCGVariables variables;

        TFoldingProfiler(id, binding, variables).Profile(fragment.GetHead());

        TInsertCookie cookie(id);
        if (BeginInsert(&cookie)) {
            LOG_DEBUG("Cache miss for fragment %s", ~ToString(fragment.Id()));
            try {
                LOG_DEBUG("Compiling fragment %s", ~ToString(fragment.Id()));
                auto newCGFragment = New<TCachedCGFragment>(id);
                newCGFragment->Embody(Compiler_(fragment, *newCGFragment, binding));
                newCGFragment->GetCompiledBody();
                cookie.EndInsert(std::move(newCGFragment));
            } catch (const std::exception& ex) {
                LOG_DEBUG("Failed to compile fragment %s", ~ToString(fragment.Id()));
                cookie.Cancel(ex);
            }
        } else {
            LOG_DEBUG("Cache hit for fragment %s", ~ToString(fragment.Id()));
        }

        auto cgFragment = cookie.GetValue().Get().ValueOrThrow();
        auto codegenedFunction = cgFragment->GetCompiledBody();

        YCHECK(codegenedFunction);

        return std::make_pair(codegenedFunction, std::move(variables));
    }

    static void CallCodegenedFunction(TCodegenedFunction codegenedFunction, TRow constants, TPassedFragmentParams* passedFragmentParams)
    {
#ifdef DEBUG
        int dummy;
        passedFragmentParams->StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif
        codegenedFunction(constants, passedFragmentParams);
    }

    void(* volatile CallCodegenedFunctionPtr_)(TCodegenedFunction codegenedFunction, TRow constants, TPassedFragmentParams* passedFragmentParams);

private:
    TCGFragmentCompiler Compiler_;

};

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator()
    : Impl_(New<TEvaluator::TImpl>(100))
{ }

TEvaluator::~TEvaluator()
{ }

TError TEvaluator::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemafulWriterPtr writer)
{
    return Impl_->Run(callbacks, fragment, std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
