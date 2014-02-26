#include "codegen_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>

#include <core/misc/protobuf_helpers.h>
#include <core/misc/chunked_memory_pool.h>

#include <core/logging/tagged_logger.h>

#include <mutex>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/TypeBuilder.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IR/IRPrintingPasses.h>

#include <llvm/Transforms/Scalar.h>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>

#include <llvm/Analysis/Passes.h>

#include <llvm/PassManager.h>

#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/TargetSelect.h>

// TODO(sandello):
//  - Remove TTaggedLogger from a stateless evaluator
//  - Cleanup TFragmentParams, TPassedFragmentParams, TCGImmediates & TCGContext
//    and their usages
//  - Implement basic logging & profiling within evaluation code
//
////////////////////////////////////////////////////////////////////////////////

// Evaluate
namespace NYT {
namespace NQueryClient {

template <class T>
size_t THashCombine(size_t seed, const T& value)
{
    std::hash<T> hasher;
    return seed ^ (hasher(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    // TODO(lukyan): Fix this function
}

class TGroupHasher
{
public:
    explicit TGroupHasher(int keySize)
        : KeySize_(keySize)
    { }

    size_t operator() (TRow key) const
    {
        size_t result = 0;
        for (int i = 0; i < KeySize_; ++i) {
            YCHECK(key[i].Type == EValueType::Integer);
            result = THashCombine(result, key[i].Data.Integer);
        }
        return result;
    }

private:
    int KeySize_;

};

class TGroupComparer
{
public:
    explicit TGroupComparer(int keySize)
        : KeySize_(keySize)
    { }

    bool operator() (TRow lhs, TRow rhs) const
    {
        for (int i = 0; i < KeySize_; ++i) {
            YCHECK(lhs[i].Type == EValueType::Integer);
            YCHECK(rhs[i].Type == EValueType::Integer);
            if (lhs[i].Data.Integer != rhs[i].Data.Integer) {
                return false;
            }
        }
        return true;
    }

private:
    int KeySize_;

};

typedef std::unordered_set<TRow, TGroupHasher, TGroupComparer> TGroupedRows;

struct TPassedFragmentParams
{
    IEvaluateCallbacks* Callbacks;
    TPlanContext* Context;
    std::vector<TDataSplits>* DataSplitsArray;
};

static const int MaxRowsPerRead = 512;
static const int MaxRowsPerWrite = 512;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NQueryClient;
using namespace NVersionedTableClient;

// Opaque types

template <bool cross>
class TypeBuilder<ISchemedWriter*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<TChunkedMemoryPool*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<std::vector<TRow>*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<std::pair<std::vector<TRow>, TChunkedMemoryPool*>*, cross>
    : public TypeBuilder<void*, cross>
{ };
template <bool cross>
class TypeBuilder<TGroupedRows*, cross>
    : public TypeBuilder<void*, cross>
{ };

// Aggregate types

template <bool cross>
class TypeBuilder<TValueData, cross>
{
public:
    static Type* get(LLVMContext& context)
    {
        enum
        {
            UnionSize0 = sizeof(i64),
            UnionSize1 = UnionSize0 > sizeof(double) ? UnionSize0 : sizeof(double),
            UnionSize2 = UnionSize1 > sizeof(const char*) ? UnionSize1 : sizeof(const char*)
        };

        static_assert(UnionSize2 == sizeof(i64), "Unexpected union size.");

        return TypeBuilder<i64, cross>::get(context);
    }

    enum Fields
    {
        Integer,
        Double,
        String
    };

    static Type* getAs(Fields dataFields, LLVMContext& context)
    {
        switch (dataFields) {
            case Fields::Integer:
                return TypeBuilder<i64*, cross>::get(context);
            case Fields::Double:
                return TypeBuilder<double*, cross>::get(context);
            case Fields::String:
                return TypeBuilder<const char**, cross>::get(context);
        }
        YUNREACHABLE();
    }
};

template <bool cross>
class TypeBuilder<TValue, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<ui16, cross>::get(context),
            TypeBuilder<ui16, cross>::get(context),
            TypeBuilder<ui32, cross>::get(context),
            TypeBuilder<TValueData, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Id,
        Type,
        Length,
        Data
    };
};

template <bool cross>
class TypeBuilder<TRowHeader, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<ui32, cross>::get(context),
            TypeBuilder<ui32, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Count,
        Padding
    };
};

template <bool cross>
class TypeBuilder<TRow, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<TRowHeader*, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Header
    };
};

template <bool cross>
class TypeBuilder<TPassedFragmentParams, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<void*, cross>::get(context),
            TypeBuilder<void*, cross>::get(context),
            TypeBuilder<void*, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Callbacks,
        Context,
        DataSplitsArray
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NVersionedTableClient;
using namespace NConcurrency;

// Import extensively used LLVM types.
using llvm::BasicBlock;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::Function;
using llvm::FunctionType;
using llvm::Instruction;
using llvm::Type;
using llvm::TypeBuilder;
using llvm::Value;

template <bool PreserveNames = true>
class TContextPreservingInserter
{
protected:
    mutable std::unordered_set<Value*> ValuesInContext_;

    void InsertHelper(
        Instruction* instruction,
        const llvm::Twine& name,
        BasicBlock* basicBlock,
        BasicBlock::iterator insertPoint) const
    {
        ValuesInContext_.insert(static_cast<Value*>(instruction));

        if (basicBlock) {
            basicBlock->getInstList().insert(insertPoint, instruction);
        }

        if (PreserveNames) {
            instruction->setName(name);
        }
    }

};

class TContextIRBuilder
    : public llvm::IRBuilder<true, llvm::ConstantFolder, TContextPreservingInserter<true> >
{
private:
    typedef llvm::IRBuilder<true, llvm::ConstantFolder, TContextPreservingInserter<true> > Base;

    //! Builder associated with the parent context.
    TContextIRBuilder* Parent_;

    //! Pointer to the closure.
    //! Note that this value belongs to the current context.
    Value* ClosurePtr_;

    //! Closure itself.
    //! Note that this value belongs to the parent context.
    Value* Closure_;

    //! Translates captured values in the parent context into their index in the closure.
    yhash_map<Value*, int> Mapping_;

    static const unsigned int MaxClosureSize = 32;

public:
    TContextIRBuilder(llvm::BasicBlock* basicBlock, TContextIRBuilder* parent, Value* closurePtr)
        : Base(basicBlock)
        , Parent_(parent)
        , ClosurePtr_(closurePtr)
    {
        Function* function = basicBlock->getParent();
        for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
            ValuesInContext_.insert(it);
        }

        Closure_ = Parent_->CreateAlloca(
            TypeBuilder<void*, false>::get(basicBlock->getContext()),
            ConstantInt::get(
                Type::getInt32Ty(basicBlock->getContext()),
                MaxClosureSize),
            "closure");
    }

    TContextIRBuilder(llvm::BasicBlock* basicBlock)
        : Base(basicBlock)
        , Parent_(nullptr)
        , ClosurePtr_(nullptr)
        , Closure_(nullptr)
    {
        Function* function = basicBlock->getParent();
        for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
            ValuesInContext_.insert(it);
        }
    }

    Value* ViaClosure(Value* value, llvm::Twine name = llvm::Twine())
    {
        // If |value| belongs to the current context, then we can use it directly.
        if (this->ValuesInContext_.count(value) > 0) {
            return value;
        }

        if (name.isTriviallyEmpty()) {
            name = value->getName();
        }

        // Otherwise, capture |value| in the parent context.
        YCHECK(Parent_);
        YCHECK(ClosurePtr_);

        Value* valueInParent = Parent_->ViaClosure(value, name);

        // Check if we have already captured this value.
        auto it = Mapping_.find(valueInParent);
        int indexInClosure;

        if (it != Mapping_.end()) {
            // If yes, use the captured version.
            indexInClosure = it->second;
        } else {
            // If no, save the value into the closure in the parent context.
            indexInClosure = Mapping_.size();

            YCHECK(indexInClosure < MaxClosureSize);
            Mapping_[valueInParent] = indexInClosure;

            Value* valueInParentPtr = Parent_->CreateAlloca(
                valueInParent->getType(),
                nullptr,
                name + "InParentPtr");

            Parent_->CreateStore(
                valueInParent,
                valueInParentPtr);
            Parent_->CreateStore(
                valueInParentPtr,
                Parent_->CreatePointerCast(
                    Parent_->CreateConstGEP1_32(Closure_, indexInClosure),
                    valueInParentPtr->getType()->getPointerTo(),
                    name + "ClosureSlotPtr"
                )
            );
        }

        // Load the value to the current context through the closure.
        return
            this->CreateLoad(
                this->CreateLoad(
                    this->CreatePointerCast(
                        this->CreateConstGEP1_32(ClosurePtr_, indexInClosure),
                        value->getType()->getPointerTo()->getPointerTo(),
                        name + "ClosureSlotPtr"
                    ),
                    name + "InParentPtr"
                ),
                name
            );
    }

    Value* GetClosure() const
    {
        return Closure_;
    }

};

typedef TContextIRBuilder TIRBuilder;

DECLARE_ENUM(EFoldingObjectType,
    (TableSchema)

    (ScanOp)
    (UnionOp)
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
);

// TODO(sandello): Better names for these.
struct TCGImmediates
{
    yhash_map<const TExpression*, int> NodeToConstantIndex;
    yhash_map<const TScanOperator*, int> ScanOpToDataSplits;
};

// TODO(sandello): Better names for these.
struct TFragmentParams
    : TCGImmediates
{
    std::vector<TValue> ConstantArray;
    std::vector<TDataSplits> DataSplitsArray;
};
struct TCGContext
{
    const TCGImmediates& Params;

    Value* ConstantsRow;
    Value* PassedFragmentParamsPtr;

    TCGContext(
        const TCGImmediates& params,
        Value* constantsRow,
        Value* passedFragmentParamsPtr)
        : Params(params)
        , ConstantsRow(constantsRow)
        , PassedFragmentParamsPtr(passedFragmentParamsPtr)
    { }

    Value* GetConstantsRows(TIRBuilder& builder) const
    {
        return builder.ViaClosure(
            ConstantsRow,
            "constantsRow");
    }

    Value* GetPassedFragmentParamsPtr(TIRBuilder& builder) const
    {
        return builder.ViaClosure(
            PassedFragmentParamsPtr,
            "passedFragmentParamsPtr");
    }

};

class TFragmentProfileVisitor
{
private:
    llvm::FoldingSetNodeID& Id;
    TFragmentParams& FragmentParams;

public:
    explicit TFragmentProfileVisitor(llvm::FoldingSetNodeID& id, TFragmentParams& fragmentParams)
        : Id(id)
        , FragmentParams(fragmentParams)
    { }

    void Profile(const TTableSchema& tableSchema);
    void Profile(const TExpression* expr);
    void Profile(const TNamedExpression& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TOperator* op);
};

void TFragmentProfileVisitor::Profile(const TTableSchema& tableSchema)
{
    Id.AddInteger(EFoldingObjectType::TableSchema);
}

void TFragmentProfileVisitor::Profile(const TExpression* expr)
{
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral: {
            const auto* integerLiteralExpr = expr->As<TIntegerLiteralExpression>();
            Id.AddInteger(EFoldingObjectType::IntegerLiteralExpr);

            int constantIndex = FragmentParams.ConstantArray.size();
            FragmentParams.ConstantArray.push_back(MakeIntegerValue<TValue>(integerLiteralExpr->GetValue()));
            FragmentParams.NodeToConstantIndex[expr] = constantIndex;

            break;
        }

        case EExpressionKind::DoubleLiteral: {
            const auto* doubleLiteralExpr = expr->As<TDoubleLiteralExpression>();
            Id.AddInteger(EFoldingObjectType::DoubleLiteralExpr);

            int constantIndex = FragmentParams.ConstantArray.size();
            FragmentParams.ConstantArray.push_back(MakeIntegerValue<TValue>(doubleLiteralExpr->GetValue()));
            FragmentParams.NodeToConstantIndex[expr] = constantIndex;

            break;
        }

        case EExpressionKind::Reference: {
            const auto* referenceExpr = expr->As<TReferenceExpression>();
            Id.AddInteger(EFoldingObjectType::ReferenceExpr);
            Id.AddString(~referenceExpr->GetColumnName());

            break;
        }

        case EExpressionKind::Function: {
            const auto* functionExpr = expr->As<TFunctionExpression>();
            Id.AddInteger(EFoldingObjectType::FunctionExpr);
            Id.AddString(~functionExpr->GetFunctionName());

            for (const auto& argument : functionExpr->Arguments()) {
                Profile(argument);
            }

            break;
        }

        case EExpressionKind::BinaryOp: {
            const auto* binaryOp = expr->As<TBinaryOpExpression>();
            Id.AddInteger(EFoldingObjectType::BinaryOpExpr);
            Id.AddInteger(binaryOp->GetOpcode());

            Profile(binaryOp->GetLhs());
            Profile(binaryOp->GetRhs());

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TFragmentProfileVisitor::Profile(const TNamedExpression& namedExpression)
{
    Id.AddInteger(EFoldingObjectType::NamedExpression);
    Id.AddString(~namedExpression.Name);

    Profile(namedExpression.Expression);
}

void TFragmentProfileVisitor::Profile(const TAggregateItem& aggregateItem)
{
    Id.AddInteger(EFoldingObjectType::AggregateItem);
    Id.AddInteger(aggregateItem.AggregateFunction);
    Id.AddString(~aggregateItem.Name);

    Profile(aggregateItem.Expression);
}

void TFragmentProfileVisitor::Profile(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan: {
            const auto* scanOp = op->As<TScanOperator>();
            Id.AddInteger(EFoldingObjectType::ScanOp);

            Profile(scanOp->GetTableSchema());

            int dataSplitsIndex = FragmentParams.DataSplitsArray.size();
            FragmentParams.DataSplitsArray.push_back(scanOp->DataSplits());
            FragmentParams.ScanOpToDataSplits[scanOp] = dataSplitsIndex;

            break;
        }

        case EOperatorKind::Union: {
            const auto* unionOp = op->As<TUnionOperator>();
            Id.AddInteger(EFoldingObjectType::UnionOp);

            for (const auto* source : unionOp->Sources()) {
                Profile(source);
            }

            break;
        }

        case EOperatorKind::Filter: {
            const auto* filterOp = op->As<TFilterOperator>();
            Id.AddInteger(EFoldingObjectType::FilterOp);

            Profile(filterOp->GetPredicate());
            Profile(filterOp->GetSource());

            break;
        }

        case EOperatorKind::Project: {
            const auto* projectOp = op->As<TProjectOperator>();
            Id.AddInteger(EFoldingObjectType::ProjectOp);

            for (const auto& projection : projectOp->Projections()) {
                Profile(projection);
            }

            Profile(projectOp->GetSource());

            break;
        }

        case EOperatorKind::Group: {
            const auto* groupOp = op->As<TGroupOperator>();
            Id.AddInteger(EFoldingObjectType::GroupOp);

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

typedef void (*TCodegenedFunction)(
    TRow constants,
    TPassedFragmentParams* passedFragmentParams,
    std::pair<std::vector<TRow>, TChunkedMemoryPool*>* batch,
    ISchemedWriter* writer);

class TCodegenedFragment
    : public llvm::FastFoldingSetNode
{
public:
    TCodegenedFragment(const llvm::FoldingSetNodeID& id, TCodegenedFunction codegenedFunction)
        : llvm::FastFoldingSetNode(id)
        , CodegenedFunction_(codegenedFunction)
    { }

    TCodegenedFunction CodegenedFunction_;
};

typedef std::function<void(TIRBuilder& builder, Value* row)> TCodegenConsumer;

class TCodegenControllerImpl
    : public NNonCopyable::TNonCopyable
{
public:
    // Common
    explicit TCodegenControllerImpl(IInvokerPtr invoker);
    ~TCodegenControllerImpl();

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer);

private:
    // Codegen
    template <class TResult, class... TArgs>
    Function* GetRoutine(TResult(*functionPtr)(TArgs...), const llvm::Twine& name = "")
    {
        void* voidPtr = reinterpret_cast<void*>(functionPtr);

        auto it = CachedRoutines_.find(voidPtr);
        if (it != CachedRoutines_.end()) {
            return it->second;
        }

        Function* function = Function::Create(
            TypeBuilder<TResult(TArgs...), false>::get(Context_),
            Function::ExternalLinkage,
            name,
            Module_);

        ExecutionEngine_->addGlobalMapping(function, voidPtr);

        CachedRoutines_[voidPtr] = function;

        return function;
    }

    TCodegenedFunction CodegenEvaluate(
        const TOperator* op,
        const TCGImmediates& fragmentParams);

    void CodegenOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenScanOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TScanOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenUnionOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TUnionOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenFilterOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TFilterOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenProjectOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TProjectOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenGroupOp(
        TIRBuilder& builder,
        const TCGContext& ctx,
        const TGroupOperator* op,
        const TCodegenConsumer& codegenConsumer);

    TError EvaluateViaCache(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer);

private:
    IInvokerPtr CodegenInvoker_;
    NLog::TTaggedLogger Logger;

    llvm::LLVMContext Context_;
    llvm::Module* Module_;
    llvm::PassManagerBuilder PassManagerBuilder_;
    std::unique_ptr<llvm::ExecutionEngine> ExecutionEngine_;
    std::unique_ptr<llvm::FunctionPassManager> FunctionPassManager_;
    std::unique_ptr<llvm::PassManager> ModulePassManager_;

    std::map<void*, Function*> CachedRoutines_;

    TSpinLock FoldingSetSpinLock_;
    llvm::FoldingSet<TCodegenedFragment> CodegenedFragmentsFoldingSet_;

};

////////////////////////////////////////////////////////////////////////////////

std::once_flag InitializeNativeTargetFlag;

TCodegenControllerImpl::TCodegenControllerImpl(IInvokerPtr invoker)
    : CodegenInvoker_(CreateSerializedInvoker(invoker))
    , Logger(QueryClientLogger)
{
    std::call_once(InitializeNativeTargetFlag, llvm::InitializeNativeTarget);

    Module_ = new llvm::Module("codegen", Context_);

    std::string errorStr;
    // (EngineBuilder owns Module).
    ExecutionEngine_.reset(llvm::EngineBuilder(Module_).setErrorStr(&errorStr).create());

    if (!ExecutionEngine_) {
        delete Module_;
        THROW_ERROR_EXCEPTION("Could not create llvm::ExecutionEngine: %s", errorStr.c_str());
    }

    llvm::PassManagerBuilder passManagerBuilder;
    PassManagerBuilder_.OptLevel = 2;
    PassManagerBuilder_.SizeLevel = 0;
    PassManagerBuilder_.Inliner = llvm::createFunctionInliningPass();

    FunctionPassManager_ = std::make_unique<llvm::FunctionPassManager>(Module_);
    FunctionPassManager_->add(new llvm::DataLayout(*ExecutionEngine_->getDataLayout()));
    PassManagerBuilder_.populateFunctionPassManager(*FunctionPassManager_);
    FunctionPassManager_->doInitialization();

    ModulePassManager_ = std::make_unique<llvm::PassManager>();
    ModulePassManager_->add(new llvm::DataLayout(*ExecutionEngine_->getDataLayout()));
    PassManagerBuilder_.populateModulePassManager(*ModulePassManager_);
}

TCodegenControllerImpl::~TCodegenControllerImpl()
{
    if (FunctionPassManager_) {
        FunctionPassManager_->doFinalization();
        FunctionPassManager_.reset();
    }

    ExecutionEngine_.reset();
}

TError TCodegenControllerImpl::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemedWriterPtr writer)
{
    // TODO(sandello): This won't work. Fix me!
    Logger.AddTag(Sprintf(
        "FragmendId: %s",
        ~ToString(fragment.Id())));

    return EvaluateViaCache(callbacks, fragment, std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

namespace NRoutines {

////////////////////////////////////////////////////////////////////////////////

void WriteRow(TRow row, std::pair<std::vector<TRow>, TChunkedMemoryPool*>* batch, ISchemedWriter* writer)
{
    std::vector<TRow>* batchArray = &batch->first;
    TChunkedMemoryPool* memoryPool = batch->second;

    YASSERT(batchArray->size() < batchArray->capacity());

    TRow rowCopy = TRow::Allocate(memoryPool, row.GetCount());

    for (int i = 0; i < row.GetCount(); ++i) {
        rowCopy[i] = row[i];
    }

    batchArray->push_back(rowCopy);

    if (batchArray->size() == batchArray->capacity()) {
        if (!writer->Write(*batchArray)) {
            auto error = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
        batchArray->clear();
    }
}

void ScanOpHelper(
    TPassedFragmentParams* passedFragmentParams,
    int dataSplitsIndex,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow* rows, int size))
{
    auto callbacks = passedFragmentParams->Callbacks;
    auto context = passedFragmentParams->Context;
    auto dataSplits = (*passedFragmentParams->DataSplitsArray)[dataSplitsIndex];

    for (const auto& dataSplit : dataSplits) {
        auto reader = callbacks->GetReader(dataSplit, context);
        auto schema = GetTableSchemaFromDataSplit(dataSplit);

        {
            auto error = WaitFor(reader->Open(schema));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        std::vector<TRow> rows;
        rows.reserve(MaxRowsPerRead);

        while (true) {
            bool hasMoreData = reader->Read(&rows);
            bool shouldWait = rows.empty();

            consumeRows(consumeRowsClosure, rows.data(), rows.size());
            rows.clear();

            if (!hasMoreData) {
                break;
            }

            if (shouldWait) {
                auto error = WaitFor(reader->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }
        }
    }
}

void ProjectOpHelper(
    int projectionCount,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow row))
{
    // TODO(sandello): Think about allocating the row on the stack.
    TChunkedMemoryPool memoryPool;

    auto newRow = TRow::Allocate(&memoryPool, projectionCount);

    consumeRows(consumeRowsClosure, newRow);
}

void GroupOpHelper(
    int keySize,
    int aggregateItemCount,
    void** consumeRowsClosure,
    void (*consumeRows)(
        void** closure,
        TChunkedMemoryPool* memoryPool,
        std::vector<TRow>* groupedRows,
        TGroupedRows* rows,
        TRow* newRowPtr))
{
    std::vector<TRow> groupedRows;
    TGroupedRows rows(
        256,
        TGroupHasher(keySize),
        TGroupComparer(keySize));

    TChunkedMemoryPool memoryPool;

    auto newRow = TRow::Allocate(&memoryPool, keySize + aggregateItemCount);

    consumeRows(consumeRowsClosure, &memoryPool, &groupedRows, &rows, &newRow);

    memoryPool.Clear();
}

const TRow* FindRow(TGroupedRows* rows, TRow row)
{
    auto it = rows->find(row);
    return it != rows->end()? &*it : nullptr;
}

void AddRow(
    TChunkedMemoryPool* memoryPool,
    TGroupedRows* rows,
    std::vector<TRow>* groupedRows,
    TRow* newRow,
    int rowSize)
{
    groupedRows->push_back(*newRow);
    rows->insert(groupedRows->back());
    *newRow = TRow::Allocate(memoryPool, rowSize);
}

TRow* GetRowsData(std::vector<TRow>* groupedRows)
{
    return groupedRows->data();
}

int GetRowsSize(std::vector<TRow>* groupedRows)
{
    return groupedRows->size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

Value* CodegenExpr(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TExpression* expr,
    const TTableSchema& tableSchema,
    Value* row);

Value* CodegenFunctionExpr(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TFunctionExpression* expr,
    const TTableSchema& tableSchema,
    Value* row)
{
    YUNIMPLEMENTED();
}

Value* CodegenBinaryOpExpr(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TBinaryOpExpression* expr,
    const TTableSchema& tableSchema,
    Value* row)
{
    Value* lhsValue = CodegenExpr(builder, ctx, expr->GetLhs(), tableSchema, row);
    Value* rhsValue = CodegenExpr(builder, ctx, expr->GetRhs(), tableSchema, row);

    auto name = expr->GetName();

    switch (expr->GetOpcode()) {
        // Arithmetical operations.
#define XX(opcode, optype, foptype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    return builder.CreateBinOp(Instruction::BinaryOps::optype, lhsValue, rhsValue, ~expr->GetName()); \
                case EValueType::Double: \
                    return builder.CreateBinOp(Instruction::BinaryOps::foptype, lhsValue, rhsValue, ~expr->GetName()); \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break;
        XX(Plus, Add, FAdd)
        XX(Minus, Sub, FSub)
        XX(Multiply, Mul, FMul)
        XX(Divide, SDiv, FDiv)
#undef XX

        // Integral and logical operations.
#define XX(opcode, optype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    return builder.CreateBinOp(Instruction::BinaryOps::optype, lhsValue, rhsValue, ~expr->GetName()); \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break;
        XX(Modulo, SRem)
        XX(And, And)
        XX(Or, Or)
#undef XX

        // Comparsion operations.
#define XX(opcode, ioptype, foptype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    return builder.CreateICmp##ioptype(lhsValue, rhsValue, ~expr->GetName()); \
                case EValueType::Double: \
                    return builder.CreateFCmp##foptype(lhsValue, rhsValue, ~expr->GetName()); \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break;
        XX(Equal, EQ, UEQ)
        XX(NotEqual, NE, UNE)
        XX(Less, SLT, ULT)
        XX(LessOrEqual, SLE, ULE)
        XX(Greater, SGT, UGT)
        XX(GreaterOrEqual, SGE, UGE)
#undef XX
    }
    YUNREACHABLE();
}

Value* CodegenRowValuesArray(TIRBuilder& builder, Value* row)
{
    Value* headerPtr = builder.CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        "headerPtr");
    Value* valuesPtr = builder.CreatePointerCast(
        builder.CreateConstGEP1_32(headerPtr, 1),
        TypeBuilder<TValue*, false>::get(builder.getContext()),
        "valuesPtr");
    return valuesPtr;
}

Value* CodegenDataPtrFromRow(TIRBuilder& builder, Value* row, int index, EValueType type)
{
    Value* rowValuesArray = CodegenRowValuesArray(builder, row);
    Value* dataPtr = builder.CreateConstGEP2_32(
        rowValuesArray,
        index,
        TypeBuilder<TValue, false>::Fields::Data,
        "dataPtr");

    typedef TypeBuilder<TValueData, false> TUnionType;

    TUnionType::Fields field;
    switch (type) { // int, double or string
        case EValueType::Integer:
            field = TUnionType::Fields::Integer;
            break;
        case EValueType::Double:
            field = TUnionType::Fields::Double;
            break;
        case EValueType::String:
            field = TUnionType::Fields::String;
            break;
        default:
            YUNREACHABLE();
    }

    dataPtr = builder.CreatePointerCast(
        dataPtr,
        TUnionType::getAs(field, builder.getContext()),
        "dataPtr");

    return dataPtr;
}

Value* CodegenIdPtrFromRow(TIRBuilder& builder, Value* row, int index)
{
    Value* rowValuesArray = CodegenRowValuesArray(builder, row);
    Value* idPtr = builder.CreateConstGEP2_32(
        rowValuesArray,
        index,
        TypeBuilder<TValue, false>::Fields::Id,
        "idPtr");

    return idPtr;
}

Value* CodegenTypePtrFromRow(TIRBuilder& builder, Value* row, int index)
{
    Value* rowValuesArray = CodegenRowValuesArray(builder, row);
    Value* typePtr = builder.CreateConstGEP2_32(
        rowValuesArray,
        index,
        TypeBuilder<TValue, false>::Fields::Type,
        "typePtr");

    return typePtr;
}

Value* CodegenExpr(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TExpression* expr,
    const TTableSchema& tableSchema,
    Value* row)
{
    YASSERT(expr);
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
        case EExpressionKind::DoubleLiteral: {
            auto it = ctx.Params.NodeToConstantIndex.find(expr);
            YCHECK(it != ctx.Params.NodeToConstantIndex.end());
            int index = it->Second();
            return builder.CreateLoad(
                CodegenDataPtrFromRow(
                    builder,
                    ctx.GetConstantsRows(builder),
                    index,
                    expr->GetType(tableSchema)));
        }
        case EExpressionKind::Reference: {
            int index = tableSchema.GetColumnIndexOrThrow(expr->As<TReferenceExpression>()->GetName());
            return builder.CreateLoad(
                CodegenDataPtrFromRow(
                    builder,
                    row,
                    index,
                    expr->GetType(tableSchema)));
        }
        case EExpressionKind::Function:
            return CodegenFunctionExpr(
                builder,
                ctx,
                expr->As<TFunctionExpression>(),
                tableSchema,
                row);
        case EExpressionKind::BinaryOp:
            return CodegenBinaryOpExpr(
                builder,
                ctx,
                expr->As<TBinaryOpExpression>(),
                tableSchema,
                row);
        default:
            YUNREACHABLE();
    }
}

void CodegenAggregateFunction(
    TIRBuilder& builder,
    EAggregateFunctions aggregateFunction,
    Value* aggregateValuePtr,
    Value* newValue)
{
    Value* oldAggregateValue = builder.CreateLoad(aggregateValuePtr);
    Value* newAggregateValue = nullptr;
    switch (aggregateFunction) {
        case EAggregateFunctions::Sum:
            newAggregateValue = builder.CreateBinOp(
                Instruction::BinaryOps::Add,
                oldAggregateValue,
                newValue);
            break;
        case EAggregateFunctions::Min:
            newAggregateValue = builder.CreateSelect(
                builder.CreateICmpSLE(oldAggregateValue, newValue),
                oldAggregateValue,
                newValue);
            break;
        case EAggregateFunctions::Max:
            newAggregateValue = builder.CreateSelect(
                builder.CreateICmpSGE(oldAggregateValue, newValue),
                oldAggregateValue,
                newValue);
            break;
        default:
            YUNIMPLEMENTED();
    }
    builder.CreateStore(newAggregateValue, aggregateValuePtr);
}

void CodegenForEachRow(
    TIRBuilder& builder,
    Value* rows,
    Value* size,
    const TCodegenConsumer& codegenConsumer)
{
    auto& context = builder.getContext();

    Function* function = builder.GetInsertBlock()->getParent();

    Value* indexPtr = builder.CreateAlloca(
        TypeBuilder<i32, false>::get(context),
        ConstantInt::get(Type::getInt32Ty(context), 1, true));

    builder.CreateStore(
        ConstantInt::get(Type::getInt32Ty(context), 0, true),
        indexPtr);

    BasicBlock* loopBB = BasicBlock::Create(context, "loop", function);
    BasicBlock* condBB = BasicBlock::Create(context, "cond", function);
    BasicBlock* endloopBB = BasicBlock::Create(context, "endloop", function);

    builder.CreateBr(condBB);

    builder.SetInsertPoint(condBB);

    // index = *indexPtr
    Value* index = builder.CreateLoad(indexPtr);
    // if (index != size) ...
    builder.CreateCondBr(
        builder.CreateICmpNE(index, size),
        loopBB,
        endloopBB);

    builder.SetInsertPoint(loopBB);

    // codegenConsumer(*(rows + index))
    codegenConsumer(builder, builder.CreateLoad(builder.CreateGEP(rows, index)));

    // *indexPtr = index + 1;
    builder.CreateStore(
        builder.CreateAdd(
            index,
            ConstantInt::get(Type::getInt32Ty(context), 1)),
        indexPtr);
    builder.CreateBr(condBB);

    builder.SetInsertPoint(endloopBB);
}

void CodegenSetRowValue(
    TIRBuilder& builder,
    Value* row,
    int index,
    ui16 id,
    EValueType type,
    Value* data)
{
    auto& context = builder.getContext();

    Value* idPtr = CodegenIdPtrFromRow(builder, row, index);
    Value* typePtr = CodegenTypePtrFromRow(builder, row, index);
    Value* dataPtr = CodegenDataPtrFromRow(builder, row, index, type);

    builder.CreateStore(
        ConstantInt::get(Type::getInt16Ty(context), id),
        idPtr);
    builder.CreateStore(
        ConstantInt::get(Type::getInt16Ty(context), type),
        typePtr);
    builder.CreateStore(
        data,
        dataPtr);
}

////////////////////////////////////////////////////////////////////////////////

TCodegenedFunction TCodegenControllerImpl::CodegenEvaluate(
    const TOperator* op,
    const TCGImmediates& fragmentParams)
{
    YASSERT(op);
    LOG_DEBUG("Generating evaluation code  (Op: %p)", op);

    // See TCodegenedFunction.
    Function* function = Function::Create(
        TypeBuilder<void(TRow, TPassedFragmentParams*, void*, void*), false>::get(Context_),
        Function::ExternalLinkage,
        "Evaluate",
        Module_);

    Function::arg_iterator args = function->arg_begin();
    Value* constants = args;
    constants->setName("constants");
    Value* passedFragmentParamsPtr = ++args;
    passedFragmentParamsPtr->setName("passedFragmentParamsPtr");
    Value* batch = ++args;
    batch->setName("batch");
    Value* writer = ++args;
    writer->setName("writer");
    YCHECK(++args == function->arg_end());

    TIRBuilder builder(BasicBlock::Create(Context_, "entry", function));

    TCGContext ctx(fragmentParams, constants, passedFragmentParamsPtr);

    CodegenOp(builder, ctx, op,
        [&] (TIRBuilder& innerBuilder, Value* row) {
            Value* batchRef = innerBuilder.ViaClosure(batch);
            Value* writerRef = innerBuilder.ViaClosure(writer);
            innerBuilder.CreateCall3(
                GetRoutine(&NRoutines::WriteRow, "WriteRow"),
                row,
                batchRef,
                writerRef);
    });

    builder.CreateRetVoid();
    verifyFunction(*function);

    FunctionPassManager_->run(*function);
    ModulePassManager_->run(*Module_);

    //Module_->dump();

    TCodegenedFunction codegenedFunction = reinterpret_cast<TCodegenedFunction>(
        ExecutionEngine_->getPointerToFunction(function));

    LOG_DEBUG("Done generating evaluation code (Op: %p)", op);

    return codegenedFunction;
}

void TCodegenControllerImpl::CodegenOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan:
            return CodegenScanOp(builder, ctx, op->As<TScanOperator>(), codegenConsumer);
        case EOperatorKind::Union:
            return CodegenUnionOp(builder, ctx, op->As<TUnionOperator>(), codegenConsumer);
        case EOperatorKind::Filter:
            return CodegenFilterOp(builder, ctx, op->As<TFilterOperator>(), codegenConsumer);
        case EOperatorKind::Project:
            return CodegenProjectOp(builder, ctx, op->As<TProjectOperator>(), codegenConsumer);
        case EOperatorKind::Group:
            return CodegenGroupOp(builder, ctx, op->As<TGroupOperator>(), codegenConsumer);
    }
    YUNREACHABLE();
}

void TCodegenControllerImpl::CodegenScanOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TScanOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int), false>::get(Context_),
        Function::ExternalLinkage,
        "ScanOpInner",
        Module_);

    Function::arg_iterator args = function->arg_begin();
    Value* closure = args;
    Value* rows = ++args;
    Value* size = ++args;
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(Context_, "entry", function),
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, codegenConsumer);

    innerBuilder.CreateRetVoid();

    Value* passedFragmentParamsPtr = ctx.GetPassedFragmentParamsPtr(builder);

    auto it = ctx.Params.ScanOpToDataSplits.find(op);
    YCHECK(it != ctx.Params.ScanOpToDataSplits.end());
    int dataSplitsIndex = it->Second();

    builder.CreateCall4(
        GetRoutine(&NRoutines::ScanOpHelper, "ScanOpHelper"),
        passedFragmentParamsPtr,
        ConstantInt::get(Type::getInt32Ty(Context_), dataSplitsIndex, true),
        innerBuilder.GetClosure(),
        function);
}

void TCodegenControllerImpl::CodegenUnionOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TUnionOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    for (const auto& sourceOp : op->Sources()) {
        CodegenOp(builder, ctx, sourceOp, codegenConsumer);
    }
}

void TCodegenControllerImpl::CodegenFilterOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TFilterOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto sourceTableSchema = op->GetTableSchema();

    CodegenOp(builder, ctx, op->GetSource(),
        [&] (TIRBuilder& innerBuilder, Value* row) {
            Value* result;
            result = CodegenExpr(innerBuilder, ctx, op->GetPredicate(), sourceTableSchema, row);
            result = innerBuilder.CreateZExtOrBitCast(result, TypeBuilder<i64, false>::get(Context_));

            Function* function = innerBuilder.GetInsertBlock()->getParent();

            BasicBlock* ifBB = BasicBlock::Create(Context_, "if", function);
            BasicBlock* endIfBB = BasicBlock::Create(Context_, "endif", function);

            innerBuilder.CreateCondBr(
                innerBuilder.CreateICmpNE(
                    result,
                    ConstantInt::get(Type::getInt64Ty(Context_), 0, true)),
                ifBB,
                endIfBB);

            innerBuilder.SetInsertPoint(ifBB);
            codegenConsumer(innerBuilder, row);
            innerBuilder.CreateBr(endIfBB);

            innerBuilder.SetInsertPoint(endIfBB);
    });
}

void TCodegenControllerImpl::CodegenProjectOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TProjectOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    // See ProjectOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow), false>::get(Context_),
        Function::ExternalLinkage,
        "ProjectOpInner",
        Module_);

    Function::arg_iterator args = function->arg_begin();
    Value* closure = args;
    Value* newRow = ++args;
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(Context_, "entry", function),
        &builder,
        closure);

    int projectionCount = op->GetProjectionCount();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    CodegenOp(innerBuilder, ctx, op->GetSource(),
        [&, newRow] (TIRBuilder& innerBuilder, Value* row) {
            Value* newRowRef = innerBuilder.ViaClosure(newRow);

            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = op->GetProjection(index).Expression;
                const auto& name = op->GetProjection(index).Name;
                auto id = nameTable->GetId(name);
                auto type = expr->GetType(sourceTableSchema);

                Value* data = CodegenExpr(innerBuilder, ctx, expr, sourceTableSchema, row);

                CodegenSetRowValue(innerBuilder, newRowRef, index, id, type, data);
            }

            codegenConsumer(innerBuilder, newRowRef);
    });

    innerBuilder.CreateRetVoid();

    builder.CreateCall3(
        GetRoutine(&NRoutines::ProjectOpHelper, "ProjectOpHelper"),
        ConstantInt::get(Type::getInt32Ty(Context_), projectionCount, true),
        innerBuilder.GetClosure(),
        function);
}

void TCodegenControllerImpl::CodegenGroupOp(
    TIRBuilder& builder,
    const TCGContext& ctx,
    const TGroupOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    // See GroupOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, void*, void*, void*, TRow*), false>::get(Context_),
        Function::ExternalLinkage,
        "GroupOpInner",
        Module_);

    Function::arg_iterator args = function->arg_begin();
    Value* closure = args;
    Value* memoryPool = ++args;
    Value* groupedRows = ++args;
    Value* rows = ++args;
    Value* newRowPtr = ++args;
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(Context_, "entry", function),
        &builder,
        closure);

    int keySize = op->GetGroupItemCount();
    int aggregateItemCount = op->AggregateItems().size();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    CodegenOp(innerBuilder, ctx, op->GetSource(), [&] (TIRBuilder& innerBuilder, Value* row) {
        Value* memoryPoolRef = innerBuilder.ViaClosure(memoryPool);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);

        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = op->GetGroupItem(index).Expression;
            const auto& name = op->GetGroupItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            Value* data = CodegenExpr(innerBuilder, ctx, expr, sourceTableSchema, row);

            CodegenSetRowValue(innerBuilder, newRowRef, index, id, type, data);
        }

        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& expr = op->GetAggregateItem(index).Expression;
            const auto& name = op->GetAggregateItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            Value* data = CodegenExpr(innerBuilder, ctx, expr, sourceTableSchema, row);

            CodegenSetRowValue(innerBuilder, newRowRef, keySize + index, id, type, data);
        }

        Value* foundRowPtr = innerBuilder.CreateCall2(
            GetRoutine(&NRoutines::FindRow, "FindRow"),
            rowsRef,
            newRowRef);

        Function* function = innerBuilder.GetInsertBlock()->getParent();

        BasicBlock* ifBB = BasicBlock::Create(Context_, "if", function);
        BasicBlock* elseBB = BasicBlock::Create(Context_, "else", function);
        BasicBlock* endIfBB = BasicBlock::Create(Context_, "endif", function);

        innerBuilder.CreateCondBr(
            innerBuilder.CreateICmpNE(
                foundRowPtr,
                llvm::ConstantPointerNull::get(newRowRef->getType()->getPointerTo())),
            ifBB,
            elseBB);

        innerBuilder.SetInsertPoint(ifBB);
        Value* foundRow = innerBuilder.CreateLoad(foundRowPtr);
        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& item = op->AggregateItems()[index];

            auto type = item.Expression->GetType(sourceTableSchema);
            auto fn = item.AggregateFunction;

            Value* aggregateValuePtr = CodegenDataPtrFromRow(innerBuilder, foundRow, keySize + index, type);
            Value* newValuePtr = CodegenDataPtrFromRow(innerBuilder, newRowRef, keySize + index, type);
            Value* newValue = innerBuilder.CreateLoad(newValuePtr);

            CodegenAggregateFunction(innerBuilder, fn, aggregateValuePtr, newValue);
        }
        innerBuilder.CreateBr(endIfBB);

        innerBuilder.SetInsertPoint(elseBB);
        innerBuilder.CreateCall5(
            GetRoutine(&NRoutines::AddRow, "AddRow"),
            memoryPoolRef,
            rowsRef,
            groupedRowsRef,
            newRowPtrRef,
            ConstantInt::get(Type::getInt32Ty(Context_), keySize + aggregateItemCount, true));
        innerBuilder.CreateBr(endIfBB);

        innerBuilder.SetInsertPoint(endIfBB);
    });

    CodegenForEachRow(
        innerBuilder,
        innerBuilder.CreateCall(GetRoutine(&NRoutines::GetRowsData, "GetRowsData"), groupedRows),
        innerBuilder.CreateCall(GetRoutine(&NRoutines::GetRowsSize, "GetRowsSize"), groupedRows),
        codegenConsumer);

    innerBuilder.CreateRetVoid();

    builder.CreateCall4(
        GetRoutine(&NRoutines::GroupOpHelper, "GroupOpHelper"),
        ConstantInt::get(Type::getInt32Ty(Context_), keySize, true),
        ConstantInt::get(Type::getInt32Ty(Context_), aggregateItemCount, true),
        innerBuilder.GetClosure(),
        function);
}

////////////////////////////////////////////////////////////////////////////////

TError TCodegenControllerImpl::EvaluateViaCache(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemedWriterPtr writer)
{
    llvm::FoldingSetNodeID id;
    TFragmentParams fragmentParams;

    TFragmentProfileVisitor(id, fragmentParams).Profile(fragment.GetHead());

    TCodegenedFragment* codegenedFragment = nullptr;

    {
        TGuard<TSpinLock> guard(FoldingSetSpinLock_);

        void* insertPoint = nullptr;
        codegenedFragment = CodegenedFragmentsFoldingSet_.FindNodeOrInsertPos(id, insertPoint);

        if (!codegenedFragment) {
            codegenedFragment = new TCodegenedFragment(id, nullptr);
            CodegenedFragmentsFoldingSet_.InsertNode(codegenedFragment, insertPoint);
        }
    }

    if (!codegenedFragment->CodegenedFunction_) {
        auto voidFuture = BIND([&] () {
            if (codegenedFragment->CodegenedFunction_) {
                return;
            }
            codegenedFragment->CodegenedFunction_ = CodegenEvaluate(
                fragment.GetHead(),
                fragmentParams);
        })
        .AsyncVia(CodegenInvoker_)
        .Run();

        WaitFor(voidFuture);
    }

    // Make TRow from fragmentParams.ConstantArray.
    TChunkedMemoryPool memoryPool;
    TRow constants = TRow::Allocate(&memoryPool, fragmentParams.ConstantArray.size());

    for (int i = 0; i < fragmentParams.ConstantArray.size(); ++i) {
        constants[i] = fragmentParams.ConstantArray[i];
    }

    try {
        LOG_DEBUG("Evaluating plan fragment");

        LOG_DEBUG("Opening writer");
        {
            auto error = WaitFor(writer->Open(
                fragment.GetHead()->GetTableSchema(),
                fragment.GetHead()->GetKeyColumns()));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        //std::vector<TRow> batch;
        //batch.reserve(MaxRowsPerWrite);


        std::pair<std::vector<TRow>, TChunkedMemoryPool*> batch;
        batch.second = &memoryPool;
        batch.first.reserve(MaxRowsPerWrite);

        TPassedFragmentParams passedFragmentParams;
        passedFragmentParams.Callbacks = callbacks;
        passedFragmentParams.Context = fragment.GetContext().Get();
        passedFragmentParams.DataSplitsArray = &fragmentParams.DataSplitsArray;

        codegenedFragment->CodegenedFunction_(
            constants,
            &passedFragmentParams,
            &batch,
            writer.Get());

        LOG_DEBUG("Flusing writer");
        if (!batch.first.empty()) {
            if (!writer->Write(batch.first)) {
                auto error = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }
        }

        LOG_DEBUG("Closing writer");
        {
            auto error = WaitFor(writer->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        LOG_DEBUG("Finished evaluating plan fragment");
    } catch (const std::exception& ex) {
        auto error = TError("Failed to evaluate plan fragment") << ex;
        LOG_ERROR(error);
        return error;
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

/// TCodegenController
namespace NYT {
namespace NQueryClient {

TCodegenController::TCodegenController(IInvokerPtr invoker)
    : Impl(std::make_unique<TCodegenControllerImpl>(invoker))
{ }

TCodegenController::~TCodegenController()
{ }

TError TCodegenController::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemedWriterPtr writer)
{
    return Impl->Run(callbacks, fragment, std::move(writer));
}

} // namespace NQueryClient
} // namespace NYT
