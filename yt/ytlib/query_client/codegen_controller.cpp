#include "stdafx.h"
#include "codegen_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_fragment.h"

#include "callbacks.h"

#include "cg_cache.h"
#include "cg_fragment.h"
#include "cg_routines.h"
#include "cg_types.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>
#include <ytlib/new_table_client/row_buffer.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>

#include <core/misc/lazy_ptr.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/chunked_memory_pool.h>

#include <core/logging/tagged_logger.h>

#include <llvm/IR/TypeBuilder.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/Intrinsics.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

// TODO(sandello):
//  - Cleanup TFragmentParams, TPassedFragmentParams, TCGImmediates & TCGContext
//    and their usages
//  - Implement basic logging & profiling within evaluation code
//
////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NQueryClient {

static auto& Logger = QueryClientLogger;

static TLazyIntrusivePtr<NConcurrency::TActionQueue> CodegenQueue(
    NConcurrency::TActionQueue::CreateFactory("Codegen"));

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
        auto* function = basicBlock->getParent();
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
        auto* function = basicBlock->getParent();
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

typedef std::function<void(TIRBuilder& builder, Value* row)> TCodegenConsumer;

class TCGContext
{
public:
    static Function* CodegenEvaluate(
        TCGFragment& cgFragment,
        const TCGImmediates& params,
        const TOperator* op);

private:
    TCGFragment& CGFragment_;
    llvm::LLVMContext& Context_;
    llvm::Module* Module_;
    const TCGImmediates& Params_;
    Value* ConstantsRow_;
    Value* RowBuffers_;
    Value* PassedFragmentParamsPtr_;
    std::map<void*, Function*> CachedRoutines_;

    TCGContext(
        TCGFragment& cgFragment,
        const TCGImmediates& params,
        Value* constantsRow,
        Value* rowBuffers,
        Value* passedFragmentParamsPtr)
        : CGFragment_(cgFragment)
        , Context_(CGFragment_.GetContext())
        , Module_(CGFragment_.GetModule())
        , Params_(params)
        , ConstantsRow_(constantsRow)
        , RowBuffers_(rowBuffers)
        , PassedFragmentParamsPtr_(passedFragmentParamsPtr)
    { }

    Value* GetConstantsRows(TIRBuilder& builder) const
    {
        return builder.ViaClosure(
            ConstantsRow_,
            "constantsRow");
    }

    Value* GetRowBuffers(TIRBuilder& builder) const
    {
        return builder.ViaClosure(
            RowBuffers_,
            "rowBuffers");
    }

    Value* GetPassedFragmentParamsPtr(TIRBuilder& builder) const
    {
        return builder.ViaClosure(
            PassedFragmentParamsPtr_,
            "passedFragmentParamsPtr");
    }

    Value* CodegenExpr(
        TIRBuilder& builder,
        const TExpression* expr,
        const TTableSchema& tableSchema,
        Value* row,
        Value* isNullPtr);

    Value* CodegenFunctionExpr(
        TIRBuilder& builder,
        const TFunctionExpression* expr,
        const TTableSchema& tableSchema,
        Value* row,
        Value* isNullPtr);

    Value* CodegenBinaryOpExpr(
        TIRBuilder& builder,
        const TBinaryOpExpression* expr,
        const TTableSchema& tableSchema,
        Value* row,
        Value* isNullPtr);


    void CodegenOp(
        TIRBuilder& builder,
        const TOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenScanOp(
        TIRBuilder& builder,
        const TScanOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenFilterOp(
        TIRBuilder& builder,
        const TFilterOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenProjectOp(
        TIRBuilder& builder,
        const TProjectOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenGroupOp(
        TIRBuilder& builder,
        const TGroupOperator* op,
        const TCodegenConsumer& codegenConsumer);

};

////////////////////////////////////////////////////////////////////////////////

Value* CodegenRowValuesArray(TIRBuilder& builder, Value* row)
{
    Value* headerPtr = builder.CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        "headerPtr");
    Value* valuesPtr = builder.CreatePointerCast(
        builder.CreateConstGEP1_32(headerPtr, 1, "valuesPtr"),
        TypeBuilder<TValue*, false>::get(builder.getContext()),
        "valuesPtrCasted");
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
        "dataPtrCasted");

    return dataPtr;
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

void CodegenIsNull(TIRBuilder& builder, Value* row, int index, Value* isNullPtr)
{
    auto& context = builder.getContext();
    Value* type = builder.CreateLoad(CodegenTypePtrFromRow(builder, row, index));

    Value* currentNull = builder.CreateSelect(
        builder.CreateICmpEQ(
            type, 
            ConstantInt::get(Type::getInt16Ty(context), EValueType::Null)),
        ConstantInt::getTrue(context),
        builder.CreateLoad(isNullPtr));

    builder.CreateStore(currentNull, isNullPtr);
}

Value* CodegenNullValue(TIRBuilder& builder, Type* type)
{
    if (type->isIntegerTy()) {
        return ConstantInt::get(type, 0);
    } else if (type->isDoubleTy()) {
        return ConstantFP::get(type, 0);
    }

    YUNREACHABLE();
}

Value* CodegenValueFromRow(
    TIRBuilder& builder,
    Value* row,
    int index,
    EValueType type,
    Value* isNullPtr,
    llvm::Twine name = llvm::Twine())
{
    auto& context = builder.getContext();
    Function* function = builder.GetInsertBlock()->getParent();

    auto* getValueBB = BasicBlock::Create(context, "getValue", function);
    auto* resultBB = BasicBlock::Create(context, "result", function);

    CodegenIsNull(builder, row, index, isNullPtr);

    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, getValueBB);
    auto* sourceBB = builder.GetInsertBlock();
    builder.SetInsertPoint(getValueBB);

    Value* value = builder.CreateLoad(CodegenDataPtrFromRow(builder, row, index, type));

    builder.CreateBr(resultBB);
    getValueBB = builder.GetInsertBlock();
    builder.SetInsertPoint(resultBB);

    llvm::PHINode* phiNode = builder.CreatePHI(value->getType(), 2, name);

    Value* nullValue = CodegenNullValue(builder, value->getType());

    phiNode->addIncoming(nullValue, sourceBB);
    phiNode->addIncoming(value, getValueBB);

    return phiNode;
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

void CodegenAggregateFunction(
    TIRBuilder& builder,
    Value* row,
    Value* newRow,
    EAggregateFunctions aggregateFunction,
    int index,
    EValueType type)
{
    auto& context = builder.getContext();
    Function* function = builder.GetInsertBlock()->getParent();

    Value* newValueIsNullPtr = builder.CreateAlloca(
        TypeBuilder<llvm::types::i<1>, false>::get(context), 0, "newValueIsNullPtr");
    Value* newValue = CodegenValueFromRow(builder, newRow, index, type, newValueIsNullPtr);

    auto* aggregateBB = BasicBlock::Create(context, "aggregate", function);
    auto* aggregateEndBB = BasicBlock::Create(context, "aggregateEnd", function);
    builder.CreateCondBr(builder.CreateLoad(newValueIsNullPtr), aggregateEndBB, aggregateBB);

    // aggregate
    builder.SetInsertPoint(aggregateBB);

    Value* isNullPtr = builder.CreateAlloca(
        TypeBuilder<llvm::types::i<1>, false>::get(context), 0, "isNullPtr");
    CodegenIsNull(builder, row, index, isNullPtr);

    Value* valuePtr = CodegenDataPtrFromRow(builder, row, index, type);

    auto* doAggregateBB = BasicBlock::Create(context, "doAggregate", function);
    auto* swapAggregateBB = BasicBlock::Create(context, "swapAggregate", function);
    builder.CreateCondBr(builder.CreateLoad(isNullPtr), swapAggregateBB, doAggregateBB);

    // do aggregate
    builder.SetInsertPoint(doAggregateBB);
    Value* oldAggregateValue = builder.CreateLoad(valuePtr);
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
    builder.CreateStore(newAggregateValue, valuePtr);
    builder.CreateBr(aggregateEndBB);

    // swap aggregate
    builder.SetInsertPoint(swapAggregateBB);
    builder.CreateStore(newValue, valuePtr);
    builder.CreateBr(aggregateEndBB);

    builder.SetInsertPoint(aggregateEndBB);
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
        TypeBuilder<i32, false>::get(context), nullptr, "indexPtr");

    builder.CreateStore(
        ConstantInt::get(Type::getInt32Ty(context), 0, true),
        indexPtr);

    auto* loopBB = BasicBlock::Create(context, "loop", function);
    auto* condBB = BasicBlock::Create(context, "cond", function);
    auto* endloopBB = BasicBlock::Create(context, "endloop", function);

    builder.CreateBr(condBB);

    builder.SetInsertPoint(condBB);

    // index = *indexPtr
    Value* index = builder.CreateLoad(indexPtr, "index");
    // if (index != size) ...
    builder.CreateCondBr(
        builder.CreateICmpNE(index, size),
        loopBB,
        endloopBB);

    builder.SetInsertPoint(loopBB);

    // codegenConsumer(*(rows + index))
    codegenConsumer(builder, builder.CreateLoad(builder.CreateGEP(rows, index, "rowPtr"), "rowValue"));

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
    Value* data,
    Value* isNullPtr)
{
    auto& context = builder.getContext();

    Value* idPtr = CodegenIdPtrFromRow(builder, row, index);
    Value* typePtr = CodegenTypePtrFromRow(builder, row, index);
    Value* dataPtr = CodegenDataPtrFromRow(builder, row, index, type);

    builder.CreateStore(
        ConstantInt::get(Type::getInt16Ty(context), id),
        idPtr);

    builder.CreateStore(
        builder.CreateSelect(
            builder.CreateLoad(isNullPtr), 
            ConstantInt::get(Type::getInt16Ty(context), EValueType::Null), 
            ConstantInt::get(Type::getInt16Ty(context), 
            type)),
        typePtr);
    
    builder.CreateStore(
        data,
        dataPtr);
}

////////////////////////////////////////////////////////////////////////////////

Value* TCGContext::CodegenFunctionExpr(
    TIRBuilder& builder,
    const TFunctionExpression* expr,
    const TTableSchema& tableSchema,
    Value* row,
    Value* isNullPtr)
{
    YUNIMPLEMENTED();
}

Value* TCGContext::CodegenBinaryOpExpr(
    TIRBuilder& builder,
    const TBinaryOpExpression* expr,
    const TTableSchema& tableSchema,
    Value* row,
    Value* isNullPtr)
{
    Value* lhsValue = CodegenExpr(builder, expr->GetLhs(), tableSchema, row, isNullPtr);

    Function* function = builder.GetInsertBlock()->getParent();

    auto* getRhsValueBB = BasicBlock::Create(Context_, "getRhsValue", function);
    auto* evalResultBB = BasicBlock::Create(Context_, "evalResult", function);
    auto* resultBB = BasicBlock::Create(Context_, "result", function);

    auto* sourceBB = builder.GetInsertBlock();
    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, getRhsValueBB);
    builder.SetInsertPoint(getRhsValueBB);

    Value* rhsValue = CodegenExpr(builder, expr->GetRhs(), tableSchema, row, isNullPtr);

    getRhsValueBB = builder.GetInsertBlock();
    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, evalResultBB);
    builder.SetInsertPoint(evalResultBB);

    Value* result = 0;

    auto name = expr->GetName();

    switch (expr->GetOpcode()) {
        // Arithmetical operations.
#define XX(opcode, optype, foptype) \
        case EBinaryOp::opcode: \
            switch (expr->GetType(tableSchema)) { \
                case EValueType::Integer: \
                    result = builder.CreateBinOp(Instruction::BinaryOps::optype, lhsValue, rhsValue, ~expr->GetName()); \
                    break; \
                case EValueType::Double: \
                    result = builder.CreateBinOp(Instruction::BinaryOps::foptype, lhsValue, rhsValue, ~expr->GetName()); \
                    break; \
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
                    result = builder.CreateBinOp(Instruction::BinaryOps::optype, lhsValue, rhsValue, ~expr->GetName()); \
                    break; \
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
                    result = builder.CreateICmp##ioptype(lhsValue, rhsValue, ~expr->GetName()); \
                    break; \
                case EValueType::Double: \
                    result = builder.CreateFCmp##foptype(lhsValue, rhsValue, ~expr->GetName()); \
                    break; \
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

    builder.CreateBr(resultBB);
    builder.SetInsertPoint(resultBB);

    llvm::PHINode* phiNode = builder.CreatePHI(result->getType(), 3);

    Value* nullValue = CodegenNullValue(builder, result->getType());

    phiNode->addIncoming(nullValue, sourceBB);
    phiNode->addIncoming(nullValue, getRhsValueBB);
    phiNode->addIncoming(result, evalResultBB);

    return phiNode;
}

Value* TCGContext::CodegenExpr(
    TIRBuilder& builder,
    const TExpression* expr,
    const TTableSchema& tableSchema,
    Value* row,
    Value* isNullPtr)
{
    YASSERT(expr);
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
        case EExpressionKind::DoubleLiteral: {
            auto it = Params_.NodeToConstantIndex.find(expr);
            YCHECK(it != Params_.NodeToConstantIndex.end());
            int index = it->second;

            return CodegenValueFromRow(
                builder,
                GetConstantsRows(builder),
                index,
                expr->GetType(tableSchema), isNullPtr, "literal");
        }
        case EExpressionKind::Reference: {
            int index = tableSchema.GetColumnIndexOrThrow(expr->As<TReferenceExpression>()->GetName());
            return CodegenValueFromRow(
                builder,
                row,
                index,
                expr->GetType(tableSchema), isNullPtr, "reference");
        }
        case EExpressionKind::Function:
            return CodegenFunctionExpr(
                builder,
                expr->As<TFunctionExpression>(),
                tableSchema,
                row, isNullPtr);
        case EExpressionKind::BinaryOp:
            return CodegenBinaryOpExpr(
                builder,
                expr->As<TBinaryOpExpression>(),
                tableSchema,
                row, isNullPtr);
        default:
            YUNREACHABLE();
    }
}

// Codegen operators

void TCGContext::CodegenOp(
    TIRBuilder& builder,
    const TOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan:
            return CodegenScanOp(builder, op->As<TScanOperator>(), codegenConsumer);
        case EOperatorKind::Filter:
            return CodegenFilterOp(builder, op->As<TFilterOperator>(), codegenConsumer);
        case EOperatorKind::Project:
            return CodegenProjectOp(builder, op->As<TProjectOperator>(), codegenConsumer);
        case EOperatorKind::Group:
            return CodegenGroupOp(builder, op->As<TGroupOperator>(), codegenConsumer);
    }
    YUNREACHABLE();
}

void TCGContext::CodegenScanOp(
    TIRBuilder& builder,
    const TScanOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int), false>::get(Context_),
        Function::ExternalLinkage,
        "ScanOpInner",
        Module_);

    auto args = function->arg_begin();
    Value* closure = args;
    closure->setName("closure");
    Value* rows = ++args;
    rows->setName("rows");
    Value* size = ++args;
    size->setName("size");
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(Context_, "entry", function),
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, codegenConsumer);

    innerBuilder.CreateRetVoid();

    Value* passedFragmentParamsPtr = GetPassedFragmentParamsPtr(builder);

    auto it = Params_.ScanOpToDataSplits.find(op);
    YCHECK(it != Params_.ScanOpToDataSplits.end());
    int dataSplitsIndex = it->second;

    builder.CreateCall4(
        CGFragment_.GetRoutine("ScanOpHelper"),
        passedFragmentParamsPtr,
        ConstantInt::get(Type::getInt32Ty(Context_), dataSplitsIndex, true),
        innerBuilder.GetClosure(),
        function);
}

void TCGContext::CodegenFilterOp(
    TIRBuilder& builder,
    const TFilterOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto sourceTableSchema = op->GetTableSchema();

    CodegenOp(builder, op->GetSource(),
        [&] (TIRBuilder& innerBuilder, Value* row) {

            Value* isNullPtr = innerBuilder.CreateAlloca(
                TypeBuilder<llvm::types::i<1>, false>::get(Context_), 0, "isNullPtr");
            innerBuilder.CreateStore(ConstantInt::getFalse(Context_), isNullPtr);
            
            Value* result = innerBuilder.CreateZExtOrBitCast(
                CodegenExpr(innerBuilder, op->GetPredicate(), sourceTableSchema, row, isNullPtr),
                TypeBuilder<i64, false>::get(Context_));

            Function* function = innerBuilder.GetInsertBlock()->getParent();

            auto* ifBB = BasicBlock::Create(Context_, "if", function);
            auto* endIfBB = BasicBlock::Create(Context_, "endif", function);

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

void TCGContext::CodegenProjectOp(
    TIRBuilder& builder,
    const TProjectOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    int projectionCount = op->GetProjectionCount();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(Context_));

    CodegenOp(builder, op->GetSource(),
        [&] (TIRBuilder& innerBuilder, Value* row) {
            Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);
            
            innerBuilder.CreateCall3(
                CGFragment_.GetRoutine("AllocateRow"),
                GetRowBuffers(innerBuilder),
                ConstantInt::get(Type::getInt32Ty(Context_), projectionCount, true),
                newRowPtrRef);

            Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);
            
            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = op->GetProjection(index).Expression;
                const auto& name = op->GetProjection(index).Name;
                auto id = nameTable->GetId(name);
                auto type = expr->GetType(sourceTableSchema);

                Value* isNullPtr = innerBuilder.CreateAlloca(
                    TypeBuilder<llvm::types::i<1>, false>::get(Context_),
                    0,
                    "isNullPtr");
                innerBuilder.CreateStore(ConstantInt::getFalse(Context_), isNullPtr);
                Value* data = CodegenExpr(innerBuilder, expr, sourceTableSchema, row, isNullPtr);
                CodegenSetRowValue(innerBuilder, newRowRef, index, id, type, data, isNullPtr);
            }

            codegenConsumer(innerBuilder, newRowRef);
    });
}

void TCGContext::CodegenGroupOp(
    TIRBuilder& builder,
    const TGroupOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    // See GroupOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, void*, void*), false>::get(Context_),
        Function::ExternalLinkage,
        "GroupOpInner",
        Module_);

    auto args = function->arg_begin();
    Value* closure = args;    
    Value* groupedRows = ++args;
    Value* rows = ++args;
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(Context_, "entry", function),
        &builder,
        closure);

    int keySize = op->GetGroupItemCount();
    int aggregateItemCount = op->AggregateItems().size();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(Context_));

    CodegenOp(innerBuilder, op->GetSource(), [&] (TIRBuilder& innerBuilder, Value* row) {
        Value* memoryPoolsRef = GetRowBuffers(innerBuilder);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);

        innerBuilder.CreateCall3(
            CGFragment_.GetRoutine("AllocateRow"),
            memoryPoolsRef,
            ConstantInt::get(Type::getInt32Ty(Context_), keySize + aggregateItemCount, true), newRowPtrRef);

        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = op->GetGroupItem(index).Expression;
            const auto& name = op->GetGroupItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            Value* isNullPtr = innerBuilder.CreateAlloca(
                TypeBuilder<llvm::types::i<1>, false>::get(Context_), 0, "isNullPtr");
            innerBuilder.CreateStore(ConstantInt::getFalse(Context_), isNullPtr);
            Value* data = CodegenExpr(innerBuilder, expr, sourceTableSchema, row, isNullPtr);
            CodegenSetRowValue(innerBuilder, newRowRef, index, id, type, data, isNullPtr);
        }

        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& expr = op->GetAggregateItem(index).Expression;
            const auto& name = op->GetAggregateItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            Value* isNullPtr = innerBuilder.CreateAlloca(
                TypeBuilder<llvm::types::i<1>, false>::get(Context_), 0, "isNullPtr");
            innerBuilder.CreateStore(ConstantInt::getFalse(Context_), isNullPtr);
            Value* data = CodegenExpr(innerBuilder, expr, sourceTableSchema, row, isNullPtr);
            CodegenSetRowValue(innerBuilder, newRowRef, keySize + index, id, type, data, isNullPtr);
        }

        Value* foundRowPtr = innerBuilder.CreateCall2(
            CGFragment_.GetRoutine("FindRow"),
            rowsRef,
            newRowRef);

        Function* function = innerBuilder.GetInsertBlock()->getParent();

        auto* ifBB = BasicBlock::Create(Context_, "if", function);
        auto* elseBB = BasicBlock::Create(Context_, "else", function);
        auto* endIfBB = BasicBlock::Create(Context_, "endif", function);

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

            CodegenAggregateFunction(innerBuilder, foundRow, newRowRef, fn, keySize + index, type);
        }
        innerBuilder.CreateBr(endIfBB);

        innerBuilder.SetInsertPoint(elseBB);
        innerBuilder.CreateCall5(
            CGFragment_.GetRoutine("AddRow"),
            memoryPoolsRef,
            rowsRef,
            groupedRowsRef,
            newRowPtrRef,
            ConstantInt::get(Type::getInt32Ty(Context_), keySize + aggregateItemCount, true));
        innerBuilder.CreateBr(endIfBB);

        innerBuilder.SetInsertPoint(endIfBB);
    });

    CodegenForEachRow(
        innerBuilder,
        innerBuilder.CreateCall(CGFragment_.GetRoutine("GetRowsData"), groupedRows),
        innerBuilder.CreateCall(CGFragment_.GetRoutine("GetRowsSize"), groupedRows),
        codegenConsumer);

    innerBuilder.CreateRetVoid();

    builder.CreateCall4(
        CGFragment_.GetRoutine("GroupOpHelper"),
        ConstantInt::get(Type::getInt32Ty(Context_), keySize, true),
        ConstantInt::get(Type::getInt32Ty(Context_), aggregateItemCount, true),
        innerBuilder.GetClosure(),
        function);
}

Function* TCGContext::CodegenEvaluate(
    TCGFragment& cgFragment,
    const TCGImmediates& params,
    const TOperator* op)
{
    YASSERT(op);

    // See TCodegenedFunction.
    Function* function = Function::Create(
        TypeBuilder<void(TRow, TPassedFragmentParams*, void*, void*, void*), false>::get(cgFragment.GetContext()),
        Function::ExternalLinkage,
        "Evaluate",
        cgFragment.GetModule());

    auto args = function->arg_begin();
    Value* constants = args;
    constants->setName("constants");
    Value* passedFragmentParamsPtr = ++args;
    passedFragmentParamsPtr->setName("passedFragmentParamsPtr");
    Value* batch = ++args;
    batch->setName("batch");
    Value* pools = ++args;
    pools->setName("pools");
    Value* writer = ++args;
    writer->setName("writer");
    YCHECK(++args == function->arg_end());

    TIRBuilder builder(BasicBlock::Create(cgFragment.GetContext(), "entry", function));

    TCGContext ctx(cgFragment, params, constants, pools, passedFragmentParamsPtr);

    ctx.CodegenOp(builder, op,
        [&] (TIRBuilder& innerBuilder, Value* row) {
            Value* batchRef = innerBuilder.ViaClosure(batch, "batchRef");
            Value* writerRef = innerBuilder.ViaClosure(writer, "writerRef");
            innerBuilder.CreateCall3(
                cgFragment.GetRoutine("WriteRow"),
                row,
                batchRef,
                writerRef);
    });

    builder.CreateRetVoid();

    return function;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


// Controller
namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

class TCodegenController::TImpl
    : public NNonCopyable::TNonCopyable
{
public:
    TImpl()
    {
        llvm::llvm_start_multithreaded();
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmParser();
        llvm::InitializeNativeTargetAsmPrinter();
        RegisterCGRoutines();
    }

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer)
    {
        return EvaluateViaCache(
            callbacks,
            fragment,
            std::move(writer));
    }

private:
    TCGCache Cache_;

    TError EvaluateViaCache(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer)
    {
        auto result = Cache_.Codegen(
            fragment,
            [&] (const TPlanFragment& fragment, TCGFragment& cgFragment, const TFragmentParams& params) {
                cgFragment.SetMainFunction(
                    TCGContext::CodegenEvaluate(
                        cgFragment,
                        params,
                        fragment.GetHead()));
            });

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

            TPassedFragmentParams passedFragmentParams;
            passedFragmentParams.Callbacks = callbacks;
            passedFragmentParams.Context = fragment.GetContext().Get();
            passedFragmentParams.DataSplitsArray = &fragmentParams.DataSplitsArray;

            std::vector<TRow> batch;
            batch.reserve(MaxRowsPerWrite);
            TRowBuffer pools;

            codegenedFunction(
                constants,
                &passedFragmentParams,
                &batch,
                &pools,
                writer.Get());

            LOG_DEBUG("Flusing writer (FragmentId: %s)",
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

            LOG_DEBUG("Finished evaluating plan fragment (FragmentId: %s)",
                ~ToString(fragment.Id()));
        } catch (const std::exception& ex) {
            auto error = TError("Failed to evaluate plan fragment") << ex;
            LOG_ERROR(error);
            return error;
        }

        return TError();
    }

};

////////////////////////////////////////////////////////////////////////////////

TCodegenController::TCodegenController()
    : Impl_(std::make_unique<TCodegenController::TImpl>())
{ }

TCodegenController::~TCodegenController()
{ }

TError TCodegenController::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemedWriterPtr writer)
{
    return Impl_->Run(callbacks, fragment, std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
