#include "stdafx.h"
#include "cg_fragment_compiler.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_fragment.h"

#include "callbacks.h"

#include "cg_fragment.h"
#include "cg_routines.h"
#include "cg_ir_builder.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
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

#include <mutex>

// TODO(sandello):
//  - Cleanup TPassedFragmentParams & TCGContext and their usages
//  - Implement basic logging & profiling within evaluation code
//

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

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

typedef TContextIRBuilder TIRBuilder;

typedef std::function<void(TIRBuilder& builder, Value* row)> TCodegenConsumer;

class TCGContext
{
public:
    static Function* CodegenEvaluate(
        const TPlanFragment& planFragment,
        const TCGFragment& cgFragment,
        const TCGBinding& binding);

private:
    const TCGFragment& Fragment_;
    const TCGBinding& Binding_;
    Value* ConstantsRow_;
    Value* RowBuffers_;
    Value* PassedFragmentParamsPtr_;

    TCGContext(
        const TCGFragment& cgFragment,
        const TCGBinding& binding,
        Value* constantsRow,
        Value* rowBuffers,
        Value* passedFragmentParamsPtr)
        : Fragment_(cgFragment)
        , Binding_(binding)
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
    builder.CreateStore(ConstantInt::getFalse(context), newValueIsNullPtr);
    Value* newValue = CodegenValueFromRow(builder, newRow, index, type, newValueIsNullPtr);

    auto* aggregateBB = BasicBlock::Create(context, "aggregate", function);
    auto* aggregateEndBB = BasicBlock::Create(context, "aggregateEnd", function);
    builder.CreateCondBr(builder.CreateLoad(newValueIsNullPtr), aggregateEndBB, aggregateBB);

    // aggregate
    builder.SetInsertPoint(aggregateBB);

    Value* isNullPtr = builder.CreateAlloca(
        TypeBuilder<llvm::types::i<1>, false>::get(context), 0, "isNullPtr");
    builder.CreateStore(ConstantInt::getFalse(context), isNullPtr);
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

Type* GetCommonType(Type* a, Type* b)
{
    if (a->isIntegerTy()) {
        return a->getIntegerBitWidth() > b->getIntegerBitWidth()? a : b;
    }
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

    auto* getRhsValueBB = BasicBlock::Create(builder.getContext(), "getRhsValue", function);
    auto* evalResultBB = BasicBlock::Create(builder.getContext(), "evalResult", function);
    auto* resultBB = BasicBlock::Create(builder.getContext(), "result", function);

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
                case EValueType::Integer: { \
                    Type* commonType = GetCommonType(lhsValue->getType(), rhsValue->getType()); \
                    result = builder.CreateBinOp(Instruction::BinaryOps::optype, \
                        builder.CreateIntCast(lhsValue, commonType, true), \
                        builder.CreateIntCast(rhsValue, commonType, true), \
                        ~expr->GetName()); \
                    break; }\
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
            auto it = Binding_.NodeToConstantIndex.find(expr);
            YCHECK(it != Binding_.NodeToConstantIndex.end());
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
    auto module = builder.GetInsertBlock()->getParent()->getParent();

    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "ScanOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args;
    closure->setName("closure");
    Value* rows = ++args;
    rows->setName("rows");
    Value* size = ++args;
    size->setName("size");
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(builder.getContext(), "entry", function),
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, codegenConsumer);

    innerBuilder.CreateRetVoid();

    Value* passedFragmentParamsPtr = GetPassedFragmentParamsPtr(builder);

    auto it = Binding_.ScanOpToDataSplits.find(op);
    YCHECK(it != Binding_.ScanOpToDataSplits.end());
    int dataSplitsIndex = it->second;

    builder.CreateCall4(
        Fragment_.GetRoutine("ScanOpHelper"),
        passedFragmentParamsPtr,
        builder.getInt32(dataSplitsIndex),
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

            Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
            innerBuilder.CreateStore(builder.getFalse(), isNullPtr);
            
            Value* result = innerBuilder.CreateZExtOrBitCast(
                CodegenExpr(innerBuilder, op->GetPredicate(), sourceTableSchema, row, isNullPtr),
                builder.getInt64Ty());

            Function* function = innerBuilder.GetInsertBlock()->getParent();

            auto* ifBB = BasicBlock::Create(builder.getContext(), "if", function);
            auto* endIfBB = BasicBlock::Create(builder.getContext(), "endif", function);

            innerBuilder.CreateCondBr(
                innerBuilder.CreateICmpNE(
                    result,
                    builder.getInt64(0)),
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

    Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

    CodegenOp(builder, op->GetSource(),
        [&] (TIRBuilder& innerBuilder, Value* row) {
            Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);
            
            innerBuilder.CreateCall3(
                Fragment_.GetRoutine("AllocateRow"),
                GetRowBuffers(innerBuilder),
                builder.getInt32(projectionCount),
                newRowPtrRef);

            Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = op->GetProjection(index).Expression;
                const auto& name = op->GetProjection(index).Name;
                auto id = nameTable->GetId(name);
                auto type = expr->GetType(sourceTableSchema);

                Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
                innerBuilder.CreateStore(builder.getFalse(), isNullPtr);
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
    auto module = builder.GetInsertBlock()->getParent()->getParent();

    // See GroupOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, void*, void*), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "GroupOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args;    
    Value* groupedRows = ++args;
    Value* rows = ++args;
    YCHECK(++args == function->arg_end());

    TIRBuilder innerBuilder(
        BasicBlock::Create(builder.getContext(), "entry", function),
        &builder,
        closure);

    int keySize = op->GetGroupItemCount();
    int aggregateItemCount = op->AggregateItems().size();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

    CodegenOp(innerBuilder, op->GetSource(), [&] (TIRBuilder& innerBuilder, Value* row) {
        Value* memoryPoolsRef = GetRowBuffers(innerBuilder);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);

        innerBuilder.CreateCall3(
            Fragment_.GetRoutine("AllocateRow"),
            memoryPoolsRef,
            builder.getInt32(keySize + aggregateItemCount),
            newRowPtrRef);

        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = op->GetGroupItem(index).Expression;
            const auto& name = op->GetGroupItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
            innerBuilder.CreateStore(builder.getFalse(), isNullPtr);
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

            Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
            innerBuilder.CreateStore(builder.getFalse(), isNullPtr);
            Value* data = CodegenExpr(innerBuilder, expr, sourceTableSchema, row, isNullPtr);
            CodegenSetRowValue(innerBuilder, newRowRef, keySize + index, id, type, data, isNullPtr);
        }

        Value* foundRowPtr = innerBuilder.CreateCall2(
            Fragment_.GetRoutine("FindRow"),
            rowsRef,
            newRowRef);

        Function* function = innerBuilder.GetInsertBlock()->getParent();

        auto* ifBB = BasicBlock::Create(builder.getContext(), "if", function);
        auto* elseBB = BasicBlock::Create(builder.getContext(), "else", function);
        auto* endIfBB = BasicBlock::Create(builder.getContext(), "endif", function);

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
            Fragment_.GetRoutine("AddRow"),
            memoryPoolsRef,
            rowsRef,
            groupedRowsRef,
            newRowPtrRef,
            builder.getInt32(keySize + aggregateItemCount));
        innerBuilder.CreateBr(endIfBB);

        innerBuilder.SetInsertPoint(endIfBB);
    });

    CodegenForEachRow(
        innerBuilder,
        innerBuilder.CreateCall(Fragment_.GetRoutine("GetRowsData"), groupedRows),
        innerBuilder.CreateCall(Fragment_.GetRoutine("GetRowsSize"), groupedRows),
        codegenConsumer);

    innerBuilder.CreateRetVoid();

    builder.CreateCall4(
        Fragment_.GetRoutine("GroupOpHelper"),
        builder.getInt32(keySize),
        builder.getInt32(aggregateItemCount),
        innerBuilder.GetClosure(),
        function);
}

Function* TCGContext::CodegenEvaluate(
    const TPlanFragment& planFragment,
    const TCGFragment& cgFragment,
    const TCGBinding& binding)
{
    auto op = planFragment.GetHead();
    YASSERT(op);

    auto* module = cgFragment.GetModule();
    auto& context = module->getContext();

    // See TCodegenedFunction.
    Function* function = Function::Create(
        TypeBuilder<void(TRow, TPassedFragmentParams*, void*, void*, void*), false>::get(context),
        Function::ExternalLinkage,
        "Evaluate",
        module);

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

    TIRBuilder builder(BasicBlock::Create(context, "entry", function));

    TCGContext ctx(cgFragment, binding, constants, pools, passedFragmentParamsPtr);

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

TCGFragmentCompiler CreateFragmentCompiler()
{
    using namespace std::placeholders;
    return std::bind(&TCGContext::CodegenEvaluate, _1, _2, _3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


