#include "stdafx.h"
#include "cg_fragment_compiler.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_fragment.h"

#include "cg_fragment.h"
#include "cg_routines.h"
#include "cg_ir_builder.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>

#include <core/logging/tagged_logger.h>

#include <llvm/IR/Module.h>

// TODO(sandello):
//  - Implement basic logging & profiling within evaluation code
//  - Shadow innerBuilders everywhere

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;
using namespace NConcurrency;

// Import extensively used LLVM types.
using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::Function;
using llvm::FunctionType;
using llvm::Instruction;
using llvm::PHINode;
using llvm::Twine;
using llvm::Type;
using llvm::TypeBuilder;
using llvm::Value;

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGIRBuilder& builder, Value* row)> TCodegenConsumer;

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
    Value* PassedFragmentParamsPtr_;

    TCGContext(
        const TCGFragment& cgFragment,
        const TCGBinding& binding,
        Value* constantsRow,
        Value* passedFragmentParamsPtr)
        : Fragment_(cgFragment)
        , Binding_(binding)
        , ConstantsRow_(constantsRow)
        , PassedFragmentParamsPtr_(passedFragmentParamsPtr)
    { }

    Value* GetConstantsRows(TCGIRBuilder& builder) const
    {
        return builder.ViaClosure(ConstantsRow_, "constantsRow");
    }

    Value* GetPassedFragmentParamsPtr(TCGIRBuilder& builder) const
    {
        return builder.ViaClosure(PassedFragmentParamsPtr_, "passedFragmentParamsPtr");
    }

    Value* CodegenExpr(
        TCGIRBuilder& builder,
        const TExpression* expr,
        const TTableSchema& schema,
        Value* row,
        Value* isNullPtr);

    Value* CodegenFunctionExpr(
        TCGIRBuilder& builder,
        const TFunctionExpression* expr,
        const TTableSchema& schema,
        Value* row,
        Value* isNullPtr);

    Value* CodegenBinaryOpExpr(
        TCGIRBuilder& builder,
        const TBinaryOpExpression* expr,
        const TTableSchema& schema,
        Value* row,
        Value* isNullPtr);

    void CodegenOp(
        TCGIRBuilder& builder,
        const TOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenScanOp(
        TCGIRBuilder& builder,
        const TScanOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenFilterOp(
        TCGIRBuilder& builder,
        const TFilterOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenProjectOp(
        TCGIRBuilder& builder,
        const TProjectOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenGroupOp(
        TCGIRBuilder& builder,
        const TGroupOperator* op,
        const TCodegenConsumer& codegenConsumer);

};

////////////////////////////////////////////////////////////////////////////////
// NULL helpers
//

// TODO(sandello): Wat?
Constant* CodegenNullValue(TCGIRBuilder& builder, Type* type)
{
    if (type->isIntegerTy()) {
        return ConstantInt::get(type, 0);
    } else if (type->isDoubleTy()) {
        return ConstantFP::get(type, 0);
    } else { // string
        return llvm::ConstantAggregateZero::get(TypeBuilder<TGCString, false>::get(builder.getContext()));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Row manipulation helpers
//

Value* CodegenValuesPtrFromRow(TCGIRBuilder& builder, Value* row)
{
    Value* headerPtr = builder.CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        "headerPtr");
    Value* valuesPtr = builder.CreatePointerCast(
        builder.CreateConstGEP1_32(headerPtr, 1, "valuesPtrUncasted"),
        TypeBuilder<TValue*, false>::get(builder.getContext()),
        "valuesPtr");

    return valuesPtr;
}

Value* CodegenDataPtrFromRow(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    EValueType type,
    Twine name = Twine())
{
    Value* valuesPtr = CodegenValuesPtrFromRow(builder, row);
    Value* dataPtr = builder.CreateConstGEP2_32(
        valuesPtr,
        index,
        TypeBuilder<TValue, false>::Fields::Data,
        name + ".dataPtrUncasted");

    typedef TypeBuilder<TValueData, false> TUnionType;

    TUnionType::Fields field;
    switch (type) {
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
        name + ".dataPtr");

    return dataPtr;
}

Value* CodegenTypePtrFromRow(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    Twine name = Twine())
{
    Value* valuesPtr = CodegenValuesPtrFromRow(builder, row);
    Value* typePtr = builder.CreateConstGEP2_32(
        valuesPtr,
        index,
        TypeBuilder<TValue, false>::Fields::Type,
        name + ".typePtr");

    return typePtr;
}

Value* CodegenLengthPtrFromRow(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    Twine name = Twine())
{
    Value* valuesPtr = CodegenValuesPtrFromRow(builder, row);
    Value* typePtr = builder.CreateConstGEP2_32(
        valuesPtr,
        index,
        TypeBuilder<TValue, false>::Fields::Length,
        name + ".lengthPtr");

    return typePtr;
}

void CodegenIsNull(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    Value* isNullPtr,
    Twine name = Twine())
{
    Value* typePtr = CodegenTypePtrFromRow(builder, row, index, name);
    Value* type = builder.CreateLoad(typePtr, name + ".type");

    Value* isNull = builder.CreateSelect(
        builder.CreateICmpEQ(type, builder.getInt16(EValueType::Null)),
        builder.getTrue(),
        builder.CreateLoad(isNullPtr),
        name + ".isNull");

    builder.CreateStore(isNull, isNullPtr);
}

// TODO(sandello): Think about deprecating this function.
Value* CodegenDataFromRow(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    EValueType type,
    Value* isNullPtr,
    Twine name = Twine())
{
    CodegenIsNull(builder, row, index, isNullPtr, name);

    auto* getterBB = builder.CreateBBHere(name + ".getter");
    auto* resultBB = builder.CreateBBHere(name + ".result");
    auto* sourceBB = builder.GetInsertBlock();

    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, getterBB);

    builder.SetInsertPoint(getterBB);

    Value* dataPtr = CodegenDataPtrFromRow(builder, row, index, type, name);
    Value* data = nullptr;

    if (type == EValueType::String) {
        Value* strValue = builder.CreateAlloca(TypeBuilder<TGCString, false>::get(builder.getContext()));
        Value* lengthPtr = CodegenLengthPtrFromRow(builder, row, index);

        builder.CreateStore(builder.CreateLoad(lengthPtr), builder.CreateConstGEP2_32(
            strValue,
            0,
            TypeBuilder<TGCString, false>::Fields::Length,
            "strLen"));

        builder.CreateStore(builder.CreateLoad(dataPtr), builder.CreateConstGEP2_32(
            strValue,
            0,
            TypeBuilder<TGCString, false>::Fields::Ptr,
            "strPtr"));

        data = builder.CreateLoad(strValue);
    } else {
        data = builder.CreateLoad(dataPtr);
    }

    builder.CreateBr(resultBB);
    getterBB = builder.GetInsertBlock();

    builder.SetInsertPoint(resultBB);

    Constant* null = CodegenNullValue(builder, data->getType());

    PHINode* phiNode = builder.CreatePHI(data->getType(), 2, name);
    phiNode->addIncoming(null, sourceBB);
    phiNode->addIncoming(data, getterBB);

    return phiNode;
}

Value* CodegenIdPtrFromRow(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    Twine name = Twine())
{
    Value* valuesPtr = CodegenValuesPtrFromRow(builder, row);
    Value* idPtr = builder.CreateConstGEP2_32(
        valuesPtr,
        index,
        TypeBuilder<TValue, false>::Fields::Id,
        name + ".idPtr");

    return idPtr;
}

void CodegenSetRowValue(
    TCGIRBuilder& builder,
    Value* row,
    int index,
    ui16 id,
    EValueType type,
    Value* data,
    Value* isNullPtr)
{
    Value* idPtr = CodegenIdPtrFromRow(builder, row, index);
    Value* typePtr = CodegenTypePtrFromRow(builder, row, index);
    
    builder.CreateStore(builder.getInt16(id), idPtr);
    builder.CreateStore(
        builder.CreateSelect(
            builder.CreateLoad(isNullPtr),
            builder.getInt16(EValueType::Null),
            builder.getInt16(type)),
        typePtr);

    Value* dataPtr = CodegenDataPtrFromRow(builder, row, index, type);

    if (type == EValueType::String) {
        Value* lengthPtr = CodegenLengthPtrFromRow(builder, row, index);

        builder.CreateStore(
            builder.CreateExtractValue(data, TypeBuilder<TGCString, false>::Fields::Length),
            lengthPtr);

        builder.CreateStore(
            builder.CreateExtractValue(data, TypeBuilder<TGCString, false>::Fields::Ptr),
            dataPtr);
    } else {
        builder.CreateStore(data, dataPtr);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Operator helpers
//

void CodegenAggregateFunction(
    TCGIRBuilder& builder,
    Value* aggregateRow,
    Value* newRow,
    EAggregateFunctions aggregateFunction,
    int index,
    EValueType type)
{
    // Check if a new data is null.
    Value* newDataIsNullPtr = builder.CreateAlloca(
        builder.getInt1Ty(),
        0,
        "newDataIsNullPtr");
    builder.CreateStore(builder.getFalse(), newDataIsNullPtr);

    auto* aggregateBeforeBB = builder.CreateBBHere("aggregate.before");
    auto* aggregateAfterBB = builder.CreateBBHere("aggregate.after");
    builder.CreateCondBr(
        builder.CreateLoad(newDataIsNullPtr),
        aggregateAfterBB,
        aggregateBeforeBB);

    // Check if an aggregated data is null.
    builder.SetInsertPoint(aggregateBeforeBB);

    Value* aggregateDataIsNullPtr = builder.CreateAlloca(
        builder.getInt1Ty(),
        0,
        "aggregateDataIsNullPtr");
    builder.CreateStore(builder.getFalse(), aggregateDataIsNullPtr);

    CodegenIsNull(builder, aggregateRow, index, aggregateDataIsNullPtr);

    Value* newData = CodegenDataFromRow(builder, newRow, index, type, newDataIsNullPtr);
    Value* aggregateDataPtr = CodegenDataPtrFromRow(builder, aggregateRow, index, type);

    auto* aggregateReplaceBB = builder.CreateBBHere("aggregate.replace");
    auto* aggregateUpdateBB = builder.CreateBBHere("aggregate.update");
    builder.CreateCondBr(
        builder.CreateLoad(aggregateDataIsNullPtr),
        aggregateReplaceBB,
        aggregateUpdateBB);

    // Replace aggregated result with a new one.
    builder.SetInsertPoint(aggregateReplaceBB);

    builder.CreateStore(newData, aggregateDataPtr);
    builder.CreateBr(aggregateAfterBB);

    // Update previously aggregated result with a new one.
    builder.SetInsertPoint(aggregateUpdateBB);

    Value* oldAggregateData = builder.CreateLoad(aggregateDataPtr);
    Value* newAggregateData = nullptr;

    switch (aggregateFunction) {
        case EAggregateFunctions::Sum:
            newAggregateData = builder.CreateBinOp(
                Instruction::BinaryOps::Add,
                oldAggregateData,
                newData);
            break;
        case EAggregateFunctions::Min:
            newAggregateData = builder.CreateSelect(
                builder.CreateICmpSLE(oldAggregateData, newData),
                oldAggregateData,
                newData);
            break;
        case EAggregateFunctions::Max:
            newAggregateData = builder.CreateSelect(
                builder.CreateICmpSGE(oldAggregateData, newData),
                oldAggregateData,
                newData);
            break;
        default:
            YUNIMPLEMENTED();
    }

    builder.CreateStore(newAggregateData, aggregateDataPtr);
    builder.CreateBr(aggregateAfterBB);

    builder.SetInsertPoint(aggregateAfterBB);
}

void CodegenForEachRow(
    TCGIRBuilder& builder,
    Value* rows,
    Value* size,
    const TCodegenConsumer& codegenConsumer)
{
    auto* loopBB = builder.CreateBBHere("loop");
    auto* condBB = builder.CreateBBHere("cond");
    auto* endloopBB = builder.CreateBBHere("endloop");

    // index = 0
    Value* indexPtr = builder.CreateAlloca(builder.getInt32Ty(), nullptr, "indexPtr");
    builder.CreateStore(builder.getInt32(0), indexPtr);
    builder.CreateBr(condBB);

    builder.SetInsertPoint(condBB);

    // if (index != size) ...
    Value* index = builder.CreateLoad(indexPtr, "index");
    builder.CreateCondBr(builder.CreateICmpNE(index, size), loopBB, endloopBB);

    builder.SetInsertPoint(loopBB);

    // row = rows[index]; consume(row);
    Value* row = builder.CreateLoad(builder.CreateGEP(rows, index, "rowPtr"), "row");
    codegenConsumer(builder, row);
    // index = index + 1
    builder.CreateStore(builder.CreateAdd(index, builder.getInt32(1)), indexPtr);
    builder.CreateBr(condBB);

    builder.SetInsertPoint(endloopBB);
}

////////////////////////////////////////////////////////////////////////////////
// Expressions
//

Value* TCGContext::CodegenFunctionExpr(
    TCGIRBuilder& builder,
    const TFunctionExpression* expr,
    const TTableSchema& schema,
    Value* row,
    Value* isNullPtr)
{
    YUNIMPLEMENTED();
}

Value* TCGContext::CodegenBinaryOpExpr(
    TCGIRBuilder& builder,
    const TBinaryOpExpression* expr,
    const TTableSchema& schema,
    Value* row,
    Value* isNullPtr)
{
    auto name = expr->GetName();
    auto type = expr->GetType(schema);

    auto* getLhsValueBB = builder.CreateBBHere(Twine(~name) + ".getLhs");
    auto* getRhsValueBB = builder.CreateBBHere(Twine(~name) + ".getRhs");
    auto* evalBB = builder.CreateBBHere(Twine(~name));
    auto* resultBB = builder.CreateBBHere(Twine(~name));

    builder.CreateBr(getLhsValueBB);

    // Evaluate LHS.
    builder.SetInsertPoint(getLhsValueBB);

    Value* lhsValue = CodegenExpr(builder, expr->GetLhs(), schema, row, isNullPtr);
    getLhsValueBB = builder.GetInsertBlock();
    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, getRhsValueBB);

    // Evaluate RHS if LHS is not null.
    builder.SetInsertPoint(getRhsValueBB);

    Value* rhsValue = CodegenExpr(builder, expr->GetRhs(), schema, row, isNullPtr);
    getRhsValueBB = builder.GetInsertBlock();
    builder.CreateCondBr(builder.CreateLoad(isNullPtr), resultBB, evalBB);

    // Evaluate expression if both sides are not null.
    builder.SetInsertPoint(evalBB);

    Value* result = nullptr;

    auto getCommonType = [] (Value* a, Value* b) -> Type* {
        auto* aTy = a->getType();
        auto* bTy = b->getType();
        YCHECK(aTy->isIntegerTy() && bTy->isIntegerTy());
        return aTy->getIntegerBitWidth() > bTy->getIntegerBitWidth() ? aTy : bTy;
    };

    switch (expr->GetOpcode()) {
        // Arithmetical operations.
#define XX(opcode, ioptype, foptype) \
        case EBinaryOp::opcode: \
            switch (type) { \
                case EValueType::Integer: \
                    result = builder.CreateBinOp(Instruction::BinaryOps::ioptype, lhsValue, rhsValue, ~name); \
                    break; \
                case EValueType::Double: \
                    result = builder.CreateBinOp(Instruction::BinaryOps::foptype, lhsValue, rhsValue, ~name); \
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

        // TODO(sandello): Remove integer casts after introducing boolean type.
        // Integral and logical operations.
#define XX(opcode, optype) \
        case EBinaryOp::opcode: \
            switch (type) { \
                case EValueType::Integer: { \
                    auto commonType = getCommonType(lhsValue, rhsValue); \
                    result = builder.CreateBinOp( \
                        Instruction::BinaryOps::optype, \
                        builder.CreateIntCast(lhsValue, commonType, true), \
                        builder.CreateIntCast(rhsValue, commonType, true), \
                        ~name); \
                    break; \
                } \
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
            switch (type) { \
                case EValueType::Integer: \
                    result = builder.CreateICmp##ioptype(lhsValue, rhsValue, ~name); \
                    break; \
                case EValueType::Double: \
                    result = builder.CreateFCmp##foptype(lhsValue, rhsValue, ~name); \
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

    PHINode* phiNode = builder.CreatePHI(result->getType(), 3);

    Value* nullValue = CodegenNullValue(builder, result->getType());

    phiNode->addIncoming(nullValue, getLhsValueBB);
    phiNode->addIncoming(nullValue, getRhsValueBB);
    phiNode->addIncoming(result, evalBB);

    return phiNode;
}

Value* TCGContext::CodegenExpr(
    TCGIRBuilder& builder,
    const TExpression* expr,
    const TTableSchema& schema,
    Value* row,
    Value* isNullPtr)
{
    YASSERT(expr);
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
        case EExpressionKind::DoubleLiteral: {
            auto it = Binding_.NodeToConstantIndex.find(expr);
            YCHECK(it != Binding_.NodeToConstantIndex.end());
            auto index = it->second;
            return CodegenDataFromRow(
                builder,
                GetConstantsRows(builder),
                index,
                expr->GetType(schema),
                isNullPtr,
                "literal");
        }
        case EExpressionKind::Reference: {
            auto column = expr->As<TReferenceExpression>()->GetColumnName();
            auto index = schema.GetColumnIndexOrThrow(column);
            return CodegenDataFromRow(
                builder,
                row,
                index,
                expr->GetType(schema),
                isNullPtr,
                Twine(~column) + ".reference");
        }
        case EExpressionKind::Function:
            return CodegenFunctionExpr(
                builder,
                expr->As<TFunctionExpression>(),
                schema,
                row,
                isNullPtr);
        case EExpressionKind::BinaryOp:
            return CodegenBinaryOpExpr(
                builder,
                expr->As<TBinaryOpExpression>(),
                schema,
                row,
                isNullPtr);
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void TCGContext::CodegenOp(
    TCGIRBuilder& builder,
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
    TCGIRBuilder& builder,
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
    Value* closure = args; closure->setName("closure");
    Value* rows = ++args; rows->setName("rows");
    Value* size = ++args; size->setName("size");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder innerBuilder(
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
    TCGIRBuilder& builder,
    const TFilterOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto sourceTableSchema = op->GetTableSchema();

    CodegenOp(builder, op->GetSource(),
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
            innerBuilder.CreateStore(builder.getFalse(), isNullPtr);

            Value* result = innerBuilder.CreateZExtOrBitCast(
                CodegenExpr(innerBuilder, op->GetPredicate(), sourceTableSchema, row, isNullPtr),
                builder.getInt64Ty());

            auto* ifBB = innerBuilder.CreateBBHere("if");
            auto* endifBB = innerBuilder.CreateBBHere("endif");

            innerBuilder.CreateCondBr(
                innerBuilder.CreateICmpNE(result, builder.getInt64(0)),
                ifBB,
                endifBB);

            innerBuilder.SetInsertPoint(ifBB);
            codegenConsumer(innerBuilder, row);
            innerBuilder.CreateBr(endifBB);

            innerBuilder.SetInsertPoint(endifBB);
    });
}

void TCGContext::CodegenProjectOp(
    TCGIRBuilder& builder,
    const TProjectOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    int projectionCount = op->GetProjectionCount();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    CodegenOp(builder, op->GetSource(),
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

            innerBuilder.CreateCall3(
                Fragment_.GetRoutine("AllocateRow"),
                GetPassedFragmentParamsPtr(innerBuilder),
                builder.getInt32(projectionCount),
                newRowPtr);

            Value* newRow = innerBuilder.CreateLoad(newRowPtr);

            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = op->GetProjection(index).Expression;
                const auto& name = op->GetProjection(index).Name;
                auto id = nameTable->GetId(name);
                auto type = expr->GetType(sourceTableSchema);

                Value* isNullPtr = innerBuilder.CreateAlloca(builder.getInt1Ty(), 0, "isNullPtr");
                innerBuilder.CreateStore(builder.getFalse(), isNullPtr);
                Value* data = CodegenExpr(innerBuilder, expr, sourceTableSchema, row, isNullPtr);
                CodegenSetRowValue(innerBuilder, newRow, index, id, type, data, isNullPtr);
            }

            codegenConsumer(innerBuilder, newRow);
    });
}

void TCGContext::CodegenGroupOp(
    TCGIRBuilder& builder,
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
    Value* closure = args; closure->setName("closure");
    Value* groupedRows = ++args; groupedRows->setName("groupedRows");
    Value* rows = ++args; rows->setName("rows");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder innerBuilder(
        BasicBlock::Create(builder.getContext(), "entry", function),
        &builder,
        closure);

    int keySize = op->GetGroupItemCount();
    int aggregateItemCount = op->AggregateItems().size();

    auto sourceTableSchema = op->GetSource()->GetTableSchema();
    auto nameTable = op->GetNameTable();

    Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

    CodegenOp(innerBuilder, op->GetSource(), [&] (TCGIRBuilder& innerBuilder, Value* row) {
        Value* passedFragmentParamsPtrRef = GetPassedFragmentParamsPtr(innerBuilder);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);

        innerBuilder.CreateCall3(
            Fragment_.GetRoutine("AllocateRow"),
            passedFragmentParamsPtrRef,
            builder.getInt32(keySize + aggregateItemCount),
            newRowPtrRef);

        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = op->GetGroupItem(index).Expression;
            const auto& name = op->GetGroupItem(index).Name;
            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

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

        auto* ifBB = innerBuilder.CreateBBHere("if");
        auto* elseBB = innerBuilder.CreateBBHere("else");
        auto* endIfBB = innerBuilder.CreateBBHere("endif");

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
            passedFragmentParamsPtrRef,
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
        TypeBuilder<TCodegenedFunctionSignature, false>::get(context),
        Function::ExternalLinkage,
        "Evaluate",
        module);

    auto args = function->arg_begin();
    Value* constants = args; constants->setName("constants");
    Value* passedFragmentParamsPtr = ++args; passedFragmentParamsPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(context, "entry", function));

    TCGContext ctx(cgFragment, binding, constants, passedFragmentParamsPtr);

    ctx.CodegenOp(builder, op,
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* passedFragmentParamsPtrRef = innerBuilder.ViaClosure(passedFragmentParamsPtr);
            innerBuilder.CreateCall2(cgFragment.GetRoutine("WriteRow"), row, passedFragmentParamsPtrRef);
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

