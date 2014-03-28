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
//  - Sometimes we can write through scratch space; some simple cases:
//    * int/double/null expressions only,
//    * string expressions with references (just need to copy string data)
//    It is possible to do better memory management here.
//  - TBAA is a king
//  - Capture pointers by value in ViaClosure

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;
using namespace NConcurrency;

// Import extensively used LLVM types.
using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::ConstantPointerNull;
using llvm::Function;
using llvm::FunctionType;
using llvm::Instruction;
using llvm::PHINode;
using llvm::PointerType;
using llvm::Twine;
using llvm::Type;
using llvm::TypeBuilder;
using llvm::Value;

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGIRBuilder& builder, Value* row)> TCodegenConsumer;

static Value* CodegenValuesPtrFromRow(TCGIRBuilder&, Value*);

class TCGValue
{
private:
    typedef TypeBuilder<TValue, false> TTypeBuilder;
    typedef TypeBuilder<TValueData, false> TDataTypeBuilder;

    TCGIRBuilder& Builder_;
    Value* Type_;
    Value* Length_;
    Value* Data_;
    std::string Name_;

    TCGValue(TCGIRBuilder& builder, Value* type, Value* length, Value* data, Twine name)
        : Builder_(builder)
        , Type_(type)
        , Length_(length)
        , Data_(data)
        , Name_(name.str())
    {
        if (Type_) {
            YCHECK(Type_->getType() == TTypeBuilder::getFor(TTypeBuilder::Type, Builder_.getContext()));
        }
        if (Length_) {
            YCHECK(Length_->getType() == TTypeBuilder::getFor(TTypeBuilder::Length, Builder_.getContext()));
        }
        if (Data_) {
            YCHECK(Data_->getType() == TTypeBuilder::getFor(TTypeBuilder::Data, Builder_.getContext()));
        }
    }

public:
    TCGValue(TCGValue&& other)
        : Builder_(other.Builder_)
        , Type_(other.Type_)
        , Length_(other.Length_)
        , Data_(other.Data_)
        , Name_(std::move(other.Name_))
    {
        other.Reset();
    }

    TCGValue& operator=(TCGValue&& other)
    {
        YCHECK(&Builder_ == &other.Builder_);

        Type_ = other.Type_;
        Length_ = other.Length_;
        Data_ = other.Data_;

        other.Reset();

        return *this;
    }

    TCGValue&& Steal()
    {
        return std::move(*this);
    }

    void Reset()
    {
        Type_ = nullptr;
        Length_ = nullptr;
        Data_ = nullptr;
    }

    static TCGValue CreateFromValue(
        TCGIRBuilder& builder,
        Value* type,
        Value* length,
        Value* data,
        Twine name = Twine())
    {
        return TCGValue(builder, type, length, data, name);
    }

    static TCGValue CreateFromRow(
        TCGIRBuilder& builder,
        Value* row,
        int index,
        Twine name = Twine())
    {
        auto valuePtr = builder.CreateConstInBoundsGEP1_32(
            CodegenValuesPtrFromRow(builder, row),
            index,
            name + ".valuePtr");
        auto type = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Type, name + ".typePtr"),
            name + ".type");
        auto length = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Length, name + ".lengthPtr"),
            name + ".length");
        auto data = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Data, name + ".dataPtr"),
            name + ".data");
        return TCGValue(builder, type, length, data, name);
    }

    static TCGValue CreateNull(
        TCGIRBuilder& builder,
        Twine name = Twine())
    {
        return TCGValue(
            builder,
            builder.getInt16(EValueType::Null),
            nullptr,
            nullptr,
            name);
    }

    void StoreToRow(Value* row, int index, ui16 id)
    {
        auto name = row->getName();
        auto nameTwine =
            (name.empty() ? Twine::createNull() : Twine(name).concat(".")) +
            Twine(".at.") +
            Twine(index);

        auto valuePtr = Builder_.CreateConstInBoundsGEP1_32(
            CodegenValuesPtrFromRow(Builder_, row),
            index,
            nameTwine);

        Builder_.CreateStore(
            Builder_.getInt16(id),
            Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Id, nameTwine + ".idPtr"));

        if (Type_) {
            Builder_.CreateStore(
                Type_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Type, nameTwine + ".typePtr"));
        }
        if (Length_) {
            Builder_.CreateStore(
                Length_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Length, nameTwine + ".lengthPtr"));
        }
        if (Data_) {
            Builder_.CreateStore(
                Data_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Data, nameTwine + ".dataPtr"));
        }
    }

    Value* IsNull()
    {
        // A little bit of manual constant folding.
        if (Type_ && llvm::isa<ConstantInt>(Type_)) {
            auto* constantType = llvm::cast<ConstantInt>(Type_);
            if (constantType->getZExtValue() == EValueType::Null) {
                return Builder_.getFalse();
            }
        }
        return Builder_.CreateICmpEQ(
            Type_,
            Builder_.getInt16(EValueType::Null),
            Twine(Name_) + ".isNull");
    }

    Value* GetType()
    {
        return Type_;
    }

    Value* GetData(EValueType type)
    {
        TDataTypeBuilder::Fields field;
        switch (type) {
            case EValueType::Integer:
                field = TDataTypeBuilder::Fields::Integer;
                break;
            case EValueType::Double:
                field = TDataTypeBuilder::Fields::Double;
                break;
            case EValueType::String:
                field = TDataTypeBuilder::Fields::String;
                break;
            default:
                YUNREACHABLE();
        }
        return Builder_.CreateBitCast(
            Data_,
            TDataTypeBuilder::getAs(field, Builder_.getContext()),
            Twine(Name_) + ".data");
    }

    TCGValue& SetType(EValueType type)
    {
        Type_ = Builder_.getInt16(type);
        return *this;
    }

    TCGValue& SetTypeIfNotNull(EValueType type)
    {
        Type_ = Builder_.CreateSelect(
            IsNull(),
            Builder_.getInt16(EValueType::Null),
            Builder_.getInt16(type));
        return *this;
    }

    TCGValue& SetData(Value* data)
    {
        Data_ = Builder_.CreateBitCast(
            data,
            TDataTypeBuilder::get(Builder_.getContext()),
            Twine(Name_) + ".data");
        return *this;
    }
};

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

    TCGValue CodegenExpr(
        TCGIRBuilder& builder,
        const TExpression* expr,
        const TTableSchema& schema,
        Value* row);

    TCGValue CodegenFunctionExpr(
        TCGIRBuilder& builder,
        const TFunctionExpression* expr,
        const TTableSchema& schema,
        Value* row);

    TCGValue CodegenBinaryOpExpr(
        TCGIRBuilder& builder,
        const TBinaryOpExpression* expr,
        const TTableSchema& schema,
        Value* row);

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
// Row manipulation helpers
//

static Value* CodegenValuesPtrFromRow(TCGIRBuilder& builder, Value* row)
{
    auto name = row->getName();
    auto namePrefix = name.empty() ? Twine::createNull() : Twine(name).concat(".");

    auto headerPtr = builder.CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        namePrefix + "headerPtr");
    auto valuesPtr = builder.CreatePointerCast(
        builder.CreateConstInBoundsGEP1_32(headerPtr, 1, "valuesPtrUncasted"),
        TypeBuilder<TValue*, false>::get(builder.getContext()),
        namePrefix + "valuesPtr");

    return valuesPtr;
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
    ui16 id,
    EValueType type,
    Twine name = Twine())
{
    auto* checkNewValueBB = builder.CreateBBHere(name + ".checkNewValue");
    auto* checkAggregateValueBB = builder.CreateBBHere(name + ".checkAggregateValue");
    auto* aggregateBB = builder.CreateBBHere(name + ".aggregate");
    auto* storeBB = builder.CreateBBHere(name + ".store");
    auto* endBB = builder.CreateBBHere(name + ".end");

    builder.CreateBr(checkNewValueBB);

    // Check if a new data is NULL.
    builder.SetInsertPoint(checkNewValueBB);

    auto newValue = TCGValue::CreateFromRow(builder, newRow, index, name + ".new");
    builder.CreateCondBr(newValue.IsNull(), endBB, checkAggregateValueBB);

    // Check if an aggregated data is NULL.
    builder.SetInsertPoint(checkAggregateValueBB);

    Value* newData = newValue.GetData(type);

    auto aggregateValue = TCGValue::CreateFromRow(builder, aggregateRow, index, name + ".aggregate");
    builder.CreateCondBr(aggregateValue.IsNull(), storeBB, aggregateBB);

    // Update previously aggregated result with a new one.
    builder.SetInsertPoint(aggregateBB);

    Value* aggregateData = aggregateValue.GetData(type);

    Value* resultData = nullptr;

    switch (aggregateFunction) {
        case EAggregateFunctions::Sum:
            resultData = builder.CreateAdd(
                aggregateData,
                newData);
            break;
        case EAggregateFunctions::Min:
            resultData = builder.CreateSelect(
                builder.CreateICmpSLE(aggregateData, newData),
                aggregateData,
                newData);
            break;
        case EAggregateFunctions::Max:
            resultData = builder.CreateSelect(
                builder.CreateICmpSGE(aggregateData, newData),
                aggregateData,
                newData);
            break;
        default:
            YUNIMPLEMENTED();
    }

    builder.CreateBr(storeBB);

    builder.SetInsertPoint(storeBB);

    PHINode* phiType = builder.CreatePHI(builder.getInt16Ty(), 2, name + ".phiType");
    phiType->addIncoming(newValue.GetType(), checkAggregateValueBB);
    phiType->addIncoming(builder.getInt16(type), aggregateBB);

    PHINode* phiData = builder.CreatePHI(builder.getInt64Ty(), 2, name + ".phiData");
    phiData->addIncoming(newData, checkAggregateValueBB);
    phiData->addIncoming(resultData, aggregateBB);

    TCGValue::CreateFromValue(builder, phiType, nullptr, phiData, name)
        .StoreToRow(aggregateRow, index, id);

    builder.CreateBr(endBB);

    builder.SetInsertPoint(endBB);
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
    Value* stackState = builder.CreateStackSave("stackState");
    Value* row = builder.CreateLoad(builder.CreateGEP(rows, index, "rowPtr"), "row");
    codegenConsumer(builder, row);
    builder.CreateStackRestore(stackState);
    // index = index + 1
    builder.CreateStore(builder.CreateAdd(index, builder.getInt32(1)), indexPtr);
    builder.CreateBr(condBB);

    builder.SetInsertPoint(endloopBB);
}

////////////////////////////////////////////////////////////////////////////////
// Expressions
//

TCGValue TCGContext::CodegenFunctionExpr(
    TCGIRBuilder& builder,
    const TFunctionExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    YUNIMPLEMENTED();
}

TCGValue TCGContext::CodegenBinaryOpExpr(
    TCGIRBuilder& builder,
    const TBinaryOpExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    auto name = "{" + expr->GetName() + "}";
    auto type = expr->GetType(schema);

    auto nameTwine = Twine(name.c_str());

    // Just to make sure. ;)
    YCHECK(expr->GetLhs()->GetType(schema) == type);
    YCHECK(expr->GetRhs()->GetType(schema) == type);

    auto* getLhsValueBB = builder.CreateBBHere(nameTwine + ".getLhsValue");
    auto* getRhsValueBB = builder.CreateBBHere(nameTwine + ".getRhsValue");
    auto* evalBB = builder.CreateBBHere(nameTwine + ".eval");
    auto* resultBB = builder.CreateBBHere(nameTwine + ".result");

    builder.CreateBr(getLhsValueBB);

    // Evaluate LHS.
    builder.SetInsertPoint(getLhsValueBB);

    auto lhsValue = CodegenExpr(builder, expr->GetLhs(), schema, row);
    builder.CreateCondBr(lhsValue.IsNull(), resultBB, getRhsValueBB);
    getLhsValueBB = builder.GetInsertBlock();

    // Evaluate RHS if LHS is not null.
    builder.SetInsertPoint(getRhsValueBB);

    auto rhsValue = CodegenExpr(builder, expr->GetRhs(), schema, row);
    builder.CreateCondBr(rhsValue.IsNull(), resultBB, evalBB);
    getRhsValueBB = builder.GetInsertBlock();

    // Evaluate expression if both sides are not null.
    builder.SetInsertPoint(evalBB);

    Value* lhsData = lhsValue.GetData(type);
    Value* rhsData = rhsValue.GetData(type);
    Value* evalData = nullptr;

    switch (expr->GetOpcode()) {
        // Arithmetical operations.
#define XX(opcode, ioptype, foptype) \
        case EBinaryOp::opcode: \
            switch (type) { \
                case EValueType::Integer: \
                    evalData = builder.Create##ioptype(lhsData, rhsData); \
                    break; \
                case EValueType::Double: \
                    evalData = builder.Create##foptype(lhsData, rhsData); \
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
            switch (type) { \
                case EValueType::Integer: \
                    evalData = builder.Create##optype(lhsData, rhsData); \
                    break; \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            break;
        XX(Modulo, SRem)
        XX(And, And)
        XX(Or, Or)
#undef XX

        // TODO(sandello): Remove zext after introducing boolean type.
        // Comparsion operations.
#define XX(opcode, ioptype, foptype) \
        case EBinaryOp::opcode: \
            switch (type) { \
                case EValueType::Integer: \
                    evalData = builder.CreateICmp##ioptype(lhsData, rhsData); \
                    break; \
                case EValueType::Double: \
                    evalData = builder.CreateFCmp##foptype(lhsData, rhsData); \
                    break; \
                default: \
                    YUNREACHABLE(); /* Typechecked. */ \
            } \
            evalData = builder.CreateZExtOrBitCast(evalData, builder.getInt64Ty()); \
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

    PHINode* phiType = builder.CreatePHI(builder.getInt16Ty(), 3, nameTwine + ".phiType");
    phiType->addIncoming(builder.getInt16(EValueType::Null), getLhsValueBB);
    phiType->addIncoming(builder.getInt16(EValueType::Null), getRhsValueBB);
    phiType->addIncoming(builder.getInt16(type), evalBB);

    PHINode* phiData = builder.CreatePHI(evalData->getType(), 3, nameTwine + ".phiData");
    phiData->addIncoming(llvm::UndefValue::get(evalData->getType()), getLhsValueBB);
    phiData->addIncoming(llvm::UndefValue::get(evalData->getType()), getRhsValueBB);
    phiData->addIncoming(evalData, evalBB);

    return TCGValue::CreateFromValue(builder, phiType, nullptr, phiData, nameTwine);
}

TCGValue TCGContext::CodegenExpr(
    TCGIRBuilder& builder,
    const TExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    YASSERT(expr);
    switch (expr->GetKind()) {
        case EExpressionKind::IntegerLiteral:
        case EExpressionKind::DoubleLiteral: {
            auto it = Binding_.NodeToConstantIndex.find(expr);
            YCHECK(it != Binding_.NodeToConstantIndex.end());
            auto index = it->second;
            return TCGValue::CreateFromRow(
                builder,
                GetConstantsRows(builder),
                index,
                "literal." + Twine(index))
                .SetType(expr->GetType(schema)) // Force type as constants are non-NULL.
                .Steal();
        }
        case EExpressionKind::Reference: {
            auto column = expr->As<TReferenceExpression>()->GetColumnName();
            auto index = schema.GetColumnIndexOrThrow(column);
            return TCGValue::CreateFromRow(
                builder,
                row,
                index,
                "reference." + Twine(column.c_str()));
        }
        case EExpressionKind::Function:
            return CodegenFunctionExpr(
                builder,
                expr->As<TFunctionExpression>(),
                schema,
                row);
        case EExpressionKind::BinaryOp:
            return CodegenBinaryOpExpr(
                builder,
                expr->As<TBinaryOpExpression>(),
                schema,
                row);
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
    auto sourceSchema = op->GetTableSchema();

    CodegenOp(builder, op->GetSource(),
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            auto predicateResult = CodegenExpr(
                innerBuilder,
                op->GetPredicate(),
                sourceSchema,
                row);

            Value* result = innerBuilder.CreateZExtOrBitCast(
                predicateResult.GetData(EValueType::Integer),
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

                CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                    .SetTypeIfNotNull(type)
                    .StoreToRow(newRow, index, id);
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

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .SetTypeIfNotNull(type)
                .StoreToRow(newRowRef, index, id);
        }

        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& item = op->GetAggregateItem(index);
            const auto& expr = item.Expression;
            const auto& name = item.Name;

            auto id = nameTable->GetId(name);
            auto type = expr->GetType(sourceTableSchema);

            // TODO(sandello): Others are unsupported.
            YCHECK(type == EValueType::Integer);

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .SetTypeIfNotNull(type)
                .StoreToRow(newRowRef, keySize + index, id);
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
            const auto& item = op->GetAggregateItem(index);
            const auto& name = item.Name;

            auto id = nameTable->GetId(name);
            auto type = item.Expression->GetType(sourceTableSchema);
            auto fn = item.AggregateFunction;

            CodegenAggregateFunction(innerBuilder, foundRow, newRowRef, fn, keySize + index, id, type, name.c_str());
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

