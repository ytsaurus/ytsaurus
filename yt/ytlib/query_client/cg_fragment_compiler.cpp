#include "stdafx.h"
#include "cg_fragment_compiler.h"
#include "private.h"

#include "cg_routines.h"
#include "cg_ir_builder.h"

#include <core/codegen/public.h>
#include <core/codegen/module.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>

#include <core/logging/log.h>

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

TCGValue MakePhi(
    TCGIRBuilder& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name = Twine())
{
    Value* thenNull = thenValue.IsNull();
    Value* thenLength = thenValue.GetLength();
    Value* thenData = thenValue.GetData();
    
    Value* elseNull = elseValue.IsNull();
    Value* elseLength = elseValue.GetLength();
    Value* elseData = elseValue.GetData();

    PHINode* phiNull = builder.CreatePHI(builder.getInt1Ty(), 2, name + ".phiNull");
    phiNull->addIncoming(thenNull, thenBB);
    phiNull->addIncoming(elseNull, elseBB);

    YCHECK(thenValue.GetStaticType() == elseValue.GetStaticType());
    EValueType type = thenValue.GetStaticType();
    YCHECK(thenData->getType() == elseData->getType());

    PHINode* phiData = builder.CreatePHI(thenData->getType(), 2, name + ".phiData");
    phiData->addIncoming(thenData, thenBB);
    phiData->addIncoming(elseData, elseBB);

    PHINode* phiLength = nullptr;
    if (type == EValueType::String) {
        YCHECK(thenLength->getType() == elseLength->getType());

        phiLength = builder.CreatePHI(thenLength->getType(), 2, name + ".phiLength");
        phiLength->addIncoming(thenLength, thenBB);
        phiLength->addIncoming(elseLength, elseBB);
    }

    return TCGValue::CreateFromValue(builder, phiNull, phiLength, phiData, type, name);
}

Value* MakePhi(
    TCGIRBuilder& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    Value* thenValue,
    Value* elseValue,
    Twine name = Twine())
{
    PHINode* phiValue = builder.CreatePHI(thenValue->getType(), 2, name + ".phiValue");
    phiValue->addIncoming(thenValue, thenBB);
    phiValue->addIncoming(elseValue, elseBB);
    return phiValue;
}

template <class TBuilder, class TResult>
TResult CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<TResult(TBuilder& builder)>& thenCodegen,
    const std::function<TResult(TBuilder& builder)>& elseCodegen,
    Twine name)
{
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateCondBr(condition, thenBB, elseBB);

    builder.SetInsertPoint(thenBB);
    auto thenValue = thenCodegen(builder);
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    auto elseValue = elseCodegen(builder);
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);

    return MakePhi(builder, thenBB, elseBB, thenValue, elseValue, name);
}

template <class TBuilder>
void CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<void(TBuilder& builder)>& thenCodegen,
    const std::function<void(TBuilder& builder)>& elseCodegen)
{
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateCondBr(condition, thenBB, elseBB);

    builder.SetInsertPoint(thenBB);
    thenCodegen(builder);
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    elseCodegen(builder);
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);
}

////////////////////////////////////////////////////////////////////////////////
// Row manipulation helpers
//

Value* CodegenValuesPtrFromRow(TCGIRBuilder& builder, Value* row)
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

void CodegenForEachRow(
    TCGContext& builder,
    Value* rows,
    Value* size,
    Value* stopFlag,
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
    Value* condition = builder.CreateAnd(
        builder.CreateICmpNE(index, size),
        builder.CreateICmpEQ(
            builder.CreateLoad(stopFlag, "stopFlag"),
            builder.getInt8(0)));
    builder.CreateCondBr(condition, loopBB, endloopBB);

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

Function* CodegenGroupComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<char(TRow, TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "GroupComparer",
        llvmModule);

    auto args = function->arg_begin();
    Value* lhsRow = args; lhsRow->setName("lhsRow");
    Value* rhsRow = ++args; rhsRow->setName("rhsRow");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto returnIf = [&] (Value* condition) {
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        builder.CreateCondBr(condition, thenBB, elseBB);
        builder.SetInsertPoint(thenBB);
        builder.CreateRet(builder.getInt8(0));
        builder.SetInsertPoint(elseBB);
    };

    auto codegenEqualOp = [&] (size_t index) {
        auto lhsValue = TCGValue::CreateFromRow(
            builder,
            lhsRow,
            index,
            types[index]);

        auto rhsValue = TCGValue::CreateFromRow(
            builder,
            rhsRow,
            index,
            types[index]);

        CodegenIf<TCGIRBuilder>(
            builder,
            builder.CreateOr(lhsValue.IsNull(), rhsValue.IsNull()),
            [&] (TCGIRBuilder& builder) {
                returnIf(builder.CreateICmpNE(lhsValue.IsNull(), rhsValue.IsNull()));
            },
            [&] (TCGIRBuilder& builder) {
                auto* lhsData = lhsValue.GetData();
                auto* rhsData = rhsValue.GetData();

                switch (types[index]) {
                    case EValueType::Boolean:
                    case EValueType::Int64:
                    case EValueType::Uint64:
                        returnIf(builder.CreateICmpNE(lhsData, rhsData));
                        break;

                    case EValueType::Double:
                        returnIf(builder.CreateFCmpUNE(lhsData, rhsData));
                        break;

                    case EValueType::String: {
                        Value* lhsLength = lhsValue.GetLength();
                        Value* rhsLength = rhsValue.GetLength();

                        Value* minLength = builder.CreateSelect(
                            builder.CreateICmpULT(lhsLength, rhsLength),
                            lhsLength,
                            rhsLength);

                        Value* cmpResult = builder.CreateCall3(
                            module.GetRoutine("memcmp"),
                            lhsData,
                            rhsData,
                            builder.CreateZExt(minLength, builder.getSizeType()));

                        returnIf(builder.CreateOr(
                            builder.CreateICmpNE(cmpResult, builder.getInt32(0)),
                            builder.CreateICmpNE(lhsLength, rhsLength)));
                        break;
                    }

                    default:
                        YUNREACHABLE();
                }
            });
    };

    YCHECK(!types.empty());

    for (size_t index = 0; index < types.size(); ++index) {
        codegenEqualOp(index);
    }

    builder.CreateRet(builder.getInt8(1));

    return function;
}

Function* CodegenGroupHasherFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<ui64(TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "GroupHasher",
        llvmModule);

    auto args = function->arg_begin();
    Value* row = args; row->setName("row");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto codegenHashOp = [&] (size_t index, TCGIRBuilder& builder) -> Value* {
        auto value = TCGValue::CreateFromRow(
            builder,
            row,
            index,
            types[index]);

        auto* conditionBB = builder.CreateBBHere("condition");
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        auto* endBB = builder.CreateBBHere("end");

        builder.CreateBr(conditionBB);

        builder.SetInsertPoint(conditionBB);
        builder.CreateCondBr(value.IsNull(), elseBB, thenBB);
        conditionBB = builder.GetInsertBlock();

        builder.SetInsertPoint(thenBB);

        Value* thenResult;

        switch (value.GetStaticType()) {
            case EValueType::Boolean:
            case EValueType::Int64:
            case EValueType::Uint64:
                thenResult = builder.CreateCall(
                    module.GetRoutine("FarmHashUint64"),
                    value.Cast(builder, EValueType::Uint64).GetData());
                break;

            case EValueType::Double:
                thenResult = builder.CreateCall(
                    module.GetRoutine("FarmHashUint64"),
                    value.Cast(builder, EValueType::Uint64, true).GetData());
                break;

            case EValueType::String:
                thenResult = builder.CreateCall2(
                    module.GetRoutine("StringHash"),
                    value.GetData(),
                    value.GetLength());
                break;

            default:
                YUNIMPLEMENTED();
        }

        builder.CreateBr(endBB);
        thenBB = builder.GetInsertBlock();

        builder.SetInsertPoint(elseBB);
        auto* elseResult = builder.getInt64(0);
        builder.CreateBr(endBB);
        elseBB = builder.GetInsertBlock();

        builder.SetInsertPoint(endBB);

        PHINode* result = builder.CreatePHI(thenResult->getType(), 2);
        result->addIncoming(thenResult, thenBB);
        result->addIncoming(elseResult, elseBB);

        return result;
    };

    auto codegenHashCombine = [&] (TCGIRBuilder& builder, Value* first, Value* second) -> Value* {
        //first ^ (second + 0x9e3779b9 + (second << 6) + (second >> 2));
        return builder.CreateXor(
            first,
            builder.CreateAdd(
                builder.CreateAdd(
                    builder.CreateAdd(second, builder.getInt64(0x9e3779b9)),
                    builder.CreateLShr(second, builder.getInt64(2))),
                builder.CreateShl(second, builder.getInt64(6))));
    };

    YCHECK(!types.empty());
    Value* result = builder.getInt64(0);
    for (size_t index = 0; index < types.size(); ++index) {
        result = codegenHashCombine(builder, result, codegenHashOp(index, builder));
    }
    builder.CreateRet(result);

    return function;
}

Function* CodegenTupleComparerFunction(
    const std::vector<std::function<TCGValue(TCGIRBuilder& builder, Value* row)>>& codegenArgs,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<char(TRow, TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "RowComparer",
        llvmModule);

    auto args = function->arg_begin();
    Value* lhsRow = args; lhsRow->setName("lhsRow");
    Value* rhsRow = ++args; rhsRow->setName("rhsRow");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto returnIf = [&] (Value* condition, const TCodegenBlock& codegenInner) {
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        builder.CreateCondBr(condition, thenBB, elseBB);
        builder.SetInsertPoint(thenBB);
        builder.CreateRet(builder.CreateSelect(codegenInner(builder), builder.getInt8(1), builder.getInt8(0)));
        builder.SetInsertPoint(elseBB);
    };

    auto codegenEqualOrLessOp = [&] (int index) {
        const auto& codegenArg = codegenArgs[index];
        auto lhsValue = codegenArg(builder, lhsRow);
        auto rhsValue = codegenArg(builder, rhsRow);

        auto type = lhsValue.GetStaticType();

        YCHECK(type == rhsValue.GetStaticType());

        CodegenIf<TCGIRBuilder>(
            builder,
            builder.CreateOr(lhsValue.IsNull(), rhsValue.IsNull()),
            [&] (TCGIRBuilder& builder) {
                returnIf(
                    builder.CreateICmpNE(lhsValue.IsNull(), rhsValue.IsNull()),
                    [&] (TCGIRBuilder&) {
                        return builder.CreateICmpULT(lhsValue.IsNull(), rhsValue.IsNull());
                    });
            },
            [&] (TCGIRBuilder& builder) {
                auto* lhsData = lhsValue.GetData();
                auto* rhsData = rhsValue.GetData();

                switch (type) {
                    case EValueType::Boolean:
                    case EValueType::Int64:
                        returnIf(
                            builder.CreateICmpNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpSLT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::Uint64:
                        returnIf(
                            builder.CreateICmpNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpULT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::Double:
                        returnIf(
                            builder.CreateFCmpUNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateFCmpULT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::String: {
                        Value* lhsLength = lhsValue.GetLength();
                        Value* rhsLength = rhsValue.GetLength();

                        Value* minLength = builder.CreateSelect(
                            builder.CreateICmpULT(lhsLength, rhsLength),
                            lhsLength,
                            rhsLength);
                    
                        Value* cmpResult = builder.CreateCall3(
                            module.GetRoutine("memcmp"),
                            lhsData,
                            rhsData,
                            builder.CreateZExt(minLength, builder.getSizeType()));

                        returnIf(
                            builder.CreateICmpNE(cmpResult, builder.getInt32(0)),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpSLT(cmpResult, builder.getInt32(0));
                            });

                        returnIf(
                            builder.CreateICmpNE(lhsLength, rhsLength),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpULT(lhsLength, rhsLength);
                            });

                        break;
                    }

                    default:
                        YUNREACHABLE();
                }
            });
    };

    YCHECK(!codegenArgs.empty());

    for (int index = 0; index < codegenArgs.size(); ++index) {
        codegenEqualOrLessOp(index);
    }

    builder.CreateRet(builder.getInt8(0));

    return function;
}

Function* CodegenRowComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    std::vector<std::function<TCGValue(TCGIRBuilder& builder, Value* row)>> compareArgs;
    for (int index = 0; index < types.size(); ++index) {
        compareArgs.push_back([index, type = types[index]] (TCGIRBuilder& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                row,
                index,
                type);
        });
    }

    return CodegenTupleComparerFunction(compareArgs, module);
}

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    EValueType type)
{
    return [
            index,
            type
        ] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                builder.GetConstantsRows(),
                index,
                type,
                "literal." + Twine(index))
                .Steal();
        };
}

TCodegenExpression MakeCodegenReferenceExpr(
    int index,
    EValueType type,
    Stroka name)
{
    return [
            index,
            type,
            MOVE(name)
        ] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                row,
                index,
                type,
                "reference." + Twine(name.c_str()));
        };
}

TCGValue MakeBinaryFunctionCall(
    Stroka routineName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name,
    TCGContext& builder,
    Value* row)
{
    auto nameTwine = Twine(name.c_str());
        YCHECK(codegenArgs.size() == 2);
        auto lhsValue = codegenArgs[0](builder, row);
        YCHECK(lhsValue.GetStaticType() == EValueType::String);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            lhsValue.IsNull(),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                auto rhsValue = codegenArgs[1](builder, row);
                YCHECK(rhsValue.GetStaticType() == EValueType::String);

                return CodegenIf<TCGContext, TCGValue>(
                    builder,
                    rhsValue.IsNull(),
                    [&] (TCGContext& builder) {
                        return TCGValue::CreateNull(builder, type);
                    },
                    [&] (TCGContext& builder) {
                        Value* lhsData = lhsValue.GetData();
                        Value* lhsLength = lhsValue.GetLength();
                        Value* rhsData = rhsValue.GetData();
                        Value* rhsLength = rhsValue.GetLength();

                        Value* result = builder.CreateCall4(
                            builder.Module->GetRoutine(routineName),
                            lhsData, lhsLength, rhsData, rhsLength);

                        return TCGValue::CreateFromValue(
                            builder,
                            builder.getFalse(),
                            nullptr,
                            result,
                            type);
                    });
            },
            nameTwine);
}

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    Stroka name)
{
    return [
        MOVE(opcode),
        MOVE(codegenOperand),
        MOVE(type),
        MOVE(name)
    ] (TCGContext& builder, Value* row) {
        auto operandValue = codegenOperand(builder, row);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            operandValue.IsNull(),
            [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGIRBuilder& builder) {
                auto operandType = operandValue.GetStaticType();
                Value* operandData = operandValue.GetData();
                Value* evalData = nullptr;

                switch(opcode) {
                    case EUnaryOp::Plus:
                        evalData = operandData;
                        break;

                    case EUnaryOp::Minus:
                        switch (operandType) {
                            case EValueType::Int64:
                            case EValueType::Uint64:
                                evalData = builder.CreateSub(builder.getInt64(0), operandData);
                                break;
                            case EValueType::Double:
                                evalData = builder.CreateFSub(ConstantFP::get(builder.getDoubleTy(), 0.0), operandData);
                                break;
                            default:
                                YUNREACHABLE();
                        }
                        break;

                    case EUnaryOp::Not:
                        evalData = builder.CreateXor(
                            builder.CreateZExtOrBitCast(
                                builder.getTrue(),
                                TDataTypeBuilder::TBoolean::get(builder.getContext())),
                            operandData);
                        break;

                    default:
                        YUNREACHABLE();
                }

                return TCGValue::CreateFromValue(
                    builder,
                    builder.getFalse(),
                    nullptr,
                    evalData,
                    type);
            },
            Twine(name.c_str()));
    };
}

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    Stroka name)
{

    if (IsRelationalBinaryOp(opcode))
    {
        return [
            MOVE(opcode),
            MOVE(codegenLhs),
            MOVE(codegenRhs),
            MOVE(type),
            MOVE(name)
        ] (TCGContext& builder, Value* row) {
            auto nameTwine = Twine(name.c_str());
            auto lhsValue = codegenLhs(builder, row);
            auto rhsValue = codegenRhs(builder, row);

            #define CMP_OP(opcode, optype) \
                case EBinaryOp::opcode: \
                    evalData = builder.CreateZExtOrBitCast( \
                        builder.Create##optype(lhsData, rhsData), \
                        TDataTypeBuilder::TBoolean::get(builder.getContext())); \
                    break;

            auto compareNulls = [&] () {
                Value* lhsData = lhsValue.IsNull();
                Value* rhsData = rhsValue.IsNull();
                Value* evalData = nullptr;

                switch (opcode) {
                    CMP_OP(Equal, ICmpEQ)
                    CMP_OP(NotEqual, ICmpNE)
                    CMP_OP(Less, ICmpSLT)
                    CMP_OP(LessOrEqual, ICmpSLE)
                    CMP_OP(Greater, ICmpSGT)
                    CMP_OP(GreaterOrEqual, ICmpSGE)
                    default:
                        YUNREACHABLE();
                }

                return TCGValue::CreateFromValue(
                    builder,
                    builder.getFalse(),
                    nullptr,
                    evalData,
                    type);
            };

            return CodegenIf<TCGContext, TCGValue>(
                builder,
                lhsValue.IsNull(),
                [&] (TCGContext& builder) {

                    return compareNulls();
                },
                [&] (TCGContext& builder) {

                    return CodegenIf<TCGContext, TCGValue>(
                        builder,
                        rhsValue.IsNull(),
                        [&] (TCGContext& builder) {

                            return compareNulls();
                        },
                        [&] (TCGContext& builder) {

                            YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
                            auto operandType = lhsValue.GetStaticType();

                            Value* lhsData = lhsValue.GetData();
                            Value* rhsData = rhsValue.GetData();
                            Value* evalData = nullptr;

                            switch (operandType) {

                                case EValueType::Boolean:
                                case EValueType::Int64:
                                    switch (opcode) {
                                        CMP_OP(Equal, ICmpEQ)
                                        CMP_OP(NotEqual, ICmpNE)
                                        CMP_OP(Less, ICmpSLT)
                                        CMP_OP(LessOrEqual, ICmpSLE)
                                        CMP_OP(Greater, ICmpSGT)
                                        CMP_OP(GreaterOrEqual, ICmpSGE)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                case EValueType::Uint64:
                                    switch (opcode) {
                                        CMP_OP(Equal, ICmpEQ)
                                        CMP_OP(NotEqual, ICmpNE)
                                        CMP_OP(Less, ICmpULT)
                                        CMP_OP(LessOrEqual, ICmpULE)
                                        CMP_OP(Greater, ICmpUGT)
                                        CMP_OP(GreaterOrEqual, ICmpUGE)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                case EValueType::Double:
                                    switch (opcode) {
                                        CMP_OP(Equal, FCmpUEQ)
                                        CMP_OP(NotEqual, FCmpUNE)
                                        CMP_OP(Less, FCmpULT)
                                        CMP_OP(LessOrEqual, FCmpULE)
                                        CMP_OP(Greater, FCmpUGT)
                                        CMP_OP(GreaterOrEqual, FCmpUGE)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                case EValueType::String: {
                                    Value* lhsLength = lhsValue.GetLength();
                                    Value* rhsLength = rhsValue.GetLength();

                                    auto codegenEqual = [&] () {
                                        return CodegenIf<TCGContext, Value*>(
                                            builder,
                                            builder.CreateICmpEQ(lhsLength, rhsLength),
                                            [&] (TCGContext& builder) {
                                                Value* minLength = builder.CreateSelect(
                                                    builder.CreateICmpULT(lhsLength, rhsLength),
                                                    lhsLength,
                                                    rhsLength);

                                                Value* cmpResult = builder.CreateCall3(
                                                    builder.Module->GetRoutine("memcmp"),
                                                    lhsData,
                                                    rhsData,
                                                    builder.CreateZExt(minLength, builder.getSizeType()));

                                                return builder.CreateICmpEQ(cmpResult, builder.getInt32(0));
                                            },
                                            [&] (TCGContext& builder) {
                                                return builder.getFalse();
                                            });
                                    };

                                    auto codegenLexicographicalCompare = [&] (Value* lhsData, Value* lhsLength, Value* rhsData, Value* rhsLength) {
                                        Value* lhsLengthIsLess = builder.CreateICmpULT(lhsLength, rhsLength);
                                        Value* minLength = builder.CreateSelect(
                                            lhsLengthIsLess,
                                            lhsLength,
                                            rhsLength);

                                        Value* cmpResult = builder.CreateCall3(
                                            builder.Module->GetRoutine("memcmp"),
                                            lhsData,
                                            rhsData,
                                            builder.CreateZExt(minLength, builder.getSizeType()));

                                        return builder.CreateOr(
                                            builder.CreateICmpSLT(cmpResult, builder.getInt32(0)),
                                            builder.CreateAnd(
                                                builder.CreateICmpEQ(cmpResult, builder.getInt32(0)),
                                                lhsLengthIsLess));
                                    };

                                    switch (opcode) {
                                        case EBinaryOp::Equal:
                                            evalData = codegenEqual();
                                            break;
                                        case EBinaryOp::NotEqual:
                                            evalData = builder.CreateNot(codegenEqual());
                                            break;
                                        case EBinaryOp::Less:
                                            evalData = codegenLexicographicalCompare(lhsData, lhsLength, rhsData, rhsLength);
                                            break;
                                        case EBinaryOp::Greater:
                                            evalData = codegenLexicographicalCompare(rhsData, rhsLength, lhsData, lhsLength);
                                            break;
                                        case EBinaryOp::LessOrEqual:
                                            evalData =  builder.CreateNot(
                                                codegenLexicographicalCompare(rhsData, rhsLength, lhsData, lhsLength));
                                            break;
                                        case EBinaryOp::GreaterOrEqual:
                                            evalData = builder.CreateNot(
                                                codegenLexicographicalCompare(lhsData, lhsLength, rhsData, rhsLength));
                                            break;
                                        default:
                                            YUNREACHABLE();
                                    }

                                    evalData = builder.CreateZExtOrBitCast(
                                        evalData,
                                        TDataTypeBuilder::TBoolean::get(builder.getContext()));
                                    break;
                                }
                                default:
                                    YUNREACHABLE();
                            }

                            return TCGValue::CreateFromValue(
                                builder,
                                builder.getFalse(),
                                nullptr,
                                evalData,
                                type);
                        });
                },
                nameTwine);

                #undef CMP_OP
        };

    } else {
        return [
            MOVE(opcode),
            MOVE(codegenLhs),
            MOVE(codegenRhs),
            MOVE(type),
            MOVE(name)
        ] (TCGContext& builder, Value* row) {
            auto nameTwine = Twine(name.c_str());

            auto lhsValue = codegenLhs(builder, row);

            return CodegenIf<TCGContext, TCGValue>(
                builder,
                lhsValue.IsNull(),
                [&] (TCGContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                [&] (TCGContext& builder) {
                    auto rhsValue = codegenRhs(builder, row);

                    return CodegenIf<TCGContext, TCGValue>(
                        builder,
                        rhsValue.IsNull(),
                        [&] (TCGContext& builder) {
                            return TCGValue::CreateNull(builder, type);
                        },
                        [&] (TCGContext& builder) {
                            YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
                            auto operandType = lhsValue.GetStaticType();

                            Value* lhsData = lhsValue.GetData();
                            Value* rhsData = rhsValue.GetData();
                            Value* evalData = nullptr;

                            #define OP(opcode, optype) \
                                case EBinaryOp::opcode: \
                                    evalData = builder.Create##optype(lhsData, rhsData); \
                                    break;

                            switch (operandType) {

                                case EValueType::Boolean:
                                case EValueType::Int64:
                                    switch (opcode) {
                                        OP(Plus, Add)
                                        OP(Minus, Sub)
                                        OP(Multiply, Mul)
                                        OP(Divide, SDiv)
                                        OP(Modulo, SRem)
                                        OP(And, And)
                                        OP(Or, Or)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                case EValueType::Uint64:
                                    switch (opcode) {
                                        OP(Plus, Add)
                                        OP(Minus, Sub)
                                        OP(Multiply, Mul)
                                        OP(Divide, UDiv)
                                        OP(Modulo, URem)
                                        OP(And, And)
                                        OP(Or, Or)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                case EValueType::Double:
                                    switch (opcode) {
                                        OP(Plus, FAdd)
                                        OP(Minus, FSub)
                                        OP(Multiply, FMul)
                                        OP(Divide, FDiv)
                                        default:
                                            YUNREACHABLE();
                                    }
                                    break;
                                default:
                                    YUNREACHABLE();
                            }

                            #undef OP

                            return TCGValue::CreateFromValue(
                                builder,
                                builder.getFalse(),
                                nullptr,
                                evalData,
                                type);
                        });
               },
                nameTwine);
        };
    }
}

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex)
{
    return [
        MOVE(codegenArgs),
        MOVE(arrayIndex)
    ] (TCGContext& builder, Value* row) {
        size_t keySize = codegenArgs.size();

        Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));
        Value* executionContextPtrRef = builder.GetExecutionContextPtr();

        builder.CreateCall3(
            builder.Module->GetRoutine("AllocateRow"),
            executionContextPtrRef,
            builder.getInt32(keySize),
            newRowPtr);

        Value* newRowRef = builder.CreateLoad(newRowPtr);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto id = index;
            auto value = codegenArgs[index](builder, row);
            keyTypes.push_back(value.GetStaticType());        
            value.StoreToRow(builder, newRowRef, index, id);
        }

        Value* result = builder.CreateCall4(
            builder.Module->GetRoutine("IsRowInArray"),
            executionContextPtrRef,
            CodegenRowComparerFunction(keyTypes, *builder.Module),
            newRowRef,
            builder.getInt32(arrayIndex));

        return TCGValue::CreateFromValue(
            builder,
            builder.getFalse(),
            nullptr,
            result,
            EValueType::Boolean);
    };
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void CodegenScanOp(
    TCGContext& builder,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = builder.Module->GetModule();

    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int, char*), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "ScanOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args; closure->setName("closure");
    Value* rows = ++args; rows->setName("rows");
    Value* size = ++args; size->setName("size");
    Value* stopFlag = ++args; stopFlag->setName("stopFlag");
    YCHECK(++args == function->arg_end());

    TCGContext innerBuilder(
        function,
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, stopFlag, codegenConsumer);

    innerBuilder.CreateRetVoid();

    int dataSplitsIndex = 0;

    builder.CreateCall4(
        builder.Module->GetRoutine("ScanOpHelper"),
        builder.GetExecutionContextPtr(),
        builder.getInt32(dataSplitsIndex),
        innerBuilder.GetClosure(),
        function);
}

TCodegenSource MakeCodegenJoinOp(
    std::vector<Stroka> joinColumns,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource)
{
    return [
        MOVE(joinColumns),
        MOVE(sourceSchema),
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        auto module = builder.Module->GetModule();

        // See JoinOpHelper.
        Function* collectRows = Function::Create(
            TypeBuilder<void(void**, void*, void*, void*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "CollectRows",
            module);

        auto collectRowsArgs = collectRows->arg_begin();
        Value* closure = collectRowsArgs; closure->setName("closure");
        Value* keys = ++collectRowsArgs; keys->setName("keys");
        Value* keysLookup = ++collectRowsArgs; keysLookup->setName("keysLookup");
        Value* allRows = ++collectRowsArgs; allRows->setName("allRows");
        YCHECK(++collectRowsArgs == collectRows->arg_end());

        TCGContext collectBuilder(
            collectRows,
            &builder,
            closure);

        int joinKeySize = joinColumns.size();

        Value* newRowPtr = collectBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

        collectBuilder.CreateCall3(
            builder.Module->GetRoutine("AllocatePermanentRow"),
            collectBuilder.GetExecutionContextPtr(),
            builder.getInt32(joinKeySize),
            newRowPtr);

        codegenSource(
            collectBuilder,
            [&] (TCGContext& builder, Value* row) {
                Value* executionContextPtrRef = builder.GetExecutionContextPtr();
                Value* keysRef = builder.ViaClosure(keys);
                Value* allRowsRef = builder.ViaClosure(allRows);
                Value* keysLookupRef = builder.ViaClosure(keysLookup);
                Value* newRowPtrRef = builder.ViaClosure(newRowPtr);
                Value* newRowRef = builder.CreateLoad(newRowPtrRef);

                builder.CreateCall3(
                    builder.Module->GetRoutine("SaveJoinRow"),
                    executionContextPtrRef,
                    allRowsRef,
                    row);

                for (int index = 0; index < joinKeySize; ++index) {
                    auto id = index;

                    auto columnName = joinColumns[index];
                    auto columnIndex = sourceSchema.GetColumnIndexOrThrow(columnName);
                    auto column = sourceSchema.Columns()[columnIndex];
                    
                    TCGValue::CreateFromRow(
                        builder,
                        row,
                        columnIndex,
                        column.Type,
                        "reference." + Twine(columnName.c_str()))
                        .StoreToRow(builder, newRowRef, index, id);                
                }

                // Add row to rows and lookup;

                builder.CreateCall5(
                    builder.Module->GetRoutine("InsertJoinRow"),
                    executionContextPtrRef,
                    keysLookupRef,
                    keysRef,
                    newRowPtrRef,
                    builder.getInt32(joinKeySize));

            });

        collectBuilder.CreateRetVoid();

        // See JoinOpHelper.
        Function* consumeJoinedRows = Function::Create(
            TypeBuilder<void(void**, void*, char*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "ConsumeJoinedRows",
            module);

        auto consumeJoinedRowsArgs = consumeJoinedRows->arg_begin();
        Value* consumeClosure = consumeJoinedRowsArgs; consumeClosure->setName("consumeClosure");
        Value* joinedRows = ++consumeJoinedRowsArgs; joinedRows->setName("joinedRows");
        Value* stopFlag = ++consumeJoinedRowsArgs; stopFlag->setName("stopFlag");
        YCHECK(++consumeJoinedRowsArgs == consumeJoinedRows->arg_end());

        TCGContext consumeBuilder(
            consumeJoinedRows,
            &builder,
            consumeClosure);

        CodegenForEachRow(
            consumeBuilder,
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsData"), joinedRows),
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsSize"), joinedRows),
            stopFlag,
            codegenConsumer);

        consumeBuilder.CreateRetVoid();
 
        std::vector<EValueType> keyTypes;
        for (int index = 0; index < joinColumns.size(); ++index) {
            keyTypes.push_back(sourceSchema.FindColumn(joinColumns[index])->Type);
        }

        builder.CreateCallWithArgs(
            builder.Module->GetRoutine("JoinOpHelper"),
            {
                builder.GetExecutionContextPtr(),
                CodegenGroupHasherFunction(keyTypes, *builder.Module),
                CodegenGroupComparerFunction(keyTypes, *builder.Module),

                collectBuilder.GetClosure(),
                collectRows,

                consumeBuilder.GetClosure(),
                consumeJoinedRows
            });
    };
}

TCodegenSource MakeCodegenFilterOp(
    TCodegenExpression codegenPredicate,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenPredicate), 
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        codegenSource(
            builder,
            [&] (TCGContext& builder, Value* row) {
                auto predicateResult = codegenPredicate(builder, row);

                Value* result = builder.CreateZExtOrBitCast(
                    predicateResult.GetData(),
                    builder.getInt64Ty());

                auto* ifBB = builder.CreateBBHere("if");
                auto* endifBB = builder.CreateBBHere("endif");

                builder.CreateCondBr(
                    builder.CreateICmpNE(result, builder.getInt64(0)),
                    ifBB,
                    endifBB);

                builder.SetInsertPoint(ifBB);
                codegenConsumer(builder, row);
                builder.CreateBr(endifBB);

                builder.SetInsertPoint(endifBB);
            });
    };
}

TCodegenSource MakeCodegenProjectOp(
    std::vector<TCodegenExpression> codegenArgs,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenArgs),
        codegenSource = std::move(codegenSource)        
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        int projectionCount = codegenArgs.size();

        codegenSource(
            builder,
            [&] (TCGContext& builder, Value* row) {
                Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

                builder.CreateCall3(
                    builder.Module->GetRoutine("AllocateRow"),
                    builder.GetExecutionContextPtr(),
                    builder.getInt32(projectionCount),
                    newRowPtr);

                Value* newRow = builder.CreateLoad(newRowPtr);

                for (int index = 0; index < projectionCount; ++index) {
                    auto id = index;
                
                    codegenArgs[index](builder, row)
                        .StoreToRow(builder, newRow, index, id);
                }

                codegenConsumer(builder, newRow);
            });
    };
}

TCodegenSource MakeCodegenGroupOp(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<std::pair<TCodegenExpression, TCodegenAggregateUpdate>> codegenAggregates,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenGroupExprs),
        MOVE(codegenAggregates),
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        auto module = builder.Module->GetModule();

        // See GroupOpHelper.
        Function* collect = Function::Create(
            TypeBuilder<void(void**, void*, void*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "CollectGroups",
            module);

        auto collectArgs = collect->arg_begin();
        Value* collectClosure = collectArgs; collectClosure->setName("closure");
        Value* groupedRows = ++collectArgs; groupedRows->setName("groupedRows");
        Value* lookup = ++collectArgs; lookup->setName("lookup");
        YCHECK(++collectArgs == collect->arg_end());

        TCGContext collectBuilder(
            collect,
            &builder,
            collectClosure);

        int keySize = codegenGroupExprs.size();
        int aggregatesCount = codegenAggregates.size();

        Value* newRowPtr = collectBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

        collectBuilder.CreateCall3(
            builder.Module->GetRoutine("AllocatePermanentRow"),
            collectBuilder.GetExecutionContextPtr(),
            builder.getInt32(keySize + aggregatesCount),
            newRowPtr);

        std::vector<EValueType> keyTypes;

        codegenSource(
            collectBuilder,
            [&] (TCGContext& builder, Value* row) {
                Value* executionContextPtrRef = builder.GetExecutionContextPtr();
                Value* groupedRowsRef = builder.ViaClosure(groupedRows);
                Value* lookupRef = builder.ViaClosure(lookup);
                Value* newRowPtrRef = builder.ViaClosure(newRowPtr);
                Value* newRowRef = builder.CreateLoad(newRowPtrRef);

                for (int index = 0; index < keySize; ++index) {
                    auto id = index;

                    auto value = codegenGroupExprs[index](builder, row);
                    keyTypes.push_back(value.GetStaticType());
                    value.StoreToRow(builder, newRowRef, index, id);
                }

                std::vector<EValueType> aggregateTypes;
                for (int index = 0; index < aggregatesCount; ++index) {
                    auto id = keySize + index;

                    codegenAggregates[index].first(builder, row)
                        .StoreToRow(builder, newRowRef, keySize + index, id);
                }

                Value* foundRowPtr = builder.CreateCall5(
                    builder.Module->GetRoutine("InsertGroupRow"),
                    executionContextPtrRef,
                    lookupRef,
                    groupedRowsRef,
                    newRowPtrRef,
                    builder.getInt32(keySize + aggregatesCount));

                Value* condition = builder.CreateICmpNE(
                    foundRowPtr,
                    llvm::ConstantPointerNull::get(newRowRef->getType()->getPointerTo()));

                CodegenIf<TCGContext>(
                    builder,
                    condition,
                    [&] (TCGContext& builder) {
                        Value* foundRow = builder.CreateLoad(foundRowPtr);
                        for (int index = 0; index < aggregatesCount; ++index) {
                            auto aggState = builder.CreateConstInBoundsGEP1_32(
                                CodegenValuesPtrFromRow(builder, foundRow),
                                keySize + index);
                            auto newValue = builder.CreateConstInBoundsGEP1_32(
                                CodegenValuesPtrFromRow(builder, newRowRef),
                                keySize + index);
                            codegenAggregates[index].second(builder, aggState, newValue);
                        }
                    },
                    [&] (TCGContext& builder) { });
            });

        collectBuilder.CreateRetVoid();

        Function* consume = Function::Create(
            TypeBuilder<void(void**, void*, char*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "Consume",
            module);

        auto consumeArgs = consume->arg_begin();
        Value* consumeClosure = consumeArgs; consumeClosure->setName("closure");
        Value* finalGroupedRows = ++consumeArgs; finalGroupedRows->setName("finalGroupedRows");
        Value* stopFlag = ++consumeArgs; stopFlag->setName("stopFlag");
        YCHECK(++consumeArgs == consume->arg_end());

        TCGContext consumeBuilder(
            consume,
            &builder,
            consumeClosure);

        CodegenForEachRow(
            consumeBuilder,
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsData"), finalGroupedRows),
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsSize"), finalGroupedRows),
            stopFlag,
            codegenConsumer);

        consumeBuilder.CreateRetVoid();

        builder.CreateCallWithArgs(
            builder.Module->GetRoutine("GroupOpHelper"),
            {
                builder.GetExecutionContextPtr(),
                CodegenGroupHasherFunction(keyTypes, *builder.Module),
                CodegenGroupComparerFunction(keyTypes, *builder.Module),

                collectBuilder.GetClosure(),
                collect,

                consumeBuilder.GetClosure(),
                consume,
            });

    };
}

TCodegenSource MakeCodegenOrderOp(
    std::vector<Stroka> orderColumns,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource)
{
    return [
        MOVE(orderColumns),
        MOVE(sourceSchema),
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        auto module = builder.Module->GetModule();

        // See OrderOpHelper.
        Function* collectRows = Function::Create(
            TypeBuilder<void(void**, void*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "CollectRows",
            module);

        auto collectRowsArgs = collectRows->arg_begin();
        Value* closure = collectRowsArgs; closure->setName("closure");
        Value* topN = ++collectRowsArgs; topN->setName("topN");
        YCHECK(++collectRowsArgs == collectRows->arg_end());

        TCGContext collectBuilder(
            collectRows,
            &builder,
            closure);

        codegenSource(
            collectBuilder,
            [&] (TCGContext& builder, Value* row) {
                Value* topNRef = builder.ViaClosure(topN);

                builder.CreateCall2(
                    builder.Module->GetRoutine("AddRow"),
                    topNRef,
                    row);
            });

        collectBuilder.CreateRetVoid();

        // See OrderOpHelper.
        Function* consumeOrderedRows = Function::Create(
            TypeBuilder<void(void**, void*, char*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "ConsumeOrderedRows",
            module);

        auto consumeOrderedRowsArgs = consumeOrderedRows->arg_begin();
        Value* consumeClosure = consumeOrderedRowsArgs; consumeClosure->setName("consumeClosure");
        Value* orderedRows = ++consumeOrderedRowsArgs; orderedRows->setName("orderedRows");
        Value* stopFlag = ++consumeOrderedRowsArgs; stopFlag->setName("stopFlag");
        YCHECK(++consumeOrderedRowsArgs == consumeOrderedRows->arg_end());

        TCGContext consumeBuilder(
            consumeOrderedRows,
            &builder,
            consumeClosure);

        CodegenForEachRow(
            consumeBuilder,
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsData"), orderedRows),
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsSize"), orderedRows),
            stopFlag,
            codegenConsumer);

        consumeBuilder.CreateRetVoid();

        std::vector<std::function<TCGValue(TCGIRBuilder& builder, Value* row)>> compareArgs;
        for (int index = 0; index < orderColumns.size(); ++index) {
            auto columnIndex = sourceSchema.GetColumnIndexOrThrow(orderColumns[index]);
            auto type = sourceSchema.FindColumn(orderColumns[index])->Type;

            compareArgs.push_back([columnIndex, type] (TCGIRBuilder& builder, Value* row) {
                return TCGValue::CreateFromRow(
                    builder,
                    row,
                    columnIndex,
                    type);
            });
        }

        builder.CreateCallWithArgs(
            builder.Module->GetRoutine("OrderOpHelper"),
            {
                builder.GetExecutionContextPtr(),
                CodegenTupleComparerFunction(compareArgs, *builder.Module),

                collectBuilder.GetClosure(),
                collectRows,

                consumeBuilder.GetClosure(),
                consumeOrderedRows
            });
    };
}

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    TCodegenSource codegenSource)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());

    auto& context = module->GetContext();

    auto entryFunctionName = Stroka("Evaluate");

    Function* function = Function::Create(
        TypeBuilder<TCGQuerySignature, false>::get(context),
        Function::ExternalLinkage,
        entryFunctionName.c_str(),
        module->GetModule());

    auto args = function->arg_begin();
    Value* constants = args; constants->setName("constants");
    Value* executionContextPtr = ++args; executionContextPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGContext builder(module, constants, executionContextPtr, BasicBlock::Create(context, "entry", function));

    codegenSource(
        builder,
        [&] (TCGContext& builder, Value* row) {
            builder.CreateCall2(module->GetRoutine("WriteRow"), row, builder.GetExecutionContextPtr());
        });

    builder.CreateRetVoid();

    return module->GetCompiledFunction<TCGQuerySignature>(entryFunctionName);
}

TCGExpressionCallback CodegenExpression(TCodegenExpression codegenExpression)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    auto& context = module->GetContext();

    auto entryFunctionName = Stroka("EvaluateExpression");

    Function* function = Function::Create(
        TypeBuilder<TCGExpressionSignature, false>::get(context),
        Function::ExternalLinkage,
        entryFunctionName.c_str(),
        module->GetModule());

    auto args = function->arg_begin();
    Value* resultPtr = args; resultPtr->setName("resultPtr");
    Value* inputRow = ++args; inputRow->setName("inputRow");
    Value* constants = ++args; constants->setName("constants");
    Value* executionContextPtr = ++args; executionContextPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGContext builder(module, constants, executionContextPtr, BasicBlock::Create(context, "entry", function));

    auto result = codegenExpression(builder, inputRow);

    result.StoreToValue(builder, resultPtr, 0, "writeResult");

    builder.CreateRetVoid();

    return module->GetCompiledFunction<TCGExpressionSignature>(entryFunctionName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

