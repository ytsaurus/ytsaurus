#include "cg_fragment_compiler.h"
#include "private.h"
#include "cg_ir_builder.h"
#include "cg_routines.h"

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/codegen/module.h>
#include <yt/core/codegen/public.h>

#include <yt/core/logging/log.h>

#include <llvm/IR/Module.h>

// TODO(sandello):
//  - Implement basic logging & profiling within evaluation code
//  - Sometimes we can write through scratch space; some simple cases:
//    * int/double/null expressions only,
//    * string expressions with references (just need to copy string data)
//    It is possible to do better memory management here.
//  - TBAA is a king
//  - Capture pointers by value in ViaClosure

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;
using namespace NConcurrency;

using NCodegen::TCGModule;


////////////////////////////////////////////////////////////////////////////////
// Operator helpers
//

Value* CodegenAllocateRow(TCGIRBuilderPtr& builder, size_t valueCount)
{
    Value* newRowPtr = builder->CreateAlloca(TypeBuilder<TRow, false>::get(builder->getContext()));

    size_t size = sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue) * valueCount;

    Value* newRowData = builder->CreateAlignedAlloca(
        TypeBuilder<char, false>::get(builder->getContext()),
        8,
        builder->getInt32(size));

    builder->CreateStore(
        builder->CreatePointerCast(newRowData, TypeBuilder<TRowHeader*, false>::get(builder->getContext())),
        builder->CreateConstInBoundsGEP2_32(
            nullptr,
            newRowPtr,
            0,
            TypeBuilder<TRow, false>::Fields::Header));

    Value* newRow = builder->CreateLoad(newRowPtr);

    auto headerPtr = builder->CreateExtractValue(
        newRow,
        TypeBuilder<TRow, false>::Fields::Header);

    builder->CreateStore(
        builder->getInt32(valueCount),
        builder->CreateConstInBoundsGEP2_32(
            nullptr,
            headerPtr,
            0,
            TypeBuilder<TRowHeader, false>::Fields::Count));

    builder->CreateStore(
        builder->getInt32(valueCount),
        builder->CreateConstInBoundsGEP2_32(
            nullptr,
            headerPtr,
            0,
            TypeBuilder<TRowHeader, false>::Fields::Capacity));

    return newRow;
}

void CodegenForEachRow(
    TCGContext& builder,
    Value* rows,
    Value* size,
    const TCodegenConsumer& codegenConsumer)
{
    auto* loopBB = builder->CreateBBHere("loop");
    auto* condBB = builder->CreateBBHere("cond");
    auto* endloopBB = builder->CreateBBHere("endloop");

    // index = 0
    Value* indexPtr = builder->CreateAlloca(builder->getInt64Ty(), nullptr, "indexPtr");
    builder->CreateStore(builder->getInt64(0), indexPtr);

    builder->CreateBr(condBB);

    builder->SetInsertPoint(condBB);

    // if (index != size) ...
    Value* index = builder->CreateLoad(indexPtr, "index");
    Value* condition = builder->CreateICmpNE(index, size);
    builder->CreateCondBr(condition, loopBB, endloopBB);

    builder->SetInsertPoint(loopBB);

    // row = rows[index]; consume(row);
    Value* stackState = builder->CreateStackSave("stackState");
    Value* row = builder->CreateLoad(builder->CreateGEP(rows, index, "rowPtr"), "row");
    codegenConsumer(builder, row);
    builder->CreateStackRestore(stackState);
    // index = index + 1
    builder->CreateStore(builder->CreateAdd(index, builder->getInt64(1)), indexPtr);
    builder->CreateBr(condBB);

    builder->SetInsertPoint(endloopBB);
}

////////////////////////////////////////////////////////////////////////////////
// Expressions
//

void CheckNaN(const TCGModule& module, TCGIRBuilderPtr& builder, Value* lhsValue, Value* rhsValue)
{
    CodegenIf<TCGIRBuilderPtr&>(
        builder,
        builder->CreateFCmpUNO(lhsValue, rhsValue),
        [&] (TCGIRBuilderPtr& builder) {
            builder->CreateCall(
                module.GetRoutine("ThrowQueryException"),
                {
                    builder->CreateGlobalStringPtr("Comparison with NaN")
                });
        });
}

Function* CodegenGroupComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    return MakeFunction<TComparerFunction>(module.GetModule(), "GroupComparer", [&] (
        TCGIRBuilderPtr& builder,
        Value* lhsRow,
        Value* rhsRow
    ) {
        auto returnIf = [&] (Value* condition) {
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            builder->CreateCondBr(condition, thenBB, elseBB);
            builder->SetInsertPoint(thenBB);
            builder->CreateRet(builder->getInt8(0));
            builder->SetInsertPoint(elseBB);
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

            CodegenIf<TCGIRBuilderPtr>(
                builder,
                builder->CreateOr(lhsValue.IsNull(), rhsValue.IsNull()),
                [&] (TCGIRBuilderPtr& builder) {
                    returnIf(builder->CreateICmpNE(lhsValue.IsNull(), rhsValue.IsNull()));
                },
                [&] (TCGIRBuilderPtr& builder) {
                    auto* lhsData = lhsValue.GetData();
                    auto* rhsData = rhsValue.GetData();

                    switch (types[index]) {
                        case EValueType::Boolean:
                        case EValueType::Int64:
                        case EValueType::Uint64:
                            returnIf(builder->CreateICmpNE(lhsData, rhsData));
                            break;

                        case EValueType::Double:
                            CheckNaN(module, builder, lhsData, rhsData);
                            returnIf(builder->CreateFCmpUNE(lhsData, rhsData));
                            break;

                        case EValueType::String: {
                            Value* lhsLength = lhsValue.GetLength();
                            Value* rhsLength = rhsValue.GetLength();

                            Value* minLength = builder->CreateSelect(
                                builder->CreateICmpULT(lhsLength, rhsLength),
                                lhsLength,
                                rhsLength);

                            Value* cmpResult = builder->CreateCall(
                                module.GetRoutine("memcmp"),
                                {
                                    lhsData,
                                    rhsData,
                                    builder->CreateZExt(minLength, builder->getSizeType())
                                });

                            returnIf(builder->CreateOr(
                                builder->CreateICmpNE(cmpResult, builder->getInt32(0)),
                                builder->CreateICmpNE(lhsLength, rhsLength)));
                            break;
                        }

                        default:
                            Y_UNREACHABLE();
                    }
                });
        };

        YCHECK(!types.empty());

        for (size_t index = start; index < finish; ++index) {
            codegenEqualOp(index);
        }

        builder->CreateRet(builder->getInt8(1));
    });
}

Function* CodegenGroupComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    return CodegenGroupComparerFunction(types, module, 0, types.size());
}

Value* CodegenFingerprint64(TCGIRBuilderPtr& builder, Value* x)
{
    Value* kMul = builder->getInt64(0x9ddfea08eb382d69ULL);
    Value* b = builder->CreateMul(x, kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(44)));
    b = builder->CreateMul(b, kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(41)));
    b = builder->CreateMul(b, kMul);
    return b;
};

Value* CodegenFingerprint128(TCGIRBuilderPtr& builder, Value* x, Value* y)
{
    Value* kMul = builder->getInt64(0x9ddfea08eb382d69ULL);
    Value* a = builder->CreateMul(builder->CreateXor(x, y), kMul);
    a = builder->CreateXor(a, builder->CreateLShr(a, builder->getInt64(47)));
    Value* b = builder->CreateMul(builder->CreateXor(y, a), kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(44)));
    b = builder->CreateMul(b, kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(41)));
    b = builder->CreateMul(b, kMul);
    return b;
};

Function* CodegenGroupHasherFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    return MakeFunction<THasherFunction>(module.GetModule(), "GroupHasher", [&] (
        TCGIRBuilderPtr& builder,
        Value* row
    ) {
        auto codegenHashOp = [&] (size_t index, TCGIRBuilderPtr& builder) -> Value* {
            auto value = TCGValue::CreateFromRow(
                builder,
                row,
                index,
                types[index]);

            auto* conditionBB = builder->CreateBBHere("condition");
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            auto* endBB = builder->CreateBBHere("end");

            builder->CreateBr(conditionBB);

            builder->SetInsertPoint(conditionBB);
            builder->CreateCondBr(value.IsNull(), elseBB, thenBB);
            conditionBB = builder->GetInsertBlock();

            builder->SetInsertPoint(thenBB);

            Value* thenResult;

            switch (value.GetStaticType()) {
                case EValueType::Boolean:
                case EValueType::Int64:
                case EValueType::Uint64:
                    thenResult = CodegenFingerprint64(builder, value.Cast(builder, EValueType::Uint64).GetData());
                    break;

                case EValueType::Double:
                    thenResult = CodegenFingerprint64(builder, value.Cast(builder, EValueType::Uint64, true)
                        .GetData());
                    break;

                case EValueType::String:
                    thenResult = builder->CreateCall(
                        module.GetRoutine("StringHash"),
                        {
                            value.GetData(),
                            value.GetLength()
                        });
                    break;

                default:
                    Y_UNIMPLEMENTED();
            }

            builder->CreateBr(endBB);
            thenBB = builder->GetInsertBlock();

            builder->SetInsertPoint(elseBB);
            auto* elseResult = builder->getInt64(0);
            builder->CreateBr(endBB);
            elseBB = builder->GetInsertBlock();

            builder->SetInsertPoint(endBB);

            PHINode* result = builder->CreatePHI(thenResult->getType(), 2);
            result->addIncoming(thenResult, thenBB);
            result->addIncoming(elseResult, elseBB);

            return result;
        };

        auto codegenHashCombine = [&] (TCGIRBuilderPtr& builder, Value* first, Value* second) -> Value* {
            //first ^ (second + 0x9e3779b9 + (second << 6) + (second >> 2));
            return builder->CreateXor(
                first,
                builder->CreateAdd(
                    builder->CreateAdd(
                        builder->CreateAdd(second, builder->getInt64(0x9e3779b9)),
                        builder->CreateLShr(second, builder->getInt64(2))),
                    builder->CreateShl(second, builder->getInt64(6))));
        };

        YCHECK(!types.empty());
        Value* result = builder->getInt64(0);
        for (size_t index = start; index < finish; ++index) {
            result = codegenHashCombine(builder, result, codegenHashOp(index, builder));
        }
        builder->CreateRet(result);
    });
}

Function* CodegenGroupHasherFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    return CodegenGroupHasherFunction(types, module, 0, types.size());
}

Function* CodegenTupleComparerFunction(
    const std::vector<std::function<TCGValue(TCGIRBuilderPtr& builder, Value* row)>>& codegenArgs,
    const TCGModule& module,
    const std::vector<bool>& isDesc = std::vector<bool>())
{
    return MakeFunction<TComparerFunction>(module.GetModule(), "RowComparer", [&] (
        TCGIRBuilderPtr& builder,
        Value* lhsRow,
        Value* rhsRow
    ) {
        auto returnIf = [&] (Value* condition, const TCodegenBlock& codegenInner) {
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            builder->CreateCondBr(condition, thenBB, elseBB);
            builder->SetInsertPoint(thenBB);
            builder->CreateRet(builder->CreateSelect(codegenInner(builder), builder->getInt8(1), builder->getInt8(0)));
            builder->SetInsertPoint(elseBB);
        };

        auto codegenEqualOrLessOp = [&] (int index) {
            const auto& codegenArg = codegenArgs[index];
            auto lhsValue = codegenArg(builder, lhsRow);
            auto rhsValue = codegenArg(builder, rhsRow);

            if (index < isDesc.size() && isDesc[index]) {
                std::swap(lhsValue, rhsValue);
            }

            auto type = lhsValue.GetStaticType();

            YCHECK(type == rhsValue.GetStaticType());

            CodegenIf<TCGIRBuilderPtr>(
                builder,
                builder->CreateOr(lhsValue.IsNull(), rhsValue.IsNull()),
                [&] (TCGIRBuilderPtr& builder) {
                    returnIf(
                        builder->CreateICmpNE(lhsValue.IsNull(), rhsValue.IsNull()),
                        [&] (TCGIRBuilderPtr& builder) {
                            return builder->CreateICmpUGT(lhsValue.IsNull(), rhsValue.IsNull());
                        });
                },
                [&] (TCGIRBuilderPtr& builder) {
                    auto* lhsData = lhsValue.GetData();
                    auto* rhsData = rhsValue.GetData();

                    switch (type) {
                        case EValueType::Boolean:
                        case EValueType::Int64:
                            returnIf(
                                builder->CreateICmpNE(lhsData, rhsData),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return builder->CreateICmpSLT(lhsData, rhsData);
                                });
                            break;

                        case EValueType::Uint64:
                            returnIf(
                                builder->CreateICmpNE(lhsData, rhsData),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return builder->CreateICmpULT(lhsData, rhsData);
                                });
                            break;

                        case EValueType::Double:
                            CheckNaN(module, builder, lhsData, rhsData);

                            returnIf(
                                builder->CreateFCmpUNE(lhsData, rhsData),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return builder->CreateFCmpULT(lhsData, rhsData);
                                });
                            break;

                        case EValueType::String: {
                            Value* lhsLength = lhsValue.GetLength();
                            Value* rhsLength = rhsValue.GetLength();

                            Value* minLength = builder->CreateSelect(
                                builder->CreateICmpULT(lhsLength, rhsLength),
                                lhsLength,
                                rhsLength);

                            Value* cmpResult = builder->CreateCall(
                                module.GetRoutine("memcmp"),
                                {
                                    lhsData,
                                    rhsData,
                                    builder->CreateZExt(minLength, builder->getSizeType())
                                });

                            returnIf(
                                builder->CreateICmpNE(cmpResult, builder->getInt32(0)),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return builder->CreateICmpSLT(cmpResult, builder->getInt32(0));
                                });

                            returnIf(
                                builder->CreateICmpNE(lhsLength, rhsLength),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return builder->CreateICmpULT(lhsLength, rhsLength);
                                });

                            break;
                        }

                        case EValueType::Null:
                            break;

                        default:
                            Y_UNREACHABLE();
                    }
                });
        };

        for (int index = 0; index < codegenArgs.size(); ++index) {
            codegenEqualOrLessOp(index);
        }

        builder->CreateRet(builder->getInt8(0));
    });
}

Function* CodegenRowComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    std::vector<std::function<TCGValue(TCGIRBuilderPtr& builder, Value* row)>> compareArgs;
    for (int index = start; index < finish; ++index) {
        compareArgs.push_back([index, type = types[index]] (TCGIRBuilderPtr& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                row,
                index,
                type);
        });
    }

    return CodegenTupleComparerFunction(compareArgs, module);
}

Function* CodegenRowComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    return CodegenRowComparerFunction(types, module, 0, types.size());
}

Value* CodegenLexicographicalCompare(
    TCGBaseContext& builder,
    Value* lhsData,
    Value* lhsLength,
    Value* rhsData,
    Value* rhsLength)
{
    Value* lhsLengthIsLess = builder->CreateICmpULT(lhsLength, rhsLength);
    Value* minLength = builder->CreateSelect(
        lhsLengthIsLess,
        lhsLength,
        rhsLength);

    Value* cmpResult = builder->CreateCall(
        builder.Module->GetRoutine("memcmp"),
        {
            lhsData,
            rhsData,
            builder->CreateZExt(minLength, builder->getSizeType())
        });

    return builder->CreateOr(
        builder->CreateICmpSLT(cmpResult, builder->getInt32(0)),
        builder->CreateAnd(
            builder->CreateICmpEQ(cmpResult, builder->getInt32(0)),
            lhsLengthIsLess));
};

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    EValueType type)
{
    return [
            index,
            type
        ] (TCGExprContext& builder, Value* row) {
            auto valuePtr = builder->CreatePointerCast(
                builder.GetOpaqueValue(index),
                TypeBuilder<TValue*, false>::get(builder->getContext()));

            return TCGValue::CreateFromLlvmValue(
                builder,
                valuePtr,
                type,
                "literal." + Twine(index))
                .Steal();

        };
}

TCodegenExpression MakeCodegenReferenceExpr(
    int index,
    EValueType type,
    TString name)
{
    return [
            index,
            type,
            MOVE(name)
        ] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                row,
                index,
                type,
                "reference." + Twine(name.c_str()));
        };
}

TCodegenValue MakeCodegenFunctionContext(
    int index)
{
    return [
            index
        ] (TCGBaseContext& builder) {
            return builder.GetOpaqueValue(index);
        };
}

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(codegenOperand),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder, Value* row) {
        auto operandValue = codegenOperand(builder, row);

        return CodegenIf<TCGIRBuilderPtr, TCGValue>(
            builder,
            operandValue.IsNull(),
            [&] (TCGIRBuilderPtr& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGIRBuilderPtr& builder) {
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
                                evalData = builder->CreateSub(builder->getInt64(0), operandData);
                                break;
                            case EValueType::Double:
                                evalData = builder->CreateFSub(ConstantFP::get(builder->getDoubleTy(), 0.0), operandData);
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                        break;


                    case EUnaryOp::BitNot:
                        evalData = builder->CreateNot(operandData);
                        break;

                    case EUnaryOp::Not:
                        evalData = builder->CreateXor(
                            builder->CreateZExtOrBitCast(
                                builder->getTrue(),
                                TDataTypeBuilder::TBoolean::get(builder->getContext())),
                            operandData);
                        break;

                    default:
                        Y_UNREACHABLE();
                }

                return TCGValue::CreateFromValue(
                    builder,
                    builder->getFalse(),
                    nullptr,
                    evalData,
                    type);
            },
            Twine(name.c_str()));
    };
}

TCodegenExpression MakeCodegenLogicalBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(codegenLhs),
        MOVE(codegenRhs),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder, Value* row) {
        auto compare = [&] (bool parameter) {
            auto lhsValue = codegenLhs(builder, row);
            auto rhsValue = codegenRhs(builder, row);

            if (lhsValue.GetStaticType() == EValueType::Null) {
                lhsValue = TCGValue::CreateNull(builder, type);
            }
            if (rhsValue.GetStaticType() == EValueType::Null) {
                rhsValue = TCGValue::CreateNull(builder, type);
            }

            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                lhsValue.IsNull(),
                [&] (TCGExprContext& builder) {
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        rhsValue.IsNull(),
                        [&] (TCGBaseContext& builder) {
                            return TCGValue::CreateNull(builder, type);
                        },
                        [&] (TCGBaseContext& builder) {
                            Value* rhsData = rhsValue.GetData();
                            return CodegenIf<TCGBaseContext, TCGValue>(
                                builder,
                                builder->CreateICmpEQ(rhsData, builder->getInt8(parameter)),
                                [&] (TCGBaseContext& builder) {
                                    return rhsValue;
                                },
                                [&] (TCGBaseContext& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                });
                        });
                },
                [&] (TCGExprContext& builder) {
                    Value* lhsData = lhsValue.GetData();
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        builder->CreateICmpEQ(lhsData, builder->getInt8(parameter)),
                        [&] (TCGBaseContext& builder) {
                            return lhsValue;
                        },
                        [&] (TCGBaseContext& builder) {
                            return CodegenIf<TCGBaseContext, TCGValue>(
                                builder,
                                rhsValue.IsNull(),
                                [&] (TCGBaseContext& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                },
                                [&] (TCGBaseContext& builder) {
                                    return rhsValue;
                                });
                        });
                });
        };

        switch (opcode) {
            case EBinaryOp::And:
                return compare(false);
            case EBinaryOp::Or:
                return compare(true);
            default:
                Y_UNREACHABLE();
        }
    };
}

TCodegenExpression MakeCodegenRelationalBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(codegenLhs),
        MOVE(codegenRhs),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder, Value* row) {
        auto nameTwine = Twine(name.c_str());
        auto lhsValue = codegenLhs(builder, row);
        auto rhsValue = codegenRhs(builder, row);

        #define CMP_OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->CreateZExtOrBitCast( \
                    builder->Create##optype(lhsData, rhsData), \
                    TDataTypeBuilder::TBoolean::get(builder->getContext())); \
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
                    Y_UNREACHABLE();
            }

            return TCGValue::CreateFromValue(
                builder,
                builder->getFalse(),
                nullptr,
                evalData,
                type);
        };

        return CodegenIf<TCGBaseContext, TCGValue>(
            builder,
            lhsValue.IsNull(),
            [&] (TCGBaseContext& builder) {
                return compareNulls();
            },
            [&] (TCGBaseContext& builder) {
                return CodegenIf<TCGBaseContext, TCGValue>(
                    builder,
                    rhsValue.IsNull(),
                    [&] (TCGBaseContext& builder) {
                        return compareNulls();
                    },
                    [&] (TCGBaseContext& builder) {
                        if (lhsValue.GetStaticType() == EValueType::Null ||
                            rhsValue.GetStaticType() == EValueType::Null)
                        {
                            // Stub value. Nulls are handled above.
                            return TCGValue::CreateNull(builder, type);
                        }

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
                                        Y_UNREACHABLE();
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
                                        Y_UNREACHABLE();
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
                                        Y_UNREACHABLE();
                                }
                                break;
                            case EValueType::String: {
                                Value* lhsLength = lhsValue.GetLength();
                                Value* rhsLength = rhsValue.GetLength();

                                auto codegenEqual = [&] () {
                                    return CodegenIf<TCGBaseContext, Value*>(
                                        builder,
                                        builder->CreateICmpEQ(lhsLength, rhsLength),
                                        [&] (TCGBaseContext& builder) {
                                            Value* minLength = builder->CreateSelect(
                                                builder->CreateICmpULT(lhsLength, rhsLength),
                                                lhsLength,
                                                rhsLength);

                                            Value* cmpResult = builder->CreateCall(
                                                builder.Module->GetRoutine("memcmp"),
                                                {
                                                    lhsData,
                                                    rhsData,
                                                    builder->CreateZExt(minLength, builder->getSizeType())
                                                });

                                            return builder->CreateICmpEQ(cmpResult, builder->getInt32(0));
                                        },
                                        [&] (TCGBaseContext& builder) {
                                            return builder->getFalse();
                                        });
                                };

                                switch (opcode) {
                                    case EBinaryOp::Equal:
                                        evalData = codegenEqual();
                                        break;
                                    case EBinaryOp::NotEqual:
                                        evalData = builder->CreateNot(codegenEqual());
                                        break;
                                    case EBinaryOp::Less:
                                        evalData = CodegenLexicographicalCompare(builder, lhsData, lhsLength, rhsData, rhsLength);
                                        break;
                                    case EBinaryOp::Greater:
                                        evalData = CodegenLexicographicalCompare(builder, rhsData, rhsLength, lhsData, lhsLength);
                                        break;
                                    case EBinaryOp::LessOrEqual:
                                        evalData =  builder->CreateNot(
                                            CodegenLexicographicalCompare(builder, rhsData, rhsLength, lhsData, lhsLength));
                                        break;
                                    case EBinaryOp::GreaterOrEqual:
                                        evalData = builder->CreateNot(
                                            CodegenLexicographicalCompare(builder, lhsData, lhsLength, rhsData, rhsLength));
                                        break;
                                    default:
                                        Y_UNREACHABLE();
                                }

                                evalData = builder->CreateZExtOrBitCast(
                                    evalData,
                                    TDataTypeBuilder::TBoolean::get(builder->getContext()));
                                break;
                            }
                            default:
                                Y_UNREACHABLE();
                        }

                        return TCGValue::CreateFromValue(
                            builder,
                            builder->getFalse(),
                            nullptr,
                            evalData,
                            type);
                    });
            },
            nameTwine);

            #undef CMP_OP
    };
}

TCodegenExpression MakeCodegenArithmeticBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(codegenLhs),
        MOVE(codegenRhs),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder, Value* row) {
        auto nameTwine = Twine(name.c_str());

        auto lhsValue = codegenLhs(builder, row);

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            lhsValue.IsNull(),
            [&] (TCGExprContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGExprContext& builder) {
                auto rhsValue = codegenRhs(builder, row);

                return CodegenIf<TCGBaseContext, TCGValue>(
                    builder,
                    rhsValue.IsNull(),
                    [&] (TCGBaseContext& builder) {
                        return TCGValue::CreateNull(builder, type);
                    },
                    [&] (TCGBaseContext& builder) {
                        YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
                        auto operandType = lhsValue.GetStaticType();

                        Value* lhsData = lhsValue.GetData();
                        Value* rhsData = rhsValue.GetData();
                        Value* evalData = nullptr;

                        #define OP(opcode, optype) \
                            case EBinaryOp::opcode: \
                                evalData = builder->Create##optype(lhsData, rhsData); \
                                break;

                        auto checkZero = [&] (Value* value) {
                            CodegenIf<TCGBaseContext>(
                                builder,
                                builder->CreateIsNull(value),
                                [] (TCGBaseContext& builder) {
                                    builder->CreateCall(
                                        builder.Module->GetRoutine("ThrowQueryException"),
                                        {
                                            builder->CreateGlobalStringPtr("Division by zero")
                                        });
                                });
                        };

                        #define OP_ZERO_CHECKED(opcode, optype) \
                            case EBinaryOp::opcode: \
                                checkZero(rhsData); \
                                evalData = builder->Create##optype(lhsData, rhsData); \
                                break;

                        switch (operandType) {

                            case EValueType::Int64:
                                switch (opcode) {
                                    OP(Plus, Add)
                                    OP(Minus, Sub)
                                    OP(Multiply, Mul)
                                    OP_ZERO_CHECKED(Divide, SDiv)
                                    OP_ZERO_CHECKED(Modulo, SRem)
                                    OP(BitAnd, And)
                                    OP(BitOr, Or)
                                    OP(LeftShift, Shl)
                                    OP(RightShift, LShr)
                                    default:
                                        Y_UNREACHABLE();
                                }
                                break;
                            case EValueType::Uint64:
                                switch (opcode) {
                                    OP(Plus, Add)
                                    OP(Minus, Sub)
                                    OP(Multiply, Mul)
                                    OP_ZERO_CHECKED(Divide, UDiv)
                                    OP_ZERO_CHECKED(Modulo, URem)
                                    OP(BitAnd, And)
                                    OP(BitOr, Or)
                                    OP(And, And)
                                    OP(Or, Or)
                                    OP(LeftShift, Shl)
                                    OP(RightShift, LShr)
                                    default:
                                        Y_UNREACHABLE();
                                }
                                break;
                            case EValueType::Double:
                                switch (opcode) {
                                    OP(Plus, FAdd)
                                    OP(Minus, FSub)
                                    OP(Multiply, FMul)
                                    OP(Divide, FDiv)
                                    default:
                                        Y_UNREACHABLE();
                                }
                                break;
                            default:
                                Y_UNREACHABLE();
                        }

                        #undef OP

                        return TCGValue::CreateFromValue(
                            builder,
                            builder->getFalse(),
                            nullptr,
                            evalData,
                            type);
                    });
           },
            nameTwine);
    };
}

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    TString name)
{
    if (IsLogicalBinaryOp(opcode)) {
        return MakeCodegenLogicalBinaryOpExpr(
            opcode,
            std::move(codegenLhs),
            std::move(codegenRhs),
            type,
            std::move(name));
    } else if (IsRelationalBinaryOp(opcode)) {
        return MakeCodegenRelationalBinaryOpExpr(
            opcode,
            std::move(codegenLhs),
            std::move(codegenRhs),
            type,
            std::move(name));
    } else {
        return MakeCodegenArithmeticBinaryOpExpr(
            opcode,
            std::move(codegenLhs),
            std::move(codegenRhs),
            type,
            std::move(name));
    }
}

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex)
{
    return [
        MOVE(codegenArgs),
        MOVE(arrayIndex)
    ] (TCGExprContext& builder, Value* row) {
        size_t keySize = codegenArgs.size();

        Value* newRow = CodegenAllocateRow(builder, keySize);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto id = index;
            auto value = codegenArgs[index](builder, row);
            keyTypes.push_back(value.GetStaticType());
            value.StoreToRow(builder, newRow, index, id);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("IsRowInArray"),
            {
                CodegenRowComparerFunction(keyTypes, *builder.Module),
                newRow,
                builder.GetOpaqueValue(arrayIndex)
            });

        return TCGValue::CreateFromValue(
            builder,
            builder->getFalse(),
            nullptr,
            result,
            EValueType::Boolean);
    };
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void CodegenEmptyOp(TCGOperatorContext& builder)
{ }

size_t MakeCodegenScanOp(
    TCodegenSource* codegenSource,
    size_t* slotCount)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        codegenSource(builder);

        auto consume = MakeClosure<void(TRowBuffer*, TRow*, i64)>(builder, "ScanOpInner", [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* rows,
            Value* size
        ) {
            TCGContext innerBulder(builder, buffer);
            CodegenForEachRow(innerBulder, rows, size, builder[consumerSlot]);
            innerBulder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("ScanOpHelper"),
            {
                builder.GetExecutionContext(),
                consume.ClosurePtr,
                consume.Function
            });
    };

    return consumerSlot;
}

std::tuple<size_t, size_t, size_t> MakeCodegenSplitterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    size_t streamIndex)
{
    size_t finalConsumerSlot = (*slotCount)++;
    size_t intermediateConsumerSlot = (*slotCount)++;
    size_t totalsConsumerSlot = (*slotCount)++;

    *codegenSource = [
        finalConsumerSlot,
        intermediateConsumerSlot,
        totalsConsumerSlot,
        producerSlot,
        streamIndex,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            auto* ifFinalBB = builder->CreateBBHere("ifFinal");
            auto* ifIntermediateBB = builder->CreateBBHere("ifIntermediate");
            auto* ifTotalsBB = builder->CreateBBHere("ifTotals");
            auto* endIfBB = builder->CreateBBHere("endIf");

            auto streamIndexValue = TCGValue::CreateFromRow(
                builder,
                row,
                streamIndex,
                EValueType::Uint64,
                "reference.streamIndex");

            auto switcher = builder->CreateSwitch(streamIndexValue.GetData(), endIfBB);

            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Final)), ifFinalBB);
            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Intermediate)), ifIntermediateBB);
            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Totals)), ifTotalsBB);

            builder->SetInsertPoint(ifFinalBB);
            builder[finalConsumerSlot](builder, row);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(ifIntermediateBB);
            builder[intermediateConsumerSlot](builder, row);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(ifTotalsBB);
            builder[totalsConsumerSlot](builder, row);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);
        };

        codegenSource(builder);
    };

    return std::make_tuple(finalConsumerSlot, intermediateConsumerSlot, totalsConsumerSlot);
}

size_t MakeCodegenJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    int index,
    std::vector<std::pair<TCodegenExpression, bool>> equations,
    size_t commonKeyPrefix)
{
    size_t consumerSlot = (*slotCount)++;
    *codegenSource = [
        consumerSlot,
        producerSlot,
        index,
        MOVE(equations),
        commonKeyPrefix,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        int lookupKeySize = equations.size();
        // TODO(lukyan): Do not fill in consumer
        std::vector<EValueType> lookupKeyTypes(lookupKeySize);

        auto collectRows = MakeClosure<void(TJoinClosure*, TRowBuffer*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* joinClosure,
            Value* buffer
        ) {
            Value* keyPtr = builder->CreateAlloca(TypeBuilder<TRow, false>::get(builder->getContext()));
            builder->CreateCall(
                builder.Module->GetRoutine("AllocatePermanentRow"),
                {
                    builder.GetExecutionContext(),
                    buffer,
                    builder->getInt32(lookupKeySize),
                    keyPtr
                });

            builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                Value* keyPtrRef = builder->ViaClosure(keyPtr);
                Value* keyRef = builder->CreateLoad(keyPtrRef);

                for (int column = 0; column < lookupKeySize; ++column) {
                    if (!equations[column].second) {
                        auto joinKeyValue = equations[column].first(builder, row);
                        lookupKeyTypes[column] = joinKeyValue.GetStaticType();
                        joinKeyValue.StoreToRow(builder, keyRef, column, column);
                    }
                }

                for (int column = 0; column < lookupKeySize; ++column) {
                    if (equations[column].second) {
                        auto evaluatedColumn = equations[column].first(builder, keyRef);
                        lookupKeyTypes[column] = evaluatedColumn.GetStaticType();
                        evaluatedColumn.StoreToRow(builder, keyRef, column, column);
                    }
                }

                Value* joinClosureRef = builder->ViaClosure(joinClosure);

                builder->CreateCall(
                    builder.Module->GetRoutine("InsertJoinRow"),
                    {
                        builder.GetExecutionContext(),
                        joinClosureRef,
                        keyPtrRef,
                        row
                    });
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });


        auto consumeJoinedRows = MakeClosure<void(TRowBuffer*, TRow*, i64)>(builder, "ConsumeJoinedRows", [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* joinedRows,
            Value* size
        ) {
            TCGContext innerBuilder(builder, buffer);
            CodegenForEachRow(
                innerBuilder,
                joinedRows,
                size,
                builder[consumerSlot]);

            innerBuilder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("JoinOpHelper"),
            {
                builder.GetExecutionContext(),
                builder.GetOpaqueValue(index),

                CodegenGroupHasherFunction(lookupKeyTypes, *builder.Module, commonKeyPrefix, lookupKeyTypes.size()),
                CodegenGroupComparerFunction(lookupKeyTypes, *builder.Module, commonKeyPrefix, lookupKeyTypes.size()),
                CodegenRowComparerFunction(lookupKeyTypes, *builder.Module, commonKeyPrefix, lookupKeyTypes.size()),

                CodegenGroupComparerFunction(lookupKeyTypes, *builder.Module, 0, commonKeyPrefix),
                CodegenGroupComparerFunction(lookupKeyTypes, *builder.Module),
                CodegenRowComparerFunction(lookupKeyTypes, *builder.Module),

                builder->getInt32(lookupKeySize),

                collectRows.ClosurePtr,
                collectRows.Function,

                consumeJoinedRows.ClosurePtr,
                consumeJoinedRows.Function
            });
    };

    return consumerSlot;
}

size_t MakeCodegenFilterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenExpression codegenPredicate)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(codegenPredicate),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            auto predicateResult = codegenPredicate(builder, row);

            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            auto* notIsNull = builder->CreateNot(predicateResult.IsNull());
            auto* isTrue = builder->CreateICmpEQ(predicateResult.GetData(), builder->getInt8(true));

            builder->CreateCondBr(
                builder->CreateAnd(notIsNull, isTrue),
                ifBB,
                endIfBB);

            builder->SetInsertPoint(ifBB);
            builder[consumerSlot](builder, row);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenAddStreamOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::vector<EValueType> sourceSchema,
    EStreamTag value)
{
    std::vector<TCodegenExpression> codegenProjectExprs;
    for (size_t index = 0; index < sourceSchema.size(); ++index) {
        codegenProjectExprs.push_back(MakeCodegenReferenceExpr(index, sourceSchema[index], ""));
    }

    codegenProjectExprs.push_back([value] (TCGExprContext& builder, Value* row) {
            return TCGValue::CreateFromValue(
                builder,
                builder->getInt1(false),
                nullptr,
                builder->getInt64(static_cast<ui64>(value)),
                EValueType::Uint64,
                "streamIndex");
        });

    return MakeCodegenProjectOp(
        codegenSource,
        slotCount,
        producerSlot,
        std::move(codegenProjectExprs));
}

size_t MakeCodegenProjectOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::vector<TCodegenExpression> codegenArgs)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(codegenArgs),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        int projectionCount = codegenArgs.size();

        Value* newRow = CodegenAllocateRow(builder, projectionCount);

        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            Value* newRowRef = builder->ViaClosure(newRow);

            for (int index = 0; index < projectionCount; ++index) {
                auto id = index;

                codegenArgs[index](builder, row)
                    .StoreToRow(builder, newRowRef, index, id);
            }

            builder[consumerSlot](builder, newRowRef);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateGroups(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<EValueType> nullTypes)
{
    return [
        MOVE(codegenGroupExprs),
        MOVE(nullTypes)
    ] (TCGContext& builder, Value* srcRow, Value* dstRow) {
        for (int index = 0; index < codegenGroupExprs.size(); index++) {
            auto value = codegenGroupExprs[index](builder, srcRow);
            value.StoreToRow(builder, dstRow, index, index);
        }

        size_t offset = codegenGroupExprs.size();
        for (int index = 0; index < nullTypes.size(); ++index) {
            TCGValue::CreateNull(builder, nullTypes[index])
                .StoreToRow(builder, dstRow, offset + index, offset + index);
        }
    };
}

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateAggregateArgs(
    size_t keySize,
    std::vector<TCodegenExpression> codegenAggregateExprs)
{
    return [
        keySize,
        MOVE(codegenAggregateExprs)
    ] (TCGContext& builder, Value* srcRow, Value* dstRow) {
        for (int index = 0; index < codegenAggregateExprs.size(); index++) {
            auto id = keySize + index;
            auto value = codegenAggregateExprs[index](builder, srcRow);
            value.StoreToRow(builder, dstRow, keySize + index, id);
        }
    };
}

std::function<void(TCGContext& builder, Value* row)> MakeCodegenAggregateInitialize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize)
{
    return [
        MOVE(codegenAggregates),
        keySize
    ] (TCGContext& builder, Value* row) {
        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto id = keySize + index;
            auto initState = codegenAggregates[index].Initialize(
                builder,
                row);
            initState.StoreToRow(
                builder,
                row,
                keySize + index,
                id);
        }
    };
}

std::function<void(TCGContext& builder, Value*, Value*)> MakeCodegenAggregateUpdate(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize,
    bool isMerge)
{
    return [
        MOVE(codegenAggregates),
        keySize,
        isMerge
    ] (TCGContext& builder, Value* newRow, Value* groupRow) {
        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto aggState = builder->CreateConstInBoundsGEP1_32(
                nullptr,
                CodegenValuesPtrFromRow(builder, groupRow),
                keySize + index);
            auto newValue = builder->CreateConstInBoundsGEP1_32(
                nullptr,
                CodegenValuesPtrFromRow(builder, newRow),
                keySize + index);

            auto id = keySize + index;
            TCodegenAggregateUpdate updateFunction;
            if (isMerge) {
                updateFunction = codegenAggregates[index].Merge;
            } else {
                updateFunction = codegenAggregates[index].Update;
            }
            updateFunction(
                builder,
                aggState,
                newValue)
                .StoreToRow(
                    builder,
                    groupRow,
                    keySize + index,
                    id);
        }
    };
}

std::function<void(TCGContext& builder, Value* row)> MakeCodegenAggregateFinalize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize)
{
    return [
        MOVE(codegenAggregates),
        keySize
    ] (TCGContext& builder, Value* row) {
        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto id = keySize + index;
            auto valuesPtr = CodegenValuesPtrFromRow(builder, row);
            auto resultValue = codegenAggregates[index].Finalize(
                builder,
                builder->CreateConstInBoundsGEP1_32(
                    nullptr,
                    valuesPtr,
                    keySize + index));
            resultValue.StoreToRow(
                builder,
                row,
                keySize + index,
                id);
        }
    };
}

std::tuple<size_t, size_t> MakeCodegenSplitOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot)
{
    size_t firstSlot = (*slotCount)++;
    size_t secondSlot = (*slotCount)++;

    *codegenSource = [
        firstSlot,
        secondSlot,
        producerSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            builder[firstSlot](builder, row);
            builder[secondSlot](builder, row);
        };

        codegenSource(builder);
    };

    return std::make_tuple(firstSlot, secondSlot);
}

size_t MakeCodegenMergeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t firstSlot,
    size_t secondSlot)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        firstSlot,
        secondSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[firstSlot] = builder[consumerSlot];
        builder[secondSlot] = builder[consumerSlot];

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::function<void(TCGContext&, Value*)> codegenInitialize,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateGroups,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateAggregateArgs,
    std::function<void(TCGContext&, Value*, Value*)> codegenUpdate,
    std::vector<EValueType> keyTypes,
    bool isMerge,
    int groupRowSize,
    bool checkNulls)
{
    // codegenInitialize calls the aggregates' initialisation functions
    // codegenEvaluateGroups evaluates the group expressions
    // codegenEvaluateAggregateArgs evaluates the aggregates' arguments
    // codegenUpdate calls the aggregates' update or merge functions

    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(codegenInitialize),
        MOVE(codegenEvaluateGroups),
        MOVE(codegenEvaluateAggregateArgs),
        MOVE(codegenUpdate),
        codegenSource = std::move(*codegenSource),
        MOVE(keyTypes),
        isMerge,
        groupRowSize,
        checkNulls
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TGroupByClosure*, TRowBuffer*)>(builder, "CollectGroups", [&] (
            TCGOperatorContext& builder,
            Value* groupByClosure,
            Value* buffer
        ) {
            Value* newRowPtr = builder->CreateAlloca(TypeBuilder<TRow, false>::get(builder->getContext()));

            builder->CreateCall(
                builder.Module->GetRoutine("AllocatePermanentRow"),
                {
                    builder.GetExecutionContext(),
                    buffer,
                    builder->getInt32(groupRowSize),
                    newRowPtr
                });

                builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                    Value* bufferRef = builder->ViaClosure(buffer);
                    Value* newRowPtrRef = builder->ViaClosure(newRowPtr);
                    Value* newRowRef = builder->CreateLoad(newRowPtrRef);

                    codegenEvaluateGroups(builder, row, newRowRef);

                    Value* groupByClosureRef = builder->ViaClosure(groupByClosure);

                    auto groupRowPtr = builder->CreateCall(
                        builder.Module->GetRoutine("InsertGroupRow"),
                        {
                            builder.GetExecutionContext(),
                            groupByClosureRef,
                            newRowRef
                        });

                    auto groupRow = builder->CreateLoad(groupRowPtr);

                    auto inserted = builder->CreateICmpEQ(
                        builder->CreateExtractValue(
                            groupRow,
                            TypeBuilder<TRow, false>::Fields::Header),
                        builder->CreateExtractValue(
                            newRowRef,
                            TypeBuilder<TRow, false>::Fields::Header));

                    TCGContext innerBuilder(builder, bufferRef);

                    CodegenIf<TCGContext>(
                        innerBuilder,
                        inserted,
                        [&] (TCGContext& builder) {
                            codegenInitialize(builder, groupRow);

                            builder->CreateCall(
                                builder.Module->GetRoutine("AllocatePermanentRow"),
                                {
                                    builder.GetExecutionContext(),
                                    bufferRef,
                                    builder->getInt32(groupRowSize),
                                    newRowPtrRef
                                });
                        });

                    // Here *newRowPtrRef != groupRow.
                    if (!isMerge) {
                        auto newRow = builder->CreateLoad(newRowPtrRef);
                        codegenEvaluateAggregateArgs(builder, row, newRow);
                        codegenUpdate(innerBuilder, newRow, groupRow);
                    } else {
                        codegenUpdate(innerBuilder, row, groupRow);
                    }

                };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consume = MakeClosure<void(TRowBuffer*, TRow*, i64)>(builder, "ConsumeGroupedRows", [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* finalGroupedRows,
            Value* size
        ) {
            TCGContext innerBuilder(builder, buffer);
            CodegenForEachRow(
                innerBuilder,
                finalGroupedRows,
                size,
                builder[consumerSlot]);

            innerBuilder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("GroupOpHelper"),
            {
                builder.GetExecutionContext(),
                CodegenGroupHasherFunction(keyTypes, *builder.Module),
                CodegenGroupComparerFunction(keyTypes, *builder.Module),
                builder->getInt32(keyTypes.size()),
                builder->getInt8(checkNulls),

                collect.ClosurePtr,
                collect.Function,

                consume.ClosurePtr,
                consume.Function,
            });

    };

    return consumerSlot;
}

size_t MakeCodegenFinalizeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::function<void(TCGContext&, Value*)> codegenFinalize)
{
    // codegenFinalize calls the aggregates' finalize functions if needed

    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(codegenFinalize),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            codegenFinalize(builder, row);
            builder[consumerSlot](builder, row);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenOrderOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::vector<TCodegenExpression> codegenExprs,
    std::vector<EValueType> orderColumnTypes,
    std::vector<EValueType> sourceSchema,
    const std::vector<bool>& isDesc)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        isDesc,
        MOVE(codegenExprs),
        MOVE(orderColumnTypes),
        MOVE(sourceSchema),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto schemaSize = sourceSchema.size();

        auto collectRows = MakeClosure<void(TTopCollector*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* topCollector
        ) {
            Value* newRow = CodegenAllocateRow(builder, schemaSize + codegenExprs.size());

            builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                Value* topCollectorRef = builder->ViaClosure(topCollector);
                Value* newRowRef = builder->ViaClosure(newRow);

                for (size_t index = 0; index < schemaSize; ++index) {
                    auto type = sourceSchema[index];
                    TCGValue::CreateFromRow(
                        builder,
                        row,
                        index,
                        type)
                        .StoreToRow(builder, newRowRef, index, index);
                }

                for (size_t index = 0; index < codegenExprs.size(); ++index) {
                    auto columnIndex = schemaSize + index;

                    auto orderValue = codegenExprs[index](builder, row);

                    orderValue.StoreToRow(builder, newRowRef, columnIndex, columnIndex);
                }

                builder->CreateCall(
                    builder.Module->GetRoutine("AddRow"),
                    {topCollectorRef, newRowRef});
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consumeOrderedRows = MakeClosure<void(TRowBuffer*, TRow*, i64)>(builder, "ConsumeOrderedRows", [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* orderedRows,
            Value* size
        ) {
            TCGContext innerBuilder(builder, buffer);
            CodegenForEachRow(
                innerBuilder,
                orderedRows,
                size,
                builder[consumerSlot]);

            builder->CreateRetVoid();
        });

        std::vector<std::function<TCGValue(TCGIRBuilderPtr& builder, Value* row)>> compareArgs;
        for (int index = 0; index < codegenExprs.size(); ++index) {
            auto columnIndex = schemaSize + index;
            auto type = orderColumnTypes[index];

            compareArgs.push_back([columnIndex, type] (TCGIRBuilderPtr& builder, Value* row) {
                return TCGValue::CreateFromRow(
                    builder,
                    row,
                    columnIndex,
                    type);
            });
        }

        builder->CreateCall(
            builder.Module->GetRoutine("OrderOpHelper"),
            {
                builder.GetExecutionContext(),
                CodegenTupleComparerFunction(compareArgs, *builder.Module, isDesc),

                collectRows.ClosurePtr,
                collectRows.Function,

                consumeOrderedRows.ClosurePtr,
                consumeOrderedRows.Function,
                builder->getInt32(schemaSize)
            });
    };

    return consumerSlot;
}

void MakeCodegenWriteOp(
    TCodegenSource* codegenSource,
    size_t producerSlot)
{
    *codegenSource = [
        producerSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TWriteOpClosure*)>(builder, "WriteOpInner", [&] (
            TCGOperatorContext& builder,
            Value* writeRowClosure
        ) {
            builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                Value* writeRowClosureRef = builder->ViaClosure(writeRowClosure);
                builder->CreateCall(
                    builder.Module->GetRoutine("WriteRow"),
                    {builder.GetExecutionContext(), writeRowClosureRef, row});
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("WriteOpHelper"),
            {
                builder.GetExecutionContext(),
                collect.ClosurePtr,
                collect.Function
            });
    };
}

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    const TCodegenSource* codegenSource,
    size_t slotCount,
    size_t opaqueValuesCount)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    const auto entryFunctionName = TString("EvaluateQuery");

    MakeFunction<TCGQuerySignature>(module->GetModule(), entryFunctionName.c_str(), [&] (
        TCGIRBuilderPtr& baseBuilder,
        Value* opaqueValuesPtr,
        Value* executionContextPtr
    ) {
        std::vector<std::shared_ptr<TCodegenConsumer>> consumers(slotCount);

        TCGOperatorContext builder(
            TCGBaseContext(baseBuilder, opaqueValuesPtr, opaqueValuesCount, module),
            executionContextPtr,
            &consumers);

        (*codegenSource)(builder);

        builder->CreateRetVoid();
    });

    module->ExportSymbol(entryFunctionName);
    return module->GetCompiledFunction<TCGQuerySignature>(entryFunctionName);
}

TCGExpressionCallback CodegenExpression(TCodegenExpression codegenExpression, size_t opaqueValuesCount)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    const auto entryFunctionName = TString("EvaluateExpression");

    MakeFunction<TCGExpressionSignature>(module->GetModule(), entryFunctionName.c_str(), [&] (
        TCGIRBuilderPtr& baseBuilder,
        Value* opaqueValuesPtr,
        Value* resultPtr,
        Value* inputRow,
        Value* buffer
    ) {
        TCGExprContext builder(TCGBaseContext(baseBuilder, opaqueValuesPtr, opaqueValuesCount, module), buffer);

        auto result = codegenExpression(builder, inputRow);
        result.StoreToValue(builder, resultPtr, 0, "writeResult");
        builder->CreateRetVoid();
    });

    module->ExportSymbol(entryFunctionName);

    return module->GetCompiledFunction<TCGExpressionSignature>(entryFunctionName);
}

TCGAggregateCallbacks CodegenAggregate(TCodegenAggregate codegenAggregate)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());

    const auto initName = TString("init");
    {
        MakeFunction<TCGAggregateInitSignature>(module->GetModule(), initName.c_str(), [&] (
            TCGIRBuilderPtr& baseBuilder,
            Value* buffer,
            Value* resultPtr
        ) {
            TCGExprContext builder(TCGBaseContext(baseBuilder, nullptr, 0, module), buffer);

            auto result = codegenAggregate.Initialize(builder, nullptr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(initName);
    }

    const auto updateName = TString("update");
    {
        MakeFunction<TCGAggregateUpdateSignature>(module->GetModule(), updateName.c_str(), [&] (
            TCGIRBuilderPtr& baseBuilder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr,
            Value* newValuePtr
        ) {
            TCGExprContext builder(TCGBaseContext(baseBuilder, nullptr, 0, module), buffer);

            auto result = codegenAggregate.Update(builder, statePtr, newValuePtr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(updateName);
    }

    const auto mergeName = TString("merge");
    {
        MakeFunction<TCGAggregateMergeSignature>(module->GetModule(), mergeName.c_str(), [&] (
            TCGIRBuilderPtr& baseBuilder,
            Value* buffer,
            Value* resultPtr,
            Value* dstStatePtr,
            Value* statePtr
        ) {
            TCGExprContext builder(TCGBaseContext(baseBuilder, nullptr, 0, module), buffer);

            auto result = codegenAggregate.Merge(builder, dstStatePtr, statePtr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(mergeName);
    }

    const auto finalizeName = TString("finalize");
    {
        MakeFunction<TCGAggregateFinalizeSignature>(module->GetModule(), finalizeName.c_str(), [&] (
            TCGIRBuilderPtr& baseBuilder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr
        ) {
            TCGExprContext builder(TCGBaseContext(baseBuilder, nullptr, 0, module), buffer);

            auto result = codegenAggregate.Finalize(builder, statePtr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(finalizeName);
    }

    return TCGAggregateCallbacks{
        module->GetCompiledFunction<TCGAggregateInitSignature>(initName),
        module->GetCompiledFunction<TCGAggregateUpdateSignature>(updateName),
        module->GetCompiledFunction<TCGAggregateMergeSignature>(mergeName),
        module->GetCompiledFunction<TCGAggregateFinalizeSignature>(finalizeName)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

