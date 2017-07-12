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
    Value* newRowPtr = builder->CreateAlloca(
        TypeBuilder<TRow, false>::get(builder->getContext()),
        nullptr,
        "allocatedRow");

    size_t size = sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue) * valueCount;

    Value* newRowData = builder->CreateAlignedAlloca(
        TypeBuilder<char, false>::get(builder->getContext()),
        8,
        builder->getInt32(size),
        "allocatedValues");

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

void CheckNaN(const TCGModulePtr& module, TCGIRBuilderPtr& builder, Value* lhsValue, Value* rhsValue)
{
    CodegenIf<TCGIRBuilderPtr&>(
        builder,
        builder->CreateFCmpUNO(lhsValue, rhsValue),
        [&] (TCGIRBuilderPtr& builder) {
            builder->CreateCall(
                module->GetRoutine("ThrowQueryException"),
                {
                    builder->CreateGlobalStringPtr("Comparison with NaN")
                });
        });
}

Function* CodegenGroupComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    return MakeFunction<TComparerFunction>(module, "GroupComparer", [&] (
        TCGBaseContext& builder,
        Value* lhsRow,
        Value* rhsRow
    ) {
        Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
        Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

        auto returnIf = [&] (Value* condition) {
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            builder->CreateCondBr(condition, thenBB, elseBB);
            builder->SetInsertPoint(thenBB);
            builder->CreateRet(builder->getInt8(0));
            builder->SetInsertPoint(elseBB);
        };

        auto codegenEqualOp = [&] (size_t index) {
            auto lhsValue = TCGValue::CreateFromRowValues(
                builder,
                lhsValues,
                index,
                types[index]);

            auto rhsValue = TCGValue::CreateFromRowValues(
                builder,
                rhsValues,
                index,
                types[index]);

            CodegenIf<TCGIRBuilderPtr>(
                builder,
                builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                [&] (TCGIRBuilderPtr& builder) {
                    returnIf(builder->CreateICmpNE(lhsValue.IsNull(builder), rhsValue.IsNull(builder)));
                },
                [&] (TCGIRBuilderPtr& builder) {
                    auto* lhsData = lhsValue.GetData(builder);
                    auto* rhsData = rhsValue.GetData(builder);

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
                            Value* lhsLength = lhsValue.GetLength(builder);
                            Value* rhsLength = rhsValue.GetLength(builder);

                            Value* minLength = builder->CreateSelect(
                                builder->CreateICmpULT(lhsLength, rhsLength),
                                lhsLength,
                                rhsLength);

                            Value* cmpResult = builder->CreateCall(
                                module->GetRoutine("memcmp"),
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
    const TCGModulePtr& module)
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
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    return MakeFunction<THasherFunction>(module, "GroupHasher", [&] (
        TCGBaseContext& builder,
        Value* row
    ) {
        Value* values = CodegenValuesPtrFromRow(builder, row);
        auto codegenHashOp = [&] (size_t index, TCGIRBuilderPtr& builder) -> Value* {
            auto value = TCGValue::CreateFromRowValues(
                builder,
                values,
                index,
                types[index]);

            auto* conditionBB = builder->CreateBBHere("condition");
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            auto* endBB = builder->CreateBBHere("end");

            builder->CreateBr(conditionBB);

            builder->SetInsertPoint(conditionBB);
            builder->CreateCondBr(value.IsNull(builder), elseBB, thenBB);
            conditionBB = builder->GetInsertBlock();

            builder->SetInsertPoint(thenBB);

            Value* thenResult;

            auto intType = TDataTypeBuilder::TInt64::get(builder->getContext());

            switch (value.GetStaticType()) {
                case EValueType::Boolean:
                case EValueType::Int64:
                case EValueType::Uint64:
                case EValueType::Double:
                    thenResult = CodegenFingerprint64(
                        builder,
                        builder->CreateZExtOrBitCast(value.GetData(builder), intType));
                    break;

                case EValueType::String:
                    thenResult = builder->CreateCall(
                        module->GetRoutine("StringHash"),
                        {
                            value.GetData(builder),
                            value.GetLength(builder)
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
    const TCGModulePtr& module)
{
    return CodegenGroupHasherFunction(types, module, 0, types.size());
}

Function* CodegenTupleComparerFunction(
    const std::vector<std::function<TCGValue(TCGIRBuilderPtr& builder, Value* values)>>& codegenArgs,
    const TCGModulePtr& module,
    const std::vector<bool>& isDesc = std::vector<bool>())
{
    return MakeFunction<TComparerFunction>(module, "RowComparer", [&] (
        TCGBaseContext& builder,
        Value* lhsRow,
        Value* rhsRow
    ) {
        Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
        Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

        auto returnIf = [&] (Value* condition, auto codegenInner) {
            auto* thenBB = builder->CreateBBHere("then");
            auto* elseBB = builder->CreateBBHere("else");
            builder->CreateCondBr(condition, thenBB, elseBB);
            builder->SetInsertPoint(thenBB);
            builder->CreateRet(builder->CreateSelect(codegenInner(builder), builder->getInt8(1), builder->getInt8(0)));
            builder->SetInsertPoint(elseBB);
        };

        auto codegenEqualOrLessOp = [&] (int index) {
            const auto& codegenArg = codegenArgs[index];
            auto lhsValue = codegenArg(builder, lhsValues);
            auto rhsValue = codegenArg(builder, rhsValues);

            if (index < isDesc.size() && isDesc[index]) {
                std::swap(lhsValue, rhsValue);
            }

            auto type = lhsValue.GetStaticType();

            YCHECK(type == rhsValue.GetStaticType());

            CodegenIf<TCGIRBuilderPtr>(
                builder,
                builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                [&] (TCGIRBuilderPtr& builder) {
                    returnIf(
                        builder->CreateICmpNE(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                        [&] (TCGIRBuilderPtr& builder) {
                            return builder->CreateICmpUGT(lhsValue.IsNull(builder), rhsValue.IsNull(builder));
                        });
                },
                [&] (TCGIRBuilderPtr& builder) {
                    auto* lhsData = lhsValue.GetData(builder);
                    auto* rhsData = rhsValue.GetData(builder);

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
                            Value* lhsLength = lhsValue.GetLength(builder);
                            Value* rhsLength = rhsValue.GetLength(builder);

                            Value* minLength = builder->CreateSelect(
                                builder->CreateICmpULT(lhsLength, rhsLength),
                                lhsLength,
                                rhsLength);

                            Value* cmpResult = builder->CreateCall(
                                module->GetRoutine("memcmp"),
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
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    std::vector<std::function<TCGValue(TCGIRBuilderPtr& builder, Value* row)>> compareArgs;
    for (int index = start; index < finish; ++index) {
        compareArgs.push_back([index, type = types[index]] (TCGIRBuilderPtr& builder, Value* values) {
            return TCGValue::CreateFromRowValues(builder, values, index, type);
        });
    }

    return CodegenTupleComparerFunction(compareArgs, module);
}

Function* CodegenRowComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module)
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

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    EValueType type)
{
    return [
            index,
            type
        ] (TCGExprContext& builder) {
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
        ] (TCGExprContext& builder) {
            return TCGValue::CreateFromRowValues(
                builder,
                builder.RowValues,
                index,
                type,
                "reference." + Twine(name.c_str()));
        };
}

llvm::StringRef ToStringRef(const TString& stroka)
{
    return llvm::StringRef(stroka.c_str(), stroka.length());
}

TCGValue CodegenFragment(
    TCGExprContext& builder,
    size_t id)
{
    const auto& expressionFragment = builder.ExpressionFragments.Items[id];

    if (expressionFragment.IsOutOfLine()) {
        builder->CreateCall(
            builder.ExpressionFragments.Functions[expressionFragment.Index],
            {
                builder.GetExpressionClosurePtr()
            });

        return TCGValue::CreateFromLlvmValue(
            builder,
            builder.GetFragmentResult(id),
            expressionFragment.Type,
            "fragment." + Twine(id))
            .Steal();
    } else {
        return expressionFragment.Generator(builder);
    }
}

void CodegenFragmentBodies(
    const TCGModulePtr& module,
    TCodegenFragmentInfos& fragmentInfos)
{
    const auto& namePrefix = fragmentInfos.NamePrefix;

    for (size_t id = 0; id < fragmentInfos.Items.size(); ++id) {
        if (fragmentInfos.Items[id].IsOutOfLine()) {
            auto name = Format("%v#%v", namePrefix, id);

            FunctionType* functionType = FunctionType::get(
                TypeBuilder<void, false>::get(module->GetModule()->getContext()),
                llvm::PointerType::getUnqual(
                    TypeBuilder<TExpressionClosure, false>::get(
                        module->GetModule()->getContext(),
                        fragmentInfos.Functions.size())),
                true);

            Function* function =  Function::Create(
                functionType,
                Function::ExternalLinkage,
                name.c_str(),
                module->GetModule());

            function->addFnAttr(llvm::Attribute::AttrKind::UWTable);

            Value* expressionClosure = function->arg_begin();
            {
                TCGIRBuilder irBuilder(function);
                auto innerBuilder = TCGExprContext::Make(
                    TCGBaseContext(TCGIRBuilderPtr(&irBuilder), module),
                    fragmentInfos,
                    expressionClosure);

                auto* evaluationNeeded = innerBuilder->CreateICmpEQ(
                    innerBuilder->CreateLoad(innerBuilder.GetFragmentFlag(id)),
                    innerBuilder->getInt8(false));

                CodegenIf<TCGExprContext>(
                    innerBuilder,
                    evaluationNeeded,
                    [&] (TCGExprContext& builder) {
                        builder.ExpressionFragments.Items[id].Generator(builder)
                            .StoreToValue(builder, builder.GetFragmentResult(id));

                        builder->CreateStore(builder->getInt8(true), builder.GetFragmentFlag(id));
                    });

                innerBuilder->CreateRetVoid();
            }

            function->addFnAttr(llvm::Attribute::AttrKind::NoInline);

            fragmentInfos.Functions[fragmentInfos.Items[id].Index] = function;
        }
    }
}

void MakeCodegenFragmentBodies(
    TCodegenSource* codegenSource,
    TCodegenFragmentInfosPtr fragmentInfos)
{
    *codegenSource = [
        MOVE(fragmentInfos),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        CodegenFragmentBodies(builder.Module, *fragmentInfos);
        codegenSource(builder);
    };
}

TCodegenSimpleValue MakeCodegenFunctionContext(
    int index)
{
    return [
            index
        ] (TCGExprContext& builder) {
            return builder.GetOpaqueValue(index);
        };
}

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    size_t operandId,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(operandId),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder) {
        auto operandValue = CodegenFragment(builder, operandId);

        return CodegenIf<TCGIRBuilderPtr, TCGValue>(
            builder,
            operandValue.IsNull(builder),
            [&] (TCGIRBuilderPtr& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGIRBuilderPtr& builder) {
                auto operandType = operandValue.GetStaticType();
                Value* operandData = operandValue.GetData(builder);
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
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(lhsId),
        MOVE(rhsId),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder) {
        auto compare = [&] (bool parameter) {
            auto lhsValue = CodegenFragment(builder, lhsId);
            auto rhsValue = CodegenFragment(builder, rhsId);

            Value* lhsIsNull = lhsValue.IsNull(builder);
            Value* rhsIsNull = rhsValue.IsNull(builder);

            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                lhsIsNull,
                [&] (TCGExprContext& builder) {
                    return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                        builder,
                        rhsIsNull,
                        [&] (TCGIRBuilderPtr& builder) {
                            return TCGValue::CreateNull(builder, type);
                        },
                        [&] (TCGIRBuilderPtr& builder) {
                            Value* rhsData = rhsValue.GetData(builder);
                            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                                builder,
                                builder->CreateICmpEQ(rhsData, builder->getInt8(parameter)),
                                [&] (TCGIRBuilderPtr& builder) {
                                    return rhsValue;
                                },
                                [&] (TCGIRBuilderPtr& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                });
                        });
                },
                [&] (TCGExprContext& builder) {
                    Value* lhsData = lhsValue.GetData(builder);
                    return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                        builder,
                        builder->CreateICmpEQ(lhsData, builder->getInt8(parameter)),
                        [&] (TCGIRBuilderPtr& builder) {
                            return lhsValue;
                        },
                        [&] (TCGIRBuilderPtr& builder) {
                            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                                builder,
                                rhsIsNull,
                                [&] (TCGIRBuilderPtr& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                },
                                [&] (TCGIRBuilderPtr& builder) {
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
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(lhsId),
        MOVE(rhsId),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder) {
        auto nameTwine = Twine(name.c_str());
        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        #define CMP_OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->CreateZExtOrBitCast( \
                    builder->Create##optype(lhsData, rhsData), \
                    TDataTypeBuilder::TBoolean::get(builder->getContext())); \
                break;

        auto compareNulls = [&] () {
            Value* lhsData = lhsValue.IsNull(builder);
            Value* rhsData = rhsValue.IsNull(builder);
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
            builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
            [&] (TCGBaseContext& builder) {
                return compareNulls();
            },
            [&] (TCGBaseContext& builder) {
                YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
                auto operandType = lhsValue.GetStaticType();

                Value* lhsData = lhsValue.GetData(builder);
                Value* rhsData = rhsValue.GetData(builder);
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
                        Value* lhsLength = lhsValue.GetLength(builder);
                        Value* rhsLength = rhsValue.GetLength(builder);

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
            },
            nameTwine);

            #undef CMP_OP
    };
}

TCodegenExpression MakeCodegenArithmeticBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name)
{
    return [
        MOVE(opcode),
        MOVE(lhsId),
        MOVE(rhsId),
        MOVE(type),
        MOVE(name)
    ] (TCGExprContext& builder) {
        auto nameTwine = Twine(name.c_str());

        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
        auto operandType = lhsValue.GetStaticType();

        #define OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->Create##optype(lhsData, rhsData); \
                break;

        if ((opcode == EBinaryOp::Divide || opcode == EBinaryOp::Modulo) &&
            (operandType == EValueType::Uint64 || operandType == EValueType::Int64))
        {
            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                [&] (TCGExprContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                [&] (TCGBaseContext& builder) {
                    Value* lhsData = lhsValue.GetData(builder);
                    Value* rhsData = rhsValue.GetData(builder);
                    Value* evalData = nullptr;

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

                    checkZero(rhsData);

                    switch (operandType) {
                        case EValueType::Int64:
                            switch (opcode) {
                                OP(Divide, SDiv)
                                OP(Modulo, SRem)
                                default:
                                    Y_UNREACHABLE();
                            }
                            break;
                        case EValueType::Uint64:
                            switch (opcode) {
                                OP(Divide, UDiv)
                                OP(Modulo, URem)
                                default:
                                    Y_UNREACHABLE();
                            }
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
                nameTwine);
        } else {
            Value* lhsData = lhsValue.GetData(builder);
            Value* rhsData = rhsValue.GetData(builder);
            Value* evalData = nullptr;

            switch (operandType) {
                case EValueType::Int64:
                    switch (opcode) {
                        OP(Plus, Add)
                        OP(Minus, Sub)
                        OP(Multiply, Mul)
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

            return TCGValue::CreateFromValue(
                builder,
                builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                nullptr,
                evalData,
                type);
        }

        #undef OP
    };
}

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name)
{
    if (IsLogicalBinaryOp(opcode)) {
        return MakeCodegenLogicalBinaryOpExpr(
            opcode,
            lhsId,
            rhsId,
            type,
            std::move(name));
    } else if (IsRelationalBinaryOp(opcode)) {
        return MakeCodegenRelationalBinaryOpExpr(
            opcode,
            lhsId,
            rhsId,
            type,
            std::move(name));
    } else {
        return MakeCodegenArithmeticBinaryOpExpr(
            opcode,
            lhsId,
            rhsId,
            type,
            std::move(name));
    }
}

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<size_t> argIds,
    int arrayIndex)
{
    return [
        MOVE(argIds),
        MOVE(arrayIndex)
    ] (TCGExprContext& builder) {
        size_t keySize = argIds.size();

        Value* newRow = CodegenAllocateRow(builder, keySize);
        Value* newValues = CodegenValuesPtrFromRow(builder, newRow);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            keyTypes.push_back(value.GetStaticType());
            value.StoreToValues(builder, newValues, index);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("IsRowInArray"),
            {
                CodegenRowComparerFunction(keyTypes, builder.Module),
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

            auto headerPtr = builder->CreateExtractValue(
                row,
                TypeBuilder<TRow, false>::Fields::Header,
                Twine("row.headerPtr"));

            auto countPtr = builder->CreateStructGEP(
                nullptr,
                headerPtr,
                TypeBuilder<TRowHeader, false>::Fields::Count,
                Twine("headerPtr.count"));

            builder->CreateStore(builder->getInt32(streamIndex), countPtr);

            auto switcher = builder->CreateSwitch(streamIndexValue.GetData(builder), endIfBB);

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
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<std::pair<size_t, bool>> equations,
    size_t commonKeyPrefix)
{
    size_t consumerSlot = (*slotCount)++;
    *codegenSource = [
        consumerSlot,
        producerSlot,
        index,
        MOVE(fragmentInfos),
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
                Value* keyValues = CodegenValuesPtrFromRow(builder, keyRef);

                auto rowBuilder = TCGExprContext::Make(builder, *fragmentInfos, row, builder.Buffer);
                for (int column = 0; column < lookupKeySize; ++column) {
                    if (!equations[column].second) {
                        auto joinKeyValue = CodegenFragment(rowBuilder, equations[column].first);
                        lookupKeyTypes[column] = joinKeyValue.GetStaticType();
                        joinKeyValue.StoreToValues(rowBuilder, keyValues, column);
                    }
                }

                builder->CreateStore(
                    keyValues,
                    builder->CreateStructGEP(
                        nullptr,
                        rowBuilder.GetExpressionClosurePtr(),
                        TypeBuilder<TExpressionClosure, false>::Fields::RowValues));

                TCGExprContext evaluatedColumnsBuilder(builder, TCGExprData{
                    *fragmentInfos,
                    rowBuilder.Buffer,
                    CodegenValuesPtrFromRow(builder, keyRef),
                    rowBuilder.ExpressionClosurePtr});

                for (int column = 0; column < lookupKeySize; ++column) {
                    if (equations[column].second) {
                        auto evaluatedColumn = CodegenFragment(
                            evaluatedColumnsBuilder,
                            equations[column].first);
                        lookupKeyTypes[column] = evaluatedColumn.GetStaticType();
                        evaluatedColumn.StoreToValues(evaluatedColumnsBuilder, keyValues, column);
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

        const auto& module = builder.Module;

        builder->CreateCall(
            module->GetRoutine("JoinOpHelper"),
            {
                builder.GetExecutionContext(),
                builder.GetOpaqueValue(index),

                CodegenGroupHasherFunction(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                CodegenGroupComparerFunction(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                CodegenRowComparerFunction(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),

                CodegenGroupComparerFunction(lookupKeyTypes, module, 0, commonKeyPrefix),
                CodegenGroupComparerFunction(lookupKeyTypes, module),
                CodegenRowComparerFunction(lookupKeyTypes, module),

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
    TCodegenFragmentInfosPtr fragmentInfos,
    size_t predicateId)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(fragmentInfos),
        predicateId,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            auto rowBuilder = TCGExprContext::Make(builder, *fragmentInfos, row, builder.Buffer);
            auto predicateResult = CodegenFragment(rowBuilder, predicateId);


            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            auto* notIsNull = builder->CreateNot(predicateResult.IsNull(builder));
            auto* isTrue = builder->CreateICmpEQ(predicateResult.GetData(builder), builder->getInt8(true));

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
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(sourceSchema),
        value,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        Value* newRow = CodegenAllocateRow(builder, sourceSchema.size() + 1);

        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            Value* newRowRef = builder->ViaClosure(newRow);
            Value* newValues = CodegenValuesPtrFromRow(builder, newRowRef);

            for (int index = 0; index < sourceSchema.size(); ++index) {
                auto id = index;

                TCGValue::CreateFromRow(builder, row, index, sourceSchema[index])
                    .StoreToValues(builder, newValues, index, id);
            }

            TCGValue::CreateFromValue(
                builder,
                builder->getInt1(false),
                nullptr,
                builder->getInt64(static_cast<ui64>(value)),
                EValueType::Uint64,
                "streamIndex")
                .StoreToValues(builder, newValues, sourceSchema.size(), sourceSchema.size());

            builder[consumerSlot](builder, newRowRef);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenProjectOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> argIds)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        MOVE(fragmentInfos),
        MOVE(argIds),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        int projectionCount = argIds.size();

        Value* newRow = CodegenAllocateRow(builder, projectionCount);

        builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
            Value* newRowRef = builder->ViaClosure(newRow);
            Value* newValues = CodegenValuesPtrFromRow(builder, newRowRef);

            auto innerBuilder = TCGExprContext::Make(builder, *fragmentInfos, row, builder.Buffer);

            for (int index = 0; index < projectionCount; ++index) {
                auto id = index;

                CodegenFragment(innerBuilder, argIds[index])
                    .StoreToValues(innerBuilder, newValues, index, id);
            }

            builder[consumerSlot](builder, newRowRef);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateGroups(
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> groupExprsIds,
    std::vector<EValueType> nullTypes)
{
    return [
        MOVE(fragmentInfos),
        MOVE(groupExprsIds),
        MOVE(nullTypes)
    ] (TCGContext& builder, Value* srcRow, Value* dstRow) {
        auto innerBuilder = TCGExprContext::Make(builder, *fragmentInfos, srcRow, builder.Buffer);

        Value* dstValues = CodegenValuesPtrFromRow(builder, dstRow);

        for (int index = 0; index < groupExprsIds.size(); index++) {
            CodegenFragment(innerBuilder, groupExprsIds[index])
                .StoreToValues(builder, dstValues, index, index);
        }

        size_t offset = groupExprsIds.size();
        for (int index = 0; index < nullTypes.size(); ++index) {
            TCGValue::CreateNull(builder, nullTypes[index])
                .StoreToValues(builder, dstValues, offset + index, offset + index);
        }
    };
}

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateAggregateArgs(
    size_t keySize,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> aggregateExprIds)
{
    return [
        keySize,
        MOVE(fragmentInfos),
        MOVE(aggregateExprIds)
    ] (TCGContext& builder, Value* srcRow, Value* dstRow) {
        Value* dstValues = CodegenValuesPtrFromRow(builder, dstRow);

        auto innerBuilder = TCGExprContext::Make(builder, *fragmentInfos, srcRow, builder.Buffer);

        for (int index = 0; index < aggregateExprIds.size(); index++) {
            CodegenFragment(innerBuilder, aggregateExprIds[index])
                .StoreToValues(builder, dstValues, keySize + index);
        }
    };
}

std::function<void(TCGBaseContext& builder, Value*, Value*)> MakeCodegenAggregateInitialize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize)
{
    return [
        MOVE(codegenAggregates),
        keySize
    ] (TCGBaseContext& builder, Value* buffer, Value* row) {
        Value* values = CodegenValuesPtrFromRow(builder, row);

        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto id = keySize + index;
            codegenAggregates[index].Initialize(builder, buffer, row)
                .StoreToValues(builder, values, keySize + index, id);
        }
    };
}

std::function<void(TCGBaseContext& builder, Value*, Value*, Value*)> MakeCodegenAggregateUpdate(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize,
    bool isMerge)
{
    return [
        MOVE(codegenAggregates),
        keySize,
        isMerge
    ] (TCGBaseContext& builder, Value* buffer, Value* newRow, Value* groupRow) {
        Value* newValues = CodegenValuesPtrFromRow(builder, newRow);
        Value* groupValues = CodegenValuesPtrFromRow(builder, groupRow);

        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto aggState = builder->CreateConstInBoundsGEP1_32(
                nullptr,
                groupValues,
                keySize + index);
            auto newValue = builder->CreateConstInBoundsGEP1_32(
                nullptr,
                newValues,
                keySize + index);

            auto id = keySize + index;
            TCodegenAggregateUpdate updateFunction;
            if (isMerge) {
                updateFunction = codegenAggregates[index].Merge;
            } else {
                updateFunction = codegenAggregates[index].Update;
            }
            updateFunction(builder, buffer, aggState, newValue)
                .StoreToValues(builder, groupValues, keySize + index, id);
        }
    };
}

std::function<void(TCGBaseContext& builder, Value*, Value*)> MakeCodegenAggregateFinalize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize)
{
    return [
        MOVE(codegenAggregates),
        keySize
    ] (TCGBaseContext& builder, Value* buffer, Value* row) {
        Value* values = CodegenValuesPtrFromRow(builder, row);

        for (int index = 0; index < codegenAggregates.size(); index++) {
            auto id = keySize + index;
            auto resultValue = codegenAggregates[index].Finalize(
                builder,
                buffer,
                builder->CreateConstInBoundsGEP1_32(
                    nullptr,
                    values,
                    keySize + index));
            resultValue.StoreToValues(builder, values, keySize + index, id);
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
    std::function<void(TCGBaseContext&, Value*, Value*)> codegenInitialize,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateGroups,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateAggregateArgs,
    std::function<void(TCGBaseContext&, Value*, Value*, Value*)> codegenUpdate,
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
                        builder,
                        inserted,
                        [&] (TCGContext& builder) {
                            codegenInitialize(builder, bufferRef, groupRow);

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
                        codegenUpdate(builder, bufferRef, newRow, groupRow);
                    } else {
                        codegenUpdate(builder, bufferRef, row, groupRow);
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
                CodegenGroupHasherFunction(keyTypes, builder.Module),
                CodegenGroupComparerFunction(keyTypes, builder.Module),
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
    std::function<void(TCGBaseContext&, Value*, Value*)> codegenFinalize)
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
            codegenFinalize(builder, builder.Buffer, row);
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
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> exprIds,
    std::vector<EValueType> orderColumnTypes,
    std::vector<EValueType> sourceSchema,
    const std::vector<bool>& isDesc)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        isDesc,
        MOVE(fragmentInfos),
        MOVE(exprIds),
        MOVE(orderColumnTypes),
        MOVE(sourceSchema),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto schemaSize = sourceSchema.size();

        auto collectRows = MakeClosure<void(TTopCollector*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* topCollector
        ) {
            Value* newRow = CodegenAllocateRow(builder, schemaSize + exprIds.size());

            builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                Value* topCollectorRef = builder->ViaClosure(topCollector);
                Value* newRowRef = builder->ViaClosure(newRow);
                Value* values = CodegenValuesPtrFromRow(builder, row);
                Value* newValues = CodegenValuesPtrFromRow(builder, newRowRef);

                for (size_t index = 0; index < schemaSize; ++index) {
                    auto type = sourceSchema[index];
                    TCGValue::CreateFromRowValues(
                        builder,
                        values,
                        index,
                        type)
                        .StoreToValues(builder, newValues, index, index);
                }

                auto innerBuilder = TCGExprContext::Make(builder, *fragmentInfos, row, builder.Buffer);
                for (size_t index = 0; index < exprIds.size(); ++index) {
                    auto columnIndex = schemaSize + index;

                    CodegenFragment(innerBuilder, exprIds[index])
                        .StoreToValues(builder, newValues, columnIndex, columnIndex);
                }

                builder->CreateCall(
                    builder.Module->GetRoutine("AddRow"),
                    {
                        topCollectorRef,
                        newRowRef
                    });
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
        for (int index = 0; index < exprIds.size(); ++index) {
            auto columnIndex = schemaSize + index;
            auto type = orderColumnTypes[index];

            compareArgs.push_back([columnIndex, type] (TCGIRBuilderPtr& builder, Value* values) {
                return TCGValue::CreateFromRowValues(
                    builder,
                    values,
                    columnIndex,
                    type);
            });
        }

        builder->CreateCall(
            builder.Module->GetRoutine("OrderOpHelper"),
            {
                builder.GetExecutionContext(),
                CodegenTupleComparerFunction(compareArgs, builder.Module, isDesc),

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
    size_t slotCount)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    const auto entryFunctionName = TString("EvaluateQuery");

    MakeFunction<TCGQuerySignature>(module, entryFunctionName.c_str(), [&] (
        TCGBaseContext& baseBuilder,
        Value* opaqueValuesPtr,
        Value* executionContextPtr
    ) {
        std::vector<std::shared_ptr<TCodegenConsumer>> consumers(slotCount);

        TCGOperatorContext builder(
            TCGOpaqueValuesContext(baseBuilder, opaqueValuesPtr),
            executionContextPtr,
            &consumers);

        (*codegenSource)(builder);

        builder->CreateRetVoid();
    });

    module->ExportSymbol(entryFunctionName);
    return module->GetCompiledFunction<TCGQuerySignature>(entryFunctionName);
}

TCGExpressionCallback CodegenStandaloneExpression(
    const TCodegenFragmentInfosPtr& fragmentInfos,
    size_t exprId)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    const auto entryFunctionName = TString("EvaluateExpression");

    CodegenFragmentBodies(module, *fragmentInfos);

    MakeFunction<TCGExpressionSignature>(module, entryFunctionName.c_str(), [&] (
        TCGBaseContext& baseBuilder,
        Value* opaqueValuesPtr,
        Value* resultPtr,
        Value* inputRow,
        Value* buffer
    ) {
        auto builder = TCGExprContext::Make(
            TCGOpaqueValuesContext(baseBuilder, opaqueValuesPtr),
            *fragmentInfos,
            inputRow,
            buffer);

        CodegenFragment(builder, exprId)
            .StoreToValue(builder, resultPtr, 0, "writeResult");
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
        MakeFunction<TCGAggregateInitSignature>(module, initName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr
        ) {
            codegenAggregate.Initialize(builder, buffer, nullptr)
                .StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(initName);
    }

    const auto updateName = TString("update");
    {
        MakeFunction<TCGAggregateUpdateSignature>(module, updateName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr,
            Value* newValuePtr
        ) {
            auto result = codegenAggregate.Update(builder, buffer, statePtr, newValuePtr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(updateName);
    }

    const auto mergeName = TString("merge");
    {
        MakeFunction<TCGAggregateMergeSignature>(module, mergeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr,
            Value* dstStatePtr,
            Value* statePtr
        ) {
            auto result = codegenAggregate.Merge(builder, buffer, dstStatePtr, statePtr);
            result.StoreToValue(builder, resultPtr, 0, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(mergeName);
    }

    const auto finalizeName = TString("finalize");
    {
        MakeFunction<TCGAggregateFinalizeSignature>(module, finalizeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr
        ) {
            auto result = codegenAggregate.Finalize(builder, buffer, statePtr);
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

