#include "cg_fragment_compiler.h"
#include "private.h"
#include "cg_ir_builder.h"
#include "cg_routines.h"
#include "llvm_folding_set.h"

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

void CheckNaN(TCGBaseContext& builder, Value* lhsValue, Value* rhsValue)
{
    CodegenIf<TCGBaseContext>(
        builder,
        builder->CreateFCmpUNO(lhsValue, rhsValue),
        [&] (TCGBaseContext& builder) {
            builder->CreateCall(
                builder.Module->GetRoutine("ThrowQueryException"),
                {
                    builder->CreateGlobalStringPtr("Comparison with NaN")
                });
        });
}

struct TValueTypeLabels
{
    Constant* OnBoolean;
    Constant* OnInt;
    Constant* OnUint;
    Constant* OnDouble;
    Constant* OnString;
};

////////////////////////////////////////////////////////////////////////////////

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

TValueTypeLabels CodegenHasherBody(
    TCGBaseContext& builder,
    Value* labelsArray,
    Value* values,
    Value* startIndex,
    Value* finishIndex)
{
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


    auto* entryBB = builder->GetInsertBlock();
    auto* gotoHashBB = builder->CreateBBHere("gotoHash");
    builder->CreateBr(gotoHashBB);

    builder->SetInsertPoint(gotoHashBB);
    PHINode* indexPhi = builder->CreatePHI(builder->getInt64Ty(), 2, "phiIndex");
    indexPhi->addIncoming(startIndex, entryBB);
    PHINode* result1Phi = builder->CreatePHI(builder->getInt64Ty(), 2, "result1Phi");
    result1Phi->addIncoming(builder->getInt64(0), entryBB);

    BasicBlock* gotoNextBB = builder->CreateBBHere("gotoNext");
    builder->SetInsertPoint(gotoNextBB);
    PHINode* result2Phi = builder->CreatePHI(builder->getInt64Ty(), 2, "result2Phi");

    Value* combinedResult = codegenHashCombine(builder, result1Phi, result2Phi);
    result1Phi->addIncoming(combinedResult, gotoNextBB);

    Value* nextIndex = builder->CreateAdd(indexPhi, builder->getInt64(1));
    indexPhi->addIncoming(nextIndex, gotoNextBB);

    BasicBlock* returnBB = builder->CreateBBHere("return");
    builder->CreateCondBr(builder->CreateICmpSLT(nextIndex, finishIndex), gotoHashBB, returnBB);

    builder->SetInsertPoint(returnBB);
    builder->CreateRet(combinedResult);

    // indexPhi, cmpResultPhi, returnBB, gotoNextBB

    BasicBlock* hashScalarBB = nullptr;
    {
        hashScalarBB = builder->CreateBBHere("hashNull");
        builder->SetInsertPoint(hashScalarBB);

        auto value = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                values,
                indexPhi),
            EValueType::Int64);

        result2Phi->addIncoming(builder->getInt64(0), builder->GetInsertBlock());
        BasicBlock* hashDataBB = builder->CreateBBHere("hashData");
        builder->CreateCondBr(value.IsNull(builder), gotoNextBB, hashDataBB);

        builder->SetInsertPoint(hashDataBB);
        Value* hashResult = CodegenFingerprint64(
            builder,
            value.GetData(builder));

        result2Phi->addIncoming(hashResult, builder->GetInsertBlock());
        builder->CreateBr(gotoNextBB);
    };

    BasicBlock* cmpStringBB = nullptr;
    {
        cmpStringBB = builder->CreateBBHere("hashNull");
        builder->SetInsertPoint(cmpStringBB);

        auto value = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                values,
                indexPhi),
            EValueType::String);

        result2Phi->addIncoming(builder->getInt64(0), builder->GetInsertBlock());
        BasicBlock* hashDataBB = builder->CreateBBHere("hashData");
        builder->CreateCondBr(value.IsNull(builder), gotoNextBB, hashDataBB);

        builder->SetInsertPoint(hashDataBB);
        Value* hashResult = builder->CreateCall(
            builder.Module->GetRoutine("StringHash"),
            {
                value.GetData(builder),
                value.GetLength(builder)
            });

        result2Phi->addIncoming(hashResult, builder->GetInsertBlock());
        builder->CreateBr(gotoNextBB);
    };

    builder->SetInsertPoint(gotoHashBB);

    Value* offset = builder->CreateLoad(builder->CreateGEP(labelsArray, indexPhi));
    auto* indirectBranch = builder->CreateIndirectBr(offset);
    indirectBranch->addDestination(hashScalarBB);
    indirectBranch->addDestination(cmpStringBB);

    return TValueTypeLabels{
        llvm::BlockAddress::get(hashScalarBB),
        llvm::BlockAddress::get(hashScalarBB),
        llvm::BlockAddress::get(hashScalarBB),
        llvm::BlockAddress::get(hashScalarBB),
        llvm::BlockAddress::get(cmpStringBB)
    };

}

////////////////////////////////////////////////////////////////////////////////

TValueTypeLabels CodegenEqComparerBody(
    TCGBaseContext& builder,
    Value* labelsArray,
    Value* lhsValues,
    Value* rhsValues,
    Value* startIndex,
    Value* finishIndex)
{
    auto* entryBB = builder->GetInsertBlock();
    auto* gotoCmpBB = builder->CreateBBHere("gotoCmp");
    builder->CreateBr(gotoCmpBB);

    builder->SetInsertPoint(gotoCmpBB);
    PHINode* indexPhi = builder->CreatePHI(builder->getInt64Ty(), 2, "phiIndex");
    indexPhi->addIncoming(startIndex, entryBB);

    BasicBlock* gotoNextBB = builder->CreateBBHere("gotoNext");
    builder->SetInsertPoint(gotoNextBB);
    Value* nextIndex = builder->CreateAdd(indexPhi, builder->getInt64(1));
    indexPhi->addIncoming(nextIndex, gotoNextBB);

    BasicBlock* returnEqBB = builder->CreateBBHere("returnEq");
    builder->CreateCondBr(builder->CreateICmpSLT(nextIndex, finishIndex), gotoCmpBB, returnEqBB);

    builder->SetInsertPoint(returnEqBB);
    builder->CreateRet(builder->getInt8(true));

    BasicBlock* returnNotEqBB = builder->CreateBBHere("returnNotEq");
    builder->SetInsertPoint(returnNotEqBB);
    builder->CreateRet(builder->getInt8(false));

    // indexPhi, resultPhi, returnBB, gotoNextBB

    auto cmpScalar = [&] (auto cmpEqual, EValueType type, const char* name) {
        BasicBlock* cmpBB = builder->CreateBBHere(Twine("cmp.").concat(name));
        builder->SetInsertPoint(cmpBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);
        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        Value* nullEqual = builder->CreateICmpEQ(lhsIsNull, rhsIsNull);
        Value* isEqual = builder->CreateAnd(
            nullEqual,
            builder->CreateOr(
                lhsIsNull,
                cmpEqual(builder, lhsData, rhsData)));

        builder->CreateCondBr(isEqual, gotoNextBB, returnNotEqBB);

        return cmpBB;
    };

    auto cmpScalarWithCheck = [&] (auto cmpEqual, EValueType type, const char* name) {
        BasicBlock* cmpBB = builder->CreateBBHere(Twine("cmp.").concat(name));
        builder->SetInsertPoint(cmpBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);

        BasicBlock* cmpNullBB = builder->CreateBBHere(Twine("cmpNull.").concat(name));
        BasicBlock* cmpDataBB = builder->CreateBBHere(Twine("cmpData.").concat(name));
        builder->CreateCondBr(builder->CreateOr(lhsIsNull, rhsIsNull), cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        builder->CreateCondBr(builder->CreateAnd(lhsIsNull, rhsIsNull), gotoNextBB, returnNotEqBB);

        builder->SetInsertPoint(cmpDataBB);
        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        if (type == EValueType::Double) {
            CheckNaN(builder, lhsData, rhsData);
        }

        builder->CreateCondBr(cmpEqual(builder, lhsData, rhsData), gotoNextBB, returnNotEqBB);

        return cmpBB;
    };

    BasicBlock* cmpIntBB = cmpScalar(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpEQ(lhsData, rhsData);
        },
        EValueType::Int64,
        "int64");

    BasicBlock* cmpUintBB = cmpScalar(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpEQ(lhsData, rhsData);
        },
        EValueType::Uint64,
        "uint64");

    BasicBlock* cmpDoubleBB = cmpScalarWithCheck(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateFCmpUEQ(lhsData, rhsData);
        },
        EValueType::Double,
        "double");

    BasicBlock* cmpStringBB = nullptr;
    {
        cmpStringBB = builder->CreateBBHere("cmp.string");
        builder->SetInsertPoint(cmpStringBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            EValueType::String);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            EValueType::String);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);

        Value* anyNull = builder->CreateOr(lhsIsNull, rhsIsNull);

        BasicBlock* cmpNullBB = builder->CreateBBHere("cmpNull.string");
        BasicBlock* cmpDataBB = builder->CreateBBHere("cmpData.string");
        builder->CreateCondBr(anyNull, cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        builder->CreateCondBr(builder->CreateICmpEQ(lhsIsNull, rhsIsNull), gotoNextBB, returnNotEqBB);

        builder->SetInsertPoint(cmpDataBB);

        Value* lhsLength = lhsValue.GetLength(builder);
        Value* rhsLength = rhsValue.GetLength(builder);

        Value* cmpLength = builder->CreateICmpULT(lhsLength, rhsLength);

        Value* minLength = builder->CreateSelect(
            cmpLength,
            lhsLength,
            rhsLength);

        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        Value* cmpResult = builder->CreateCall(
            builder.Module->GetRoutine("memcmp"),
            {
                lhsData,
                rhsData,
                builder->CreateZExt(minLength, builder->getSizeType())
            });

        builder->CreateCondBr(
            builder->CreateAnd(
                builder->CreateICmpEQ(cmpResult, builder->getInt32(0)),
                builder->CreateICmpEQ(lhsLength, rhsLength)),
            gotoNextBB,
            returnNotEqBB);
    }

    builder->SetInsertPoint(gotoCmpBB);

    Value* offset = builder->CreateLoad(builder->CreateGEP(labelsArray, indexPhi));
    auto* indirectBranch = builder->CreateIndirectBr(offset);
    indirectBranch->addDestination(cmpIntBB);
    indirectBranch->addDestination(cmpUintBB);
    indirectBranch->addDestination(cmpDoubleBB);
    indirectBranch->addDestination(cmpStringBB);

    return TValueTypeLabels{
        llvm::BlockAddress::get(cmpIntBB),
        llvm::BlockAddress::get(cmpIntBB),
        llvm::BlockAddress::get(cmpUintBB),
        llvm::BlockAddress::get(cmpDoubleBB),
        llvm::BlockAddress::get(cmpStringBB)
    };
};

void DefaultOnEqual(TCGBaseContext& builder)
{
    builder->CreateRet(builder->getInt8(false));
}

void DefaultOnNotEqual(TCGBaseContext& builder, Value* result, Value* index)
{
    builder->CreateRet(builder->CreateZExt(result, builder->getInt8Ty()));
}

TValueTypeLabels CodegenLessComparerBody(
    TCGBaseContext& builder,
    Value* labelsArray,
    Value* lhsValues,
    Value* rhsValues,
    Value* startIndex,
    Value* finishIndex,
    std::function<void(TCGBaseContext& builder)> onEqual,
    std::function<void(TCGBaseContext& builder, Value* result, Value* index)> onNotEqual)
{
    auto* entryBB = builder->GetInsertBlock();
    auto* gotoCmpBB = builder->CreateBBHere("gotoCmp");
    builder->CreateBr(gotoCmpBB);

    builder->SetInsertPoint(gotoCmpBB);
    PHINode* indexPhi = builder->CreatePHI(builder->getInt64Ty(), 2, "phiIndex");
    indexPhi->addIncoming(startIndex, entryBB);

    BasicBlock* gotoNextBB = builder->CreateBBHere("gotoNext");
    builder->SetInsertPoint(gotoNextBB);
    Value* nextIndex = builder->CreateAdd(indexPhi, builder->getInt64(1));
    indexPhi->addIncoming(nextIndex, gotoNextBB);

    BasicBlock* returnEqualBB = builder->CreateBBHere("returnEqual");
    builder->CreateCondBr(builder->CreateICmpSLT(nextIndex, finishIndex), gotoCmpBB, returnEqualBB);
    builder->SetInsertPoint(returnEqualBB);
    onEqual(builder);

    BasicBlock* returnBB = builder->CreateBBHere("return");
    builder->SetInsertPoint(returnBB);
    PHINode* resultPhi = builder->CreatePHI(builder->getInt1Ty(), 2, "resultPhi");

    onNotEqual(builder, resultPhi, indexPhi);

    // indexPhi, resultPhi, returnBB, gotoNextBB

    auto cmpScalar = [&] (auto cmpEqual, auto cmpLess, EValueType type, const char* name) {
        BasicBlock* cmpBB = builder->CreateBBHere(Twine("cmp.").concat(name));
        builder->SetInsertPoint(cmpBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);
        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        Value* nullEqual = builder->CreateICmpEQ(lhsIsNull, rhsIsNull);
        Value* isEqual = builder->CreateAnd(
            nullEqual,
            builder->CreateOr(
                lhsIsNull,
                cmpEqual(builder, lhsData, rhsData)));

        BasicBlock* cmpFurtherBB = builder->CreateBBHere(Twine("cmpFurther.").concat(name));
        builder->CreateCondBr(isEqual, gotoNextBB, cmpFurtherBB);

        builder->SetInsertPoint(cmpFurtherBB);

        Value* cmpResult = builder->CreateSelect(
            nullEqual,
            cmpLess(builder, lhsData, rhsData),
            builder->CreateICmpUGT(lhsIsNull, rhsIsNull));

        resultPhi->addIncoming(cmpResult, builder->GetInsertBlock());
        builder->CreateBr(returnBB);

        return cmpBB;
    };

    auto cmpScalarWithCheck = [&] (auto cmpEqual, auto cmpLess, EValueType type, const char* name) {
        BasicBlock* cmpBB = builder->CreateBBHere(Twine("cmp.").concat(name));
        builder->SetInsertPoint(cmpBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);

        BasicBlock* cmpNullBB = builder->CreateBBHere(Twine("cmpNull.").concat(name));
        BasicBlock* cmpDataBB = builder->CreateBBHere(Twine("cmpData.").concat(name));
        builder->CreateCondBr(builder->CreateOr(lhsIsNull, rhsIsNull), cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        resultPhi->addIncoming(builder->CreateICmpUGT(lhsIsNull, rhsIsNull), builder->GetInsertBlock());
        builder->CreateCondBr(builder->CreateAnd(lhsIsNull, rhsIsNull), gotoNextBB, returnBB);

        builder->SetInsertPoint(cmpDataBB);
        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        if (type == EValueType::Double) {
            CheckNaN(builder, lhsData, rhsData);
        }

        resultPhi->addIncoming(cmpLess(builder, lhsData, rhsData), builder->GetInsertBlock());
        builder->CreateCondBr(cmpEqual(builder, lhsData, rhsData), gotoNextBB, returnBB);

        return cmpBB;
    };

    BasicBlock* cmpIntBB = cmpScalar(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpEQ(lhsData, rhsData);
        },
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpSLT(lhsData, rhsData);
        },
        EValueType::Int64,
        "int64");

    BasicBlock* cmpUintBB = cmpScalar(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpEQ(lhsData, rhsData);
        },
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpULT(lhsData, rhsData);
        },
        EValueType::Uint64,
        "uint64");

    BasicBlock* cmpDoubleBB = cmpScalarWithCheck(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateFCmpUEQ(lhsData, rhsData);
        },
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateFCmpULT(lhsData, rhsData);
        },
        EValueType::Double,
        "double");

    BasicBlock* cmpStringBB = nullptr;
    {
        cmpStringBB = builder->CreateBBHere("cmp.string");
        builder->SetInsertPoint(cmpStringBB);

        auto lhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                lhsValues,
                indexPhi),
            EValueType::String);

        auto rhsValue = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                rhsValues,
                indexPhi),
            EValueType::String);

        Value* lhsIsNull = lhsValue.IsNull(builder);
        Value* rhsIsNull = rhsValue.IsNull(builder);

        Value* anyNull = builder->CreateOr(lhsIsNull, rhsIsNull);

        BasicBlock* cmpNullBB = builder->CreateBBHere("cmpNull.string");
        BasicBlock* cmpDataBB = builder->CreateBBHere("cmpData.string");
        builder->CreateCondBr(anyNull, cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        resultPhi->addIncoming(
            builder->CreateICmpUGT(lhsIsNull, rhsIsNull),
            builder->GetInsertBlock());
        builder->CreateBr(returnBB);

        builder->SetInsertPoint(cmpDataBB);

        Value* lhsLength = lhsValue.GetLength(builder);
        Value* rhsLength = rhsValue.GetLength(builder);

        Value* cmpLength = builder->CreateICmpULT(lhsLength, rhsLength);

        Value* minLength = builder->CreateSelect(
            cmpLength,
            lhsLength,
            rhsLength);

        Value* lhsData = lhsValue.GetData(builder);
        Value* rhsData = rhsValue.GetData(builder);

        Value* cmpResult = builder->CreateCall(
            builder.Module->GetRoutine("memcmp"),
            {
                lhsData,
                rhsData,
                builder->CreateZExt(minLength, builder->getSizeType())
            });

        BasicBlock* cmpLengthBB = builder->CreateBBHere("cmpLength.string");
        BasicBlock* returnContentCmpBB = builder->CreateBBHere("returnContentCmp.string");
        builder->CreateCondBr(
            builder->CreateICmpEQ(cmpResult, builder->getInt32(0)),
            cmpLengthBB,
            returnContentCmpBB);

        builder->SetInsertPoint(returnContentCmpBB);
        resultPhi->addIncoming(
            builder->CreateICmpSLT(cmpResult, builder->getInt32(0)),
            builder->GetInsertBlock());
        builder->CreateBr(returnBB);

        builder->SetInsertPoint(cmpLengthBB);

        resultPhi->addIncoming(cmpLength, builder->GetInsertBlock());
        builder->CreateCondBr(
            builder->CreateICmpEQ(lhsLength, rhsLength),
            gotoNextBB,
            returnBB);
    }

    builder->SetInsertPoint(gotoCmpBB);

    Value* offset = builder->CreateLoad(builder->CreateGEP(labelsArray, indexPhi));
    auto* indirectBranch = builder->CreateIndirectBr(offset);
    indirectBranch->addDestination(cmpIntBB);
    indirectBranch->addDestination(cmpUintBB);
    indirectBranch->addDestination(cmpDoubleBB);
    indirectBranch->addDestination(cmpStringBB);

    return TValueTypeLabels{
        llvm::BlockAddress::get(cmpIntBB),
        llvm::BlockAddress::get(cmpIntBB),
        llvm::BlockAddress::get(cmpUintBB),
        llvm::BlockAddress::get(cmpDoubleBB),
        llvm::BlockAddress::get(cmpStringBB)
    };
};

struct TComparerManager
    : public TRefCounted
{
    Function* Hasher;
    TValueTypeLabels HashLables;

    Function* EqComparer;
    TValueTypeLabels EqLables;

    Function* LessComparer;
    TValueTypeLabels LessLables;

    Function* TernaryComparer;
    TValueTypeLabels TernaryLables;

    THashMap<llvm::FoldingSetNodeID, Function*> Hashers;
    THashMap<llvm::FoldingSetNodeID, Function*> EqComparers;
    THashMap<llvm::FoldingSetNodeID, Function*> LessComparers;
    THashMap<llvm::FoldingSetNodeID, Function*> TernaryComparers;

    Function* GetHasher(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t start,
        size_t finish);

    Function* GetHasher(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module);

    Function* GetEqComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t start,
        size_t finish);

    Function* GetEqComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module);

    Function* GetLessComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t start,
        size_t finish);

    Function* GetLessComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module);

    Function* GetTernaryComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t start,
        size_t finish);

    Function* GetTernaryComparer(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module);
};

DEFINE_REFCOUNTED_TYPE(TComparerManager);

TComparerManagerPtr MakeComparerManager()
{
    return New<TComparerManager>();
}

llvm::GlobalVariable* GetLabelsArray(
    TCGBaseContext& builder,
    const std::vector<EValueType>& types,
    const TValueTypeLabels& valueTypeLabels)
{
    std::vector<Constant*> labels;
    for (auto type : types) {
        switch (type) {
            case EValueType::Boolean:
                labels.push_back(valueTypeLabels.OnBoolean);
                break;

            case EValueType::Int64:
                labels.push_back(valueTypeLabels.OnInt);
                break;

            case EValueType::Uint64:
                labels.push_back(valueTypeLabels.OnUint);
                break;

            case EValueType::Double:
                labels.push_back(valueTypeLabels.OnDouble);
                break;

            case EValueType::String: {
                labels.push_back(valueTypeLabels.OnString);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    llvm::ArrayType* labelArrayType = llvm::ArrayType::get(
        TypeBuilder<char*, false>::get(builder->getContext()),
        types.size());

    return new llvm::GlobalVariable(
        *builder.Module->GetModule(),
        labelArrayType,
        true,
        llvm::GlobalVariable::ExternalLinkage,
        llvm::ConstantArray::get(
            labelArrayType,
            labels));
}

////////////////////////////////////////////////////////////////////////////////

Function* TComparerManager::GetHasher(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t start,
        size_t finish)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = Hashers.emplace(id, nullptr);
    if (emplaced.second) {
        if (!Hasher) {
            Hasher = MakeFunction<ui64(char**, TRow, size_t, size_t)>(module, "HasherImpl", [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* row,
                Value* startIndex,
                Value* finishIndex
            ) {
                Value* values = CodegenValuesPtrFromRow(builder, row);

                HashLables = CodegenHasherBody(
                    builder,
                    labelsArray,
                    values,
                    startIndex,
                    finishIndex);
            });
        }

        emplaced.first->second = MakeFunction<THasherFunction>(module, "Hasher", [&] (
            TCGBaseContext& builder,
            Value* row
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt64(0);
            } else {
                result = builder->CreateCall(
                    Hasher,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, HashLables),
                            TypeBuilder<char**, false>::get(builder->getContext())),
                        row,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
            }

            builder->CreateRet(result);
        });
    }

    return emplaced.first->second;
}

Function* TComparerManager::GetHasher(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module)
{
    return GetHasher(types, module, 0, types.size());
}

////////////////////////////////////////////////////////////////////////////////

Function* TComparerManager::GetEqComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = EqComparers.emplace(id, nullptr);
    if (emplaced.second) {
        if (!EqComparer) {
            EqComparer = MakeFunction<char(char**, TRow, TRow, size_t, size_t)>(module, "EqComparerImpl", [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* lhsRow,
                Value* rhsRow,
                Value* startIndex,
                Value* finishIndex
            ) {
                Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
                Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

                EqLables = CodegenEqComparerBody(
                    builder,
                    labelsArray,
                    lhsValues,
                    rhsValues,
                    startIndex,
                    finishIndex);
            });
        }

        emplaced.first->second = MakeFunction<char(TRow, TRow)>(module, "EqComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt8(1);
            } else {
                result = builder->CreateCall(
                    EqComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, EqLables),
                            TypeBuilder<char**, false>::get(builder->getContext())),
                        lhsRow,
                        rhsRow,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
            }

            builder->CreateRet(result);
        });
    }

    return emplaced.first->second;
}

Function* TComparerManager::GetEqComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module)
{
    return GetEqComparer(types, module, 0, types.size());
}

////////////////////////////////////////////////////////////////////////////////

Function* TComparerManager::GetLessComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = LessComparers.emplace(id, nullptr);
    if (emplaced.second) {
        if (!LessComparer) {
            LessComparer = MakeFunction<char(char**, TRow, TRow, size_t, size_t)>(module, "LessComparerImpl", [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* lhsRow,
                Value* rhsRow,
                Value* startIndex,
                Value* finishIndex
            ) {
                Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
                Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

                LessLables = CodegenLessComparerBody(
                    builder,
                    labelsArray,
                    lhsValues,
                    rhsValues,
                    startIndex,
                    finishIndex,
                    DefaultOnEqual,
                    DefaultOnNotEqual);
            });
        }

        emplaced.first->second = MakeFunction<char(TRow, TRow)>(module, "LessComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt8(0);
            } else {
                result = builder->CreateCall(
                    LessComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, LessLables),
                            TypeBuilder<char**, false>::get(builder->getContext())),
                        lhsRow,
                        rhsRow,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
            }

            builder->CreateRet(result);
        });
    }

    return emplaced.first->second;
}

Function* TComparerManager::GetLessComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module)
{
    return GetLessComparer(types, module, 0, types.size());
}

////////////////////////////////////////////////////////////////////////////////

Function* TComparerManager::GetTernaryComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t start,
    size_t finish)
{
    YCHECK(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = TernaryComparers.emplace(id, nullptr);
    if (emplaced.second) {
        if (!TernaryComparer) {
            TernaryComparer = MakeFunction<int(char**, TRow, TRow, size_t, size_t)>(module, "TernaryComparerImpl",
             [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* lhsRow,
                Value* rhsRow,
                Value* startIndex,
                Value* finishIndex
            ) {
                Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
                Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

                TernaryLables = CodegenLessComparerBody(
                    builder,
                    labelsArray,
                    lhsValues,
                    rhsValues,
                    startIndex,
                    finishIndex,
                    [&] (TCGBaseContext& builder) {
                        builder->CreateRet(builder->getInt32(0));
                    },
                    [&] (TCGBaseContext& builder, Value* result, Value* index) {
                        builder->CreateRet(
                            builder->CreateSelect(result, builder->getInt32(-1), builder->getInt32(1)));
                    });
            });
        }

        emplaced.first->second = MakeFunction<int(TRow, TRow)>(module, "TernaryComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt32(0);
            } else {
                result = builder->CreateCall(
                    TernaryComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, TernaryLables),
                            TypeBuilder<char**, false>::get(builder->getContext())),
                        lhsRow,
                        rhsRow,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
            }

            builder->CreateRet(result);
        });
    }

    return emplaced.first->second;
}

Function* TComparerManager::GetTernaryComparer(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module)
{
    return GetTernaryComparer(types, module, 0, types.size());
}

////////////////////////////////////////////////////////////////////////////////

Function* CodegenOrderByComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t offset,
    const std::vector<bool>& isDesc)
{
    TValueTypeLabels lables;

    Function* comparer = MakeFunction<char(char**, char*, TRow, TRow)>(module, "OrderByComparerImpl", [&] (
        TCGBaseContext& builder,
        Value* labelsArray,
        Value* isDesc,
        Value* lhsRow,
        Value* rhsRow
    ) {
        Value* lhsValues = CodegenValuesPtrFromRow(builder, lhsRow);
        Value* rhsValues = CodegenValuesPtrFromRow(builder, rhsRow);

        lhsValues = builder->CreateGEP(lhsValues, builder->getInt64(offset));
        rhsValues = builder->CreateGEP(rhsValues, builder->getInt64(offset));

        lables = CodegenLessComparerBody(
            builder,
            labelsArray,
            lhsValues,
            rhsValues,
            builder->getInt64(0),
            builder->getInt64(types.size()),
            DefaultOnEqual,
            [&] (TCGBaseContext& builder, Value* result, Value* index) {
                Value* notEqualResult = builder->CreateZExt(result, builder->getInt8Ty());
                if (isDesc) {
                    notEqualResult = builder->CreateXor(
                        notEqualResult,
                        builder->CreateLoad(builder->CreateGEP(isDesc, index)));
                }

                builder->CreateRet(notEqualResult);
            });
    });

    return MakeFunction<char(TRow, TRow)>(module, "OrderByComparer", [&] (
        TCGBaseContext& builder,
        Value* lhsRow,
        Value* rhsRow
    ) {
        std::vector<Constant*> isDescFlags;
        for (size_t index = 0; index < types.size(); ++index) {
            isDescFlags.push_back(builder->getInt8(index < isDesc.size() && isDesc[index]));
        }

        llvm::ArrayType* isDescArrayType = llvm::ArrayType::get(
            TypeBuilder<char, false>::get(builder->getContext()),
            types.size());

        llvm::GlobalVariable* isDescArray =
            new llvm::GlobalVariable(
                *builder.Module->GetModule(),
                isDescArrayType,
                true,
                llvm::GlobalVariable::ExternalLinkage,
                llvm::ConstantArray::get(
                    isDescArrayType,
                    isDescFlags));

        builder->CreateRet(
            builder->CreateCall(
                comparer,
                {
                    builder->CreatePointerCast(
                        GetLabelsArray(builder, types, lables),
                        TypeBuilder<char**, false>::get(builder->getContext())),
                    builder->CreatePointerCast(
                        isDescArray,
                        TypeBuilder<char*, false>::get(builder->getContext())),
                    lhsRow,
                    rhsRow
                }));
    });
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

            Value* expressionClosure = ConvertToPointer(function->arg_begin());
            {
                TCGIRBuilder irBuilder(function);
                auto innerBuilder = TCGExprContext::Make(
                    TCGBaseContext(TCGIRBuilderPtr(&irBuilder), module),
                    fragmentInfos,
                    expressionClosure);

                Value* fragmentFlag = innerBuilder.GetFragmentFlag(id);

                auto* evaluationNeeded = innerBuilder->CreateICmpEQ(
                    innerBuilder->CreateLoad(fragmentFlag),
                    innerBuilder->getInt8(false));

                CodegenIf<TCGExprContext>(
                    innerBuilder,
                    evaluationNeeded,
                    [&] (TCGExprContext& builder) {
                        builder.ExpressionFragments.Items[id].Generator(builder)
                            .StoreToValue(builder, builder.GetFragmentResult(id));

                        builder->CreateStore(builder->getInt8(true), fragmentFlag);
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

        auto evaluate = [&] (TCGIRBuilderPtr& builder) {
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
        };

        if (builder.ExpressionFragments.Items[operandId].Nullable) {
            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                builder,
                operandValue.IsNull(builder),
                [&] (TCGIRBuilderPtr& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                evaluate,
                Twine(name.c_str()));
        } else {
            return evaluate(builder);
        }
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

            const auto& items = builder.ExpressionFragments.Items;

            Value* lhsIsNull = items[lhsId].Nullable ? lhsValue.IsNull(builder) : nullptr;
            Value* rhsIsNull = items[rhsId].Nullable ? rhsValue.IsNull(builder) : nullptr;

            auto next = [&] (TCGExprContext& builder) {
                Value* lhsData = lhsValue.GetData(builder);
                return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                    builder,
                    builder->CreateICmpEQ(lhsData, builder->getInt8(parameter)),
                    [&] (TCGIRBuilderPtr& builder) {
                        return lhsValue;
                    },
                    [&] (TCGIRBuilderPtr& builder) {
                        if (items[rhsId].Nullable) {
                            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                                builder,
                                rhsIsNull,
                                [&] (TCGIRBuilderPtr& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                },
                                [&] (TCGIRBuilderPtr& builder) {
                                    return rhsValue;
                                });
                        } else {
                            return rhsValue;
                        }
                    });
            };

            if (items[lhsId].Nullable) {
                return CodegenIf<TCGExprContext, TCGValue>(
                    builder,
                    lhsIsNull,
                    [&] (TCGExprContext& builder) {
                        auto right = [&] (TCGIRBuilderPtr& builder) {
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
                        };

                        if (items[rhsId].Nullable) {
                            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                                builder,
                                rhsIsNull,
                                [&] (TCGIRBuilderPtr& builder) {
                                    return TCGValue::CreateNull(builder, type);
                                },
                                right);
                        } else {
                            return right(builder);
                        }
                    },
                    next);
            } else {
                return next(builder);
            }
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
                evalData = builder->Create##optype(lhsData, rhsData); \
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

            return evalData;
        };

        auto cmpResultToResult = [] (TCGBaseContext& builder, Value* cmpResult, EBinaryOp opcode) {
            Value* evalData;
            switch (opcode) {
                case EBinaryOp::Equal:
                    evalData = builder->CreateIsNull(cmpResult);
                    break;
                case EBinaryOp::NotEqual:
                    evalData = builder->CreateIsNotNull(cmpResult);
                    break;
                case EBinaryOp::Less:
                    evalData = builder->CreateICmpSLT(cmpResult, builder->getInt32(0));
                    break;
                case EBinaryOp::Greater:
                    evalData = builder->CreateICmpSGT(cmpResult, builder->getInt32(0));
                    break;
                case EBinaryOp::LessOrEqual:
                    evalData =  builder->CreateICmpSLE(cmpResult, builder->getInt32(0));
                    break;
                case EBinaryOp::GreaterOrEqual:
                    evalData = builder->CreateICmpSGE(cmpResult, builder->getInt32(0));
                    break;
                default:
                    Y_UNREACHABLE();
            }
            return evalData;
        };

        auto compare = [&] (TCGBaseContext& builder) {
                if (lhsValue.GetStaticType() != rhsValue.GetStaticType()) {
                    auto resultOpcode = opcode;

                    if (rhsValue.GetStaticType() == EValueType::Any) {
                        resultOpcode = GetReversedBinaryOpcode(resultOpcode);
                        std::swap(lhsValue, rhsValue);
                    }

                    if (lhsValue.GetStaticType() == EValueType::Any) {
                        Value* lhsData = lhsValue.GetData(builder);
                        Value* lhsLength = lhsValue.GetLength(builder);

                        Value* cmpResult = nullptr;

                        switch (rhsValue.GetStaticType()) {
                            case EValueType::Boolean: {
                                cmpResult = builder->CreateCall(
                                    builder.Module->GetRoutine("CompareAnyBoolean"),
                                    {
                                        lhsData,
                                        lhsLength,
                                        rhsValue.GetData(builder)
                                    });
                                break;
                            }
                            case EValueType::Int64: {
                                cmpResult = builder->CreateCall(
                                    builder.Module->GetRoutine("CompareAnyInt64"),
                                    {
                                        lhsData,
                                        lhsLength,
                                        rhsValue.GetData(builder)
                                    });
                                break;
                            }
                            case EValueType::Uint64: {
                                cmpResult = builder->CreateCall(
                                    builder.Module->GetRoutine("CompareAnyUint64"),
                                    {
                                        lhsData,
                                        lhsLength,
                                        rhsValue.GetData(builder)
                                    });
                                break;
                            }
                            case EValueType::Double: {
                                cmpResult = builder->CreateCall(
                                    builder.Module->GetRoutine("CompareAnyDouble"),
                                    {
                                        lhsData,
                                        lhsLength,
                                        rhsValue.GetData(builder)
                                    });
                                break;
                            }
                            case EValueType::String: {
                                cmpResult = builder->CreateCall(
                                    builder.Module->GetRoutine("CompareAnyString"),
                                    {
                                        lhsData,
                                        lhsLength,
                                        rhsValue.GetData(builder),
                                        rhsValue.GetLength(builder)
                                    });
                                break;
                            }
                            default:
                                Y_UNREACHABLE();
                        }

                        return cmpResultToResult(builder, cmpResult, resultOpcode);
                    } else {
                        Y_UNREACHABLE();
                    }
                }

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

                        break;
                    }

                    case EValueType::Any: {
                        Value* lhsLength = lhsValue.GetLength(builder);
                        Value* rhsLength = rhsValue.GetLength(builder);

                        Value* cmpResult = builder->CreateCall(
                            builder.Module->GetRoutine("CompareAny"),
                            {
                                lhsData,
                                lhsLength,
                                rhsData,
                                rhsLength
                            });

                        evalData = cmpResultToResult(builder, cmpResult, opcode);
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                }

                return evalData;
            };

            #undef CMP_OP

        auto result =
            builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? CodegenIf<TCGBaseContext, Value*>(
                builder,
                builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
                [&] (TCGBaseContext& builder) {
                    return compareNulls();
                },
                compare,
                nameTwine)
            : compare(builder);

        return TCGValue::CreateFromValue(
            builder,
            builder->getFalse(),
            nullptr,
            builder->CreateZExtOrBitCast(
                result,
                TDataTypeBuilder::TBoolean::get(builder->getContext())),
            type);
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

        Value* anyNull =
            builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? builder->CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder))
            : builder->getFalse();

        if ((opcode == EBinaryOp::Divide || opcode == EBinaryOp::Modulo) &&
            (operandType == EValueType::Uint64 || operandType == EValueType::Int64))
        {
            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                anyNull,
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
                anyNull,
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

TCodegenExpression MakeCodegenInExpr(
    std::vector<size_t> argIds,
    int arrayIndex,
    TComparerManagerPtr comparerManager)
{
    return [
        MOVE(argIds),
        MOVE(arrayIndex),
        MOVE(comparerManager)
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
                comparerManager->GetLessComparer(keyTypes, builder.Module),
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

TCodegenExpression MakeCodegenTransformExpr(
    std::vector<size_t> argIds,
    TNullable<size_t> defaultExprId,
    int arrayIndex,
    EValueType resultType,
    TComparerManagerPtr comparerManager)
{
    return [
        MOVE(argIds),
        MOVE(defaultExprId),
        MOVE(arrayIndex),
        resultType,
        MOVE(comparerManager)
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
            builder.Module->GetRoutine("TransformTuple"),
            {
                comparerManager->GetLessComparer(keyTypes, builder.Module),
                newRow,
                builder.GetOpaqueValue(arrayIndex)
            });

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            builder->CreateIsNotNull(result),
            [&] (TCGExprContext& builder) {
                return TCGValue::CreateFromValue(
                    builder,
                    builder->CreateLoad(result),
                    resultType);
            },
            [&] (TCGExprContext& builder) {
                if (defaultExprId) {
                    return CodegenFragment(builder, *defaultExprId);
                } else {
                    return TCGValue::CreateNull(builder, resultType);
                }
            });
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
    size_t commonKeyPrefix,
    size_t foreignKeyPrefix,
    TComparerManagerPtr comparerManager)
{
    size_t consumerSlot = (*slotCount)++;
    *codegenSource = [
        consumerSlot,
        producerSlot,
        index,
        MOVE(fragmentInfos),
        MOVE(equations),
        commonKeyPrefix,
        foreignKeyPrefix,
        MOVE(comparerManager),
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
                    keyValues,
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

                comparerManager->GetHasher(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),

                comparerManager->GetEqComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                comparerManager->GetLessComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, commonKeyPrefix),

                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, foreignKeyPrefix),
                comparerManager->GetLessComparer(lookupKeyTypes, module, foreignKeyPrefix, lookupKeyTypes.size()),

                comparerManager->GetTernaryComparer(lookupKeyTypes, module),

                comparerManager->GetHasher(lookupKeyTypes, module),
                comparerManager->GetEqComparer(lookupKeyTypes, module),

                builder->getInt32(lookupKeySize),

                collectRows.ClosurePtr,
                collectRows.Function,

                consumeJoinedRows.ClosurePtr,
                consumeJoinedRows.Function
            });
    };

    return consumerSlot;
}

size_t MakeCodegenMultiJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    int index,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<TSingleJoinCGParameters> parameters,
    std::vector<std::pair<size_t, EValueType>> primaryColumns,
    TComparerManagerPtr comparerManager)
{
    size_t consumerSlot = (*slotCount)++;
    *codegenSource = [
        consumerSlot,
        producerSlot,
        index,
        MOVE(fragmentInfos),
        MOVE(parameters),
        MOVE(comparerManager),
        MOVE(primaryColumns),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto collectRows = MakeClosure<void(TJoinClosure*, TRowBuffer*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* joinClosure,
            Value* buffer
        ) {
            Value* keyPtrs = builder->CreateAlloca(
                TypeBuilder<TRow, false>::get(builder->getContext()),
                builder->getInt32(parameters.size()));

            Value* primaryValuesPtr = builder->CreateAlloca(TypeBuilder<TValue*, false>::get(builder->getContext()));

            builder->CreateStore(
                builder->CreateCall(
                    builder.Module->GetRoutine("AllocateJoinKeys"),
                    {
                        builder.GetExecutionContext(),
                        joinClosure,
                        keyPtrs
                    }),
                primaryValuesPtr);

            builder[producerSlot] = [&] (TCGContext& builder, Value* row) {
                Value* joinClosureRef = builder->ViaClosure(joinClosure);
                Value* keyPtrsRef = builder->ViaClosure(keyPtrs);

                auto rowBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    row,
                    builder.Buffer);

                for (size_t index = 0; index < parameters.size(); ++index) {
                    Value* keyValues = CodegenValuesPtrFromRow(
                        builder,
                        builder->CreateLoad(builder->CreateConstGEP1_32(keyPtrsRef, index)));

                    const auto& equations = parameters[index].Equations;
                    for (size_t column = 0; column < equations.size(); ++column) {
                        if (!equations[column].second) {
                            auto joinKeyValue = CodegenFragment(rowBuilder, equations[column].first);
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
                        keyValues,
                        rowBuilder.ExpressionClosurePtr});

                    for (size_t column = 0; column < equations.size(); ++column) {
                        if (equations[column].second) {
                            auto evaluatedColumn = CodegenFragment(
                                evaluatedColumnsBuilder,
                                equations[column].first);
                            evaluatedColumn.StoreToValues(evaluatedColumnsBuilder, keyValues, column);
                        }
                    }
                }

                Value* primaryValuesPtrRef = builder->ViaClosure(primaryValuesPtr);
                Value* primaryValues = builder->CreateLoad(primaryValuesPtrRef);
                Value* rowValues = CodegenValuesPtrFromRow(builder, row);
                for (size_t column = 0; column < primaryColumns.size(); ++column) {
                    TCGValue::CreateFromRowValues(
                        builder,
                        rowValues,
                        primaryColumns[column].first,
                        primaryColumns[column].second)
                        .StoreToValues(builder, primaryValues, column);
                }

                builder->CreateCall(
                    builder.Module->GetRoutine("StorePrimaryRow"),
                    {
                        builder.GetExecutionContext(),
                        joinClosureRef,
                        primaryValuesPtrRef,
                        keyPtrsRef
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

        Value* joinComparers = builder->CreateAlloca(
            TypeBuilder<TJoinComparers, false>::get(builder->getContext()),
            builder->getInt32(parameters.size()));

        typedef TypeBuilder<TJoinComparers, false>::Fields TFields;

        for (size_t index = 0; index < parameters.size(); ++index) {
            const auto& lookupKeyTypes = parameters[index].LookupKeyTypes;
            const auto& commonKeyPrefix = parameters[index].CommonKeyPrefix;
            const auto& foreignKeyPrefix = parameters[index].ForeignKeyPrefix;

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, commonKeyPrefix),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::PrefixEqComparer));

            builder->CreateStore(
                comparerManager->GetHasher(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::SuffixHasher));

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::SuffixEqComparer));

            builder->CreateStore(
                comparerManager->GetLessComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::SuffixLessComparer));

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, foreignKeyPrefix),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::ForeignPrefixEqComparer));

            builder->CreateStore(
                comparerManager->GetLessComparer(lookupKeyTypes, module, foreignKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::ForeignSuffixLessComparer));

            builder->CreateStore(
                comparerManager->GetTernaryComparer(lookupKeyTypes, module),
                builder->CreateConstGEP2_32(nullptr, joinComparers, index, TFields::FullTernaryComparer));
        }

        builder->CreateCall(
            module->GetRoutine("MultiJoinOpHelper"),
            {
                builder.GetExecutionContext(),
                builder.GetOpaqueValue(index),
                joinComparers,

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
            Value* values = CodegenValuesPtrFromRow(builder, row);

            builder->CreateMemCpy(
                builder->CreatePointerCast(newValues, builder->getInt8PtrTy()),
                builder->CreatePointerCast(values, builder->getInt8PtrTy()),
                sourceSchema.size() * sizeof(TValue), 8);

            TCGValue::CreateFromValue(
                builder,
                builder->getFalse(),
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
            codegenAggregates[index].Initialize(builder, buffer)
                .StoreToValues(builder, values, keySize + index);
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

            TCodegenAggregateUpdate updateFunction;
            if (isMerge) {
                updateFunction = codegenAggregates[index].Merge;
            } else {
                updateFunction = codegenAggregates[index].Update;
            }
            updateFunction(builder, buffer, aggState, newValue);
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
    bool checkNulls,
    TComparerManagerPtr comparerManager)
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
        checkNulls,
        MOVE(comparerManager)
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
                comparerManager->GetHasher(keyTypes, builder.Module),
                comparerManager->GetEqComparer(keyTypes, builder.Module),
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

                builder->CreateMemCpy(
                    builder->CreatePointerCast(newValues, builder->getInt8PtrTy()),
                    builder->CreatePointerCast(values, builder->getInt8PtrTy()),
                    schemaSize * sizeof(TValue), 8);

                auto innerBuilder = TCGExprContext::Make(builder, *fragmentInfos, row, builder.Buffer);
                for (size_t index = 0; index < exprIds.size(); ++index) {
                    auto columnIndex = schemaSize + index;

                    CodegenFragment(innerBuilder, exprIds[index])
                        .StoreToValues(builder, newValues, columnIndex);
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

        auto comparator = CodegenOrderByComparerFunction(
            orderColumnTypes,
            builder.Module,
            schemaSize,
            isDesc);

        builder->CreateCall(
            builder.Module->GetRoutine("OrderOpHelper"),
            {
                builder.GetExecutionContext(),
                comparator,

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
            codegenAggregate.Initialize(builder, buffer)
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
            Value* statePtr,
            Value* newValuePtr
        ) {
            codegenAggregate.Update(builder, buffer, statePtr, newValuePtr);
            builder->CreateRetVoid();
        });

        module->ExportSymbol(updateName);
    }

    const auto mergeName = TString("merge");
    {
        MakeFunction<TCGAggregateMergeSignature>(module, mergeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* dstStatePtr,
            Value* statePtr
        ) {
            codegenAggregate.Merge(builder, buffer, dstStatePtr, statePtr);
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

