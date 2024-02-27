#include "cg_fragment_compiler.h"
#include "cg_ir_builder.h"
#include "cg_routines.h"
#include "cg_helpers.h"
#include "llvm_folding_set.h"
#include "position_independent_value_caller.h"
#include "web_assembly_caller.h"

#include <yt/yt/library/codegen/module.h>
#include <yt/yt/library/codegen/public.h>

#include <yt/yt/core/logging/log.h>

#include <llvm/IR/Module.h>

// TODO(sandello):
//  - Implement basic logging & profiling within evaluation code
//  - Sometimes we can write through scratch space; some simple cases:
//    * int/double/null expressions only,
//    * string expressions with references (just need to copy string data)
//    It is possible to do better memory management here.
//  - TBAA is a king

// TODO(lukyan): Remove offset from comparers and hashers to reduce its count.
// Add offset to values in other palaces instead.

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NConcurrency;

using NCodegen::TCGModule;
using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////
// Operator helpers
//

Value* CodegenAllocateValues(const TCGIRBuilderPtr& builder, size_t valueCount)
{
    Value* newValues = builder->CreateAlignedAlloca(
        TTypeBuilder<TPIValue>::Get(builder->getContext()),
        8,
        builder->getInt64(valueCount),
        "allocatedValues");

    return newValues;
}

Value* CodegenForEachRow(
    TCGContext& builder,
    Value* rows,
    Value* size,
    const TCodegenConsumer& codegenConsumer,
    const std::vector<int>& stringLikeColumnIndices = {})
{
    auto* loopBB = builder->CreateBBHere("loop");
    auto* condBB = builder->CreateBBHere("cond");
    auto* endLoopBB = builder->CreateBBHere("endLoop");

    // index = 0
    Type* indexType = builder->getInt64Ty();
    Value* indexPtr = builder->CreateAlloca(indexType, nullptr, "indexPtr");
    builder->CreateStore(builder->getInt64(0), indexPtr);

    builder->CreateBr(condBB);

    builder->SetInsertPoint(condBB);

    // if (index != size) ...
    Value* index = builder->CreateLoad(indexType, indexPtr, "index");
    Value* condition = builder->CreateICmpNE(index, size);
    builder->CreateCondBr(condition, loopBB, endLoopBB);

    builder->SetInsertPoint(loopBB);

    // row = rows[index]
    Value* stackState = builder->CreateStackSave("stackState");
    Type* rowPointerType = TTypeBuilder<TUnversionedValue*>::Get(builder->getContext());
    Value* rowPointer = builder->CreateGEP(rowPointerType, rows, index, "rowPointer");
    Type* rowType = TTypeBuilder<TUnversionedValue*>::Get(builder->getContext());
    Value* row = builder->CreateLoad(rowType, rowPointer, "row");

    // convert row to PI
    for (auto stringLikeColumnIndex : stringLikeColumnIndices) {
        // value = row[stringLikeColumnIndex]
        Value* valueToConvert = builder->CreateConstInBoundsGEP1_32(
            TValueTypeBuilder::Get(builder->getContext()),
            row,
            stringLikeColumnIndex,
            "unversionedValueToConvert");

        // convert value.Data to PI
        Value* dataPtr = builder->CreateConstInBoundsGEP2_32(
            TValueTypeBuilder::Get(builder->getContext()),
            valueToConvert,
            0,
            TValueTypeBuilder::Data,
            ".dataPointer");
        Value* data = builder->CreateLoad(
            TDataTypeBuilder::Get(builder->getContext()),
            dataPtr,
            ".nonPIData");
        Value* dataPtrAsInt = builder->CreatePtrToInt(
            dataPtr,
            data->getType(),
            ".nonPIDataAsInt");
        data = builder->CreateSub(data, dataPtrAsInt, ".PIdata");  // Similar to SetStringPosition.
        builder->CreateStore(data, dataPtr);
    }

    // consume(row)
    Value* finished = codegenConsumer(builder, row);
    builder->CreateStackRestore(stackState);
    loopBB = builder->GetInsertBlock();

    // index = index + 1
    builder->CreateStore(builder->CreateAdd(index, builder->getInt64(1)), indexPtr);
    builder->CreateCondBr(finished, endLoopBB, condBB);

    builder->SetInsertPoint(endLoopBB);

    return MakePhi(builder, loopBB, condBB, builder->getTrue(), builder->getFalse(), "finishedPhi");
}

////////////////////////////////////////////////////////////////////////////////
// Expressions
//

void ThrowNaNException(TCGBaseContext& builder)
{
    builder->CreateCall(
        builder.Module->GetRoutine("ThrowQueryException"),
        {
            builder->CreateGlobalStringPtr("Comparison with NaN")
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

Value* CodegenFingerprint64(const TCGIRBuilderPtr& builder, Value* x)
{
    Value* kMul = builder->getInt64(0x9ddfea08eb382d69ULL);
    Value* b = builder->CreateMul(x, kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(44)));
    b = builder->CreateMul(b, kMul);
    b = builder->CreateXor(b, builder->CreateLShr(b, builder->getInt64(41)));
    b = builder->CreateMul(b, kMul);
    return b;
}

Value* CodegenFingerprint128(const TCGIRBuilderPtr& builder, Value* x, Value* y)
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
}

TValueTypeLabels CodegenHasherBody(
    TCGBaseContext& builder,
    Value* labelsArray,
    Value* values,
    Value* startIndex,
    Value* finishIndex)
{
    auto codegenHashCombine = [&] (const TCGIRBuilderPtr& builder, Value* first, Value* second) -> Value* {
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

    BasicBlock* hashInt8ScalarBB = nullptr;
    {
        hashInt8ScalarBB = builder->CreateBBHere("hashNull");
        builder->SetInsertPoint(hashInt8ScalarBB);

        auto valuePtr = builder->CreateInBoundsGEP(
            TValueTypeBuilder::Get(builder->getContext()),
            values,
            indexPhi);
        auto value = TCGValue::LoadFromRowValue(
            builder,
            valuePtr,
            EValueType::Boolean);

        result2Phi->addIncoming(builder->getInt64(0), builder->GetInsertBlock());
        BasicBlock* hashDataBB = builder->CreateBBHere("hashData");
        builder->CreateCondBr(value.GetIsNull(builder), gotoNextBB, hashDataBB);

        builder->SetInsertPoint(hashDataBB);
        Value* hashResult = CodegenFingerprint64(
            builder,
            builder->CreateIntCast(value.GetTypedData(builder), builder->getInt64Ty(), /*isSigned*/ false));

        result2Phi->addIncoming(hashResult, builder->GetInsertBlock());
        builder->CreateBr(gotoNextBB);
    };

    BasicBlock* hashInt64ScalarBB = nullptr;
    {
        hashInt64ScalarBB = builder->CreateBBHere("hashNull");
        builder->SetInsertPoint(hashInt64ScalarBB);

        auto valuePtr = builder->CreateInBoundsGEP(
            TValueTypeBuilder::Get(builder->getContext()),
            values,
            indexPhi);
        auto value = TCGValue::LoadFromRowValue(
            builder,
            valuePtr,
            EValueType::Int64);

        result2Phi->addIncoming(builder->getInt64(0), builder->GetInsertBlock());
        BasicBlock* hashDataBB = builder->CreateBBHere("hashData");
        builder->CreateCondBr(value.GetIsNull(builder), gotoNextBB, hashDataBB);

        builder->SetInsertPoint(hashDataBB);
        Value* hashResult = CodegenFingerprint64(
            builder,
            value.GetTypedData(builder));

        result2Phi->addIncoming(hashResult, builder->GetInsertBlock());
        builder->CreateBr(gotoNextBB);
    };

    BasicBlock* cmpStringBB = nullptr;
    {
        cmpStringBB = builder->CreateBBHere("hashNull");
        builder->SetInsertPoint(cmpStringBB);

        auto valuePtr = builder->CreateInBoundsGEP(
            TValueTypeBuilder::Get(builder->getContext()),
            values,
            indexPhi);
        auto value = TCGValue::LoadFromRowValue(
            builder,
            valuePtr,
            EValueType::String);

        result2Phi->addIncoming(builder->getInt64(0), builder->GetInsertBlock());
        BasicBlock* hashDataBB = builder->CreateBBHere("hashData");
        builder->CreateCondBr(value.GetIsNull(builder), gotoNextBB, hashDataBB);

        builder->SetInsertPoint(hashDataBB);
        Value* hashResult = builder->CreateCall(
            builder.Module->GetRoutine("StringHash"),
            {
                value.GetTypedData(builder),
                value.GetLength()
            });

        result2Phi->addIncoming(hashResult, builder->GetInsertBlock());
        builder->CreateBr(gotoNextBB);
    };

    builder->SetInsertPoint(gotoHashBB);

    Value* offsetPtr = builder->CreateGEP(builder->getInt8PtrTy(), labelsArray, indexPhi);
    Value* offset = builder->CreateLoad(builder->getInt8PtrTy(), offsetPtr);
    auto* indirectBranch = builder->CreateIndirectBr(offset);
    indirectBranch->addDestination(hashInt8ScalarBB);
    indirectBranch->addDestination(hashInt64ScalarBB);
    indirectBranch->addDestination(cmpStringBB);

    return TValueTypeLabels{
        llvm::BlockAddress::get(hashInt8ScalarBB),
        llvm::BlockAddress::get(hashInt64ScalarBB),
        llvm::BlockAddress::get(hashInt64ScalarBB),
        llvm::BlockAddress::get(hashInt64ScalarBB),
        llvm::BlockAddress::get(cmpStringBB)
    };
}

////////////////////////////////////////////////////////////////////////////////

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

        auto lhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);
        Value* lhsData = lhsValue.GetTypedData(builder);
        Value* rhsData = rhsValue.GetTypedData(builder);

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

        auto lhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                lhsValues,
                indexPhi),
            type);

        auto rhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                rhsValues,
                indexPhi),
            type);

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);

        BasicBlock* cmpNullBB = builder->CreateBBHere(Twine("cmpNull.").concat(name));
        BasicBlock* cmpDataBB = builder->CreateBBHere(Twine("cmpData.").concat(name));
        builder->CreateCondBr(builder->CreateOr(lhsIsNull, rhsIsNull), cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        resultPhi->addIncoming(builder->CreateICmpUGT(lhsIsNull, rhsIsNull), builder->GetInsertBlock());
        builder->CreateCondBr(builder->CreateAnd(lhsIsNull, rhsIsNull), gotoNextBB, returnBB);

        builder->SetInsertPoint(cmpDataBB);
        Value* lhsData = lhsValue.GetTypedData(builder);
        Value* rhsData = rhsValue.GetTypedData(builder);

        if (type == EValueType::Double) {
            CodegenIf<TCGBaseContext>(
                builder,
                builder->CreateFCmpUNO(lhsData, rhsData),
                ThrowNaNException);
        }

        resultPhi->addIncoming(cmpLess(builder, lhsData, rhsData), builder->GetInsertBlock());
        builder->CreateCondBr(cmpEqual(builder, lhsData, rhsData), gotoNextBB, returnBB);

        return cmpBB;
    };

    BasicBlock* cmpBooleanBB = cmpScalar(
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpEQ(lhsData, rhsData);
        },
        [] (TCGBaseContext& builder, Value* lhsData, Value* rhsData) {
            return builder->CreateICmpULT(lhsData, rhsData);
        },
        EValueType::Boolean,
        "bool");

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

        auto lhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                lhsValues,
                indexPhi),
            EValueType::String);

        auto rhsValue = TCGValue::LoadFromRowValue(
            builder,
            builder->CreateInBoundsGEP(
                TValueTypeBuilder::Get(builder->getContext()),
                rhsValues,
                indexPhi),
            EValueType::String);

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);

        Value* anyNull = builder->CreateOr(lhsIsNull, rhsIsNull);

        BasicBlock* cmpNullBB = builder->CreateBBHere("cmpNull.string");
        BasicBlock* cmpDataBB = builder->CreateBBHere("cmpData.string");
        builder->CreateCondBr(anyNull, cmpNullBB, cmpDataBB);

        builder->SetInsertPoint(cmpNullBB);
        resultPhi->addIncoming(builder->CreateICmpUGT(lhsIsNull, rhsIsNull), builder->GetInsertBlock());
        builder->CreateCondBr(builder->CreateAnd(lhsIsNull, rhsIsNull), gotoNextBB, returnBB);

        builder->SetInsertPoint(cmpDataBB);

        Value* lhsLength = lhsValue.GetLength();
        Value* rhsLength = rhsValue.GetLength();

        Value* cmpLength = builder->CreateICmpULT(lhsLength, rhsLength);

        Value* minLength = builder->CreateSelect(
            cmpLength,
            lhsLength,
            rhsLength);

        Value* lhsData = lhsValue.GetTypedData(builder);
        Value* rhsData = rhsValue.GetTypedData(builder);

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

    Value* offsetPtr = builder->CreateGEP(builder->getInt8PtrTy(), labelsArray, indexPhi);
    Value* offset = builder->CreateLoad(builder->getInt8PtrTy(), offsetPtr);
    auto* indirectBranch = builder->CreateIndirectBr(offset);
    indirectBranch->addDestination(cmpBooleanBB);
    indirectBranch->addDestination(cmpIntBB);
    indirectBranch->addDestination(cmpUintBB);
    indirectBranch->addDestination(cmpDoubleBB);
    indirectBranch->addDestination(cmpStringBB);

    return TValueTypeLabels{
        llvm::BlockAddress::get(cmpBooleanBB),
        llvm::BlockAddress::get(cmpIntBB),
        llvm::BlockAddress::get(cmpUintBB),
        llvm::BlockAddress::get(cmpDoubleBB),
        llvm::BlockAddress::get(cmpStringBB)
    };
}

struct TComparerManager
    : public TRefCounted
{
    Function* Hasher;
    TValueTypeLabels HashLabels;

    Function* UniversalComparer;
    TValueTypeLabels UniversalLabels;

    THashMap<llvm::FoldingSetNodeID, llvm::GlobalVariable*> Labels;

    THashMap<llvm::FoldingSetNodeID, Function*> Hashers;
    THashMap<llvm::FoldingSetNodeID, Function*> EqComparers;
    THashMap<llvm::FoldingSetNodeID, Function*> LessComparers;
    THashMap<llvm::FoldingSetNodeID, Function*> TernaryComparers;

    llvm::GlobalVariable* GetLabelsArray(
        TCGBaseContext& builder,
        const std::vector<EValueType>& types,
        const TValueTypeLabels& valueTypeLabels);


    void GetUniversalComparer(const TCGModulePtr& module)
    {
        if (!UniversalComparer) {
            UniversalComparer = MakeFunction<i64(char**, TPIValue*, TPIValue*, size_t, size_t)>(
                module,
                "UniversalComparerImpl",
            [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* lhsValues,
                Value* rhsValues,
                Value* startIndex,
                Value* finishIndex
            ) {
                UniversalLabels = CodegenLessComparerBody(
                    builder,
                    labelsArray,
                    lhsValues,
                    rhsValues,
                    startIndex,
                    finishIndex,
                    [&] (TCGBaseContext& builder) {
                        builder->CreateRet(builder->getInt64(0));
                    },
                    [&] (TCGBaseContext& builder, Value* result, Value* index) {
                        index = builder->CreateAdd(index, builder->getInt64(1));
                        builder->CreateRet(
                            builder->CreateSelect(result, builder->CreateNeg(index), index));
                    });
            });
        }
    }

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

    Function* CodegenOrderByComparerFunction(
        const std::vector<EValueType>& types,
        const TCGModulePtr& module,
        size_t offset,
        const std::vector<bool>& isDesc);
};

DEFINE_REFCOUNTED_TYPE(TComparerManager)

TComparerManagerPtr MakeComparerManager()
{
    return New<TComparerManager>();
}

llvm::GlobalVariable* TComparerManager::GetLabelsArray(
    TCGBaseContext& builder,
    const std::vector<EValueType>& types,
    const TValueTypeLabels& valueTypeLabels)
{
    std::vector<Constant*> labels;
    labels.reserve(types.size());
    llvm::FoldingSetNodeID id;
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
                YT_ABORT();
        }
        id.AddPointer(labels.back());
    }

    auto emplaced = Labels.emplace(id, nullptr);
    if (emplaced.second) {
        llvm::ArrayType* labelArrayType = llvm::ArrayType::get(
            TTypeBuilder<char*>::Get(builder->getContext()),
            types.size());

        emplaced.first->second = new llvm::GlobalVariable(
            *builder.Module->GetModule(),
            labelArrayType,
            true,
            llvm::GlobalVariable::ExternalLinkage,
            llvm::ConstantArray::get(
                labelArrayType,
                labels));
    }

    return emplaced.first->second;
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
            Hasher = MakeFunction<ui64(char**, TPIValue*, size_t, size_t)>(module, "HasherImpl", [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* values,
                Value* startIndex,
                Value* finishIndex
            ) {
                HashLabels = CodegenHasherBody(
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
                            GetLabelsArray(builder, types, HashLabels),
                            TTypeBuilder<char**>::Get(builder->getContext())),
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
    YT_VERIFY(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = EqComparers.emplace(id, nullptr);
    if (emplaced.second) {
        GetUniversalComparer(module);

        emplaced.first->second = MakeFunction<TComparerFunction>(module, "EqComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt8(1);
            } else {
                result = builder->CreateCall(
                    UniversalComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, UniversalLabels),
                            TTypeBuilder<char**>::Get(builder->getContext())),
                        lhsRow,
                        rhsRow,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
                result = builder->CreateZExt(
                    builder->CreateICmpEQ(result, builder->getInt64(0)),
                    builder->getInt8Ty());
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
    YT_VERIFY(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = LessComparers.emplace(id, nullptr);
    if (emplaced.second) {
        GetUniversalComparer(module);

        emplaced.first->second = MakeFunction<TComparerFunction>(module, "LessComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt8(0);
            } else {
                result = builder->CreateCall(
                    UniversalComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, UniversalLabels),
                            TTypeBuilder<char**>::Get(builder->getContext())),
                        lhsRow,
                        rhsRow,
                        builder->getInt64(start),
                        builder->getInt64(finish)
                    });
                result = builder->CreateZExt(
                    builder->CreateICmpSLT(result, builder->getInt64(0)),
                    builder->getInt8Ty());
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
    YT_VERIFY(finish <= types.size());

    llvm::FoldingSetNodeID id;
    id.AddInteger(start);
    id.AddInteger(finish);
    for (size_t index = start; index < finish; ++index)  {
        id.AddInteger(static_cast<ui16>(types[index]));
    }

    auto emplaced = TernaryComparers.emplace(id, nullptr);
    if (emplaced.second) {
        GetUniversalComparer(module);

        emplaced.first->second = MakeFunction<TTernaryComparerFunction>(module, "TernaryComparer", [&] (
            TCGBaseContext& builder,
            Value* lhsRow,
            Value* rhsRow
        ) {
            Value* result;
            if (start == finish) {
                result = builder->getInt32(0);
            } else {
                result = builder->CreateCall(
                    UniversalComparer,
                    {
                        builder->CreatePointerCast(
                            GetLabelsArray(builder, types, UniversalLabels),
                            TTypeBuilder<char**>::Get(builder->getContext())),
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

Function* TComparerManager::CodegenOrderByComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModulePtr& module,
    size_t offset,
    const std::vector<bool>& isDesc)
{
    GetUniversalComparer(module);

    return MakeFunction<char(TPIValue*, TPIValue*)>(module, "OrderByComparer", [&] (
        TCGBaseContext& builder,
        Value* lhsValues,
        Value* rhsValues
    ) {
        Type* type = TValueTypeBuilder::Get(builder->getContext());
        lhsValues = builder->CreateGEP(type, lhsValues, builder->getInt64(offset));
        rhsValues = builder->CreateGEP(type, rhsValues, builder->getInt64(offset));

        std::vector<Constant*> isDescFlags(std::ssize(types));
        for (int index = 0; index < std::ssize(types); ++index) {
            isDescFlags[index] = builder->getInt8(index < std::ssize(isDesc) && isDesc[index]);
        }

        llvm::ArrayType* isDescArrayType = llvm::ArrayType::get(
            TTypeBuilder<char>::Get(builder->getContext()),
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

        Value* result = builder->CreateCall(
            UniversalComparer,
            {
                builder->CreatePointerCast(
                    GetLabelsArray(builder, types, UniversalLabels),
                    TTypeBuilder<char**>::Get(builder->getContext())),
                lhsValues,
                rhsValues,
                builder->getInt64(0),
                builder->getInt64(types.size())
            });

        auto* thenBB = builder->CreateBBHere("then");
        auto* elseBB = builder->CreateBBHere("else");
        builder->CreateCondBr(builder->CreateICmpEQ(result, builder->getInt64(0)), thenBB, elseBB);
        builder->SetInsertPoint(thenBB);
        builder->CreateRet(builder->getInt8(false));
        builder->SetInsertPoint(elseBB);

        Value* isLess = builder->CreateICmpSLT(result, builder->getInt64(0));

        Value* index = builder->CreateSub(
            builder->CreateSelect(isLess, builder->CreateNeg(result), result),
            builder->getInt64(1));

        result = builder->CreateXor(
            builder->CreateZExt(isLess, builder->getInt8Ty()),
            builder->CreateLoad(
                builder->getInt8Ty(),
                builder->CreateGEP(
                    isDescArrayType,
                    isDescArray,
                    llvm::ArrayRef<llvm::Value*>{builder->getInt64(0), index})));

        builder->CreateRet(result);
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
}

////////////////////////////////////////////////////////////////////////////////

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    bool nullbale,
    EValueType type)
{
    return [
            index,
            nullbale,
            type
        ] (TCGExprContext& builder) {
            return TCGValue::LoadFromRowValues(
                builder,
                builder.GetLiterals(),
                index,
                nullbale,
                false,
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
            =,
            name = std::move(name)
        ] (TCGExprContext& builder) {
            return TCGValue::LoadFromRowValues(
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
                builder.GetExpressionClosurePtr(),
                builder.GetLiterals(),
                builder.RowValues
            });

        return TCGValue::LoadFromRowValue(
            builder,
            builder.GetFragmentResult(id),
            expressionFragment.Nullable,
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
                TTypeBuilder<void>::Get(module->GetModule()->getContext()),
                {
                    llvm::PointerType::getUnqual(
                        TTypeBuilder<TExpressionClosure>::Get(
                            module->GetModule()->getContext(),
                            fragmentInfos.Functions.size())),
                    TTypeBuilder<TPIValue*>::Get(module->GetModule()->getContext()),
                    TTypeBuilder<TPIValue*>::Get(module->GetModule()->getContext())
                },
                true);

            Function* function = Function::Create(
                functionType,
                Function::ExternalLinkage,
                name.c_str(),
                module->GetModule());

            function->addFnAttr(BuildUnwindTableAttribute(module->GetModule()->getContext()));
            function->addFnAttr(llvm::Attribute::AttrKind::NoInline);
            function->addFnAttr(llvm::Attribute::OptimizeForSize);

            auto args = function->arg_begin();
            Value* expressionClosure = ConvertToPointer(args++);
            Value* literals = ConvertToPointer(args++);
            Value* rowValues = ConvertToPointer(args++);
            {
                TCGIRBuilder irBuilder(function);
                auto innerBuilder = TCGExprContext::Make(
                    TCGBaseContext(TCGIRBuilderPtr(&irBuilder), module),
                    fragmentInfos,
                    expressionClosure,
                    literals,
                    rowValues);

                Value* fragmentFlag = innerBuilder.GetFragmentFlag(id);

                auto* evaluationNeeded = innerBuilder->CreateICmpEQ(
                    innerBuilder->CreateLoad(innerBuilder->getInt8Ty(), fragmentFlag),
                    innerBuilder->getInt8(static_cast<int>(EValueType::TheBottom)));

                CodegenIf<TCGExprContext>(
                    innerBuilder,
                    evaluationNeeded,
                    [&] (TCGExprContext& builder) {
                        builder.ExpressionFragments.Items[id].Generator(builder)
                            .StoreToValue(builder, builder.GetFragmentResult(id));
                    });

                innerBuilder->CreateRetVoid();
            }



            fragmentInfos.Functions[fragmentInfos.Items[id].Index] = function;
        }
    }
}

void MakeCodegenFragmentBodies(
    TCodegenSource* codegenSource,
    TCodegenFragmentInfosPtr fragmentInfos)
{
    *codegenSource = [
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos)
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
        =,
        name = std::move(name)
    ] (TCGExprContext& builder) {
        auto operandValue = CodegenFragment(builder, operandId);

        auto evaluate = [&] (const TCGIRBuilderPtr& builder) {
            auto operandType = operandValue.GetStaticType();
            Value* operandData = operandValue.GetTypedData(builder);
            Value* evalData = nullptr;

            switch (opcode) {
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
                            evalData = builder->CreateFSub(
                                ConstantFP::get(builder->getDoubleTy(), 0.0),
                                operandData);
                            break;
                        default:
                            YT_ABORT();
                    }
                    break;


                case EUnaryOp::BitNot:
                case EUnaryOp::Not:
                    evalData = builder->CreateNot(operandData);
                    break;

                default:
                    YT_ABORT();
            }

            return TCGValue::Create(
                builder,
                builder->getFalse(),
                nullptr,
                evalData,
                type);
        };

        if (builder.ExpressionFragments.Items[operandId].Nullable) {
            return CodegenIf<TCGIRBuilderPtr, TCGValue>(
                builder,
                operandValue.GetIsNull(builder),
                [&] (const TCGIRBuilderPtr& builder) {
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
        =,
        name = std::move(name)
    ] (TCGExprContext& builder) {
        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        const auto& items = builder.ExpressionFragments.Items;

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);

        if (!items[lhsId].Nullable) {
            YT_VERIFY(llvm::dyn_cast<llvm::Constant>(lhsIsNull));
        }

        if (!items[rhsId].Nullable) {
            YT_VERIFY(llvm::dyn_cast<llvm::Constant>(rhsIsNull));
        }

        Value* lhsData = lhsValue.GetTypedData(builder);
        Value* rhsData = rhsValue.GetTypedData(builder);

        Value* lhsMagic = lhsData;
        Value* rhsMagic = rhsData;

        if (opcode == EBinaryOp::Or) {
            lhsMagic = builder->CreateNot(lhsMagic);
            rhsMagic = builder->CreateNot(rhsMagic);
        }

        Value* isNull = builder->CreateOr(lhsIsNull, rhsIsNull);
        isNull = builder->CreateAnd(isNull, builder->CreateOr(lhsIsNull, lhsMagic));
        isNull = builder->CreateAnd(isNull, builder->CreateOr(rhsIsNull, rhsMagic));

        Value* result = nullptr;

        if (opcode == EBinaryOp::Or) {
            result = builder->CreateOr(lhsData, rhsData);
        } else {
            result = builder->CreateAnd(lhsData, rhsData);
        }

        return TCGValue::Create(builder, isNull, nullptr, result, type);
    };
}

TCodegenExpression MakeCodegenRelationalBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name,
    bool useCanonicalNullRelations)
{
    return [
        =,
        name = std::move(name)
    ] (TCGExprContext& builder) {
        auto nameTwine = Twine(name.c_str());
        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);

        #define CMP_OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->Create##optype(lhsData, rhsData); \
                break;

        auto compareNulls = [&] () {
            if (useCanonicalNullRelations) {
                return TCGValue::CreateNull(builder, type);
            }

            // swap args
            Value* lhsData = rhsIsNull;
            Value* rhsData = lhsIsNull;
            Value* evalData = nullptr;

            switch (opcode) {
                CMP_OP(Equal, ICmpEQ)
                CMP_OP(NotEqual, ICmpNE)
                CMP_OP(Less, ICmpULT)
                CMP_OP(LessOrEqual, ICmpULE)
                CMP_OP(Greater, ICmpUGT)
                CMP_OP(GreaterOrEqual, ICmpUGE)
                default:
                    YT_ABORT();
            }

            return TCGValue::Create(
                builder,
                builder->getFalse(),
                nullptr,
                evalData,
                type);
        };

        if (!IsStringLikeType(lhsValue.GetStaticType()) && !IsStringLikeType(rhsValue.GetStaticType())) {
            YT_VERIFY(lhsValue.GetStaticType() == rhsValue.GetStaticType());

            auto operandType = lhsValue.GetStaticType();

            Value* lhsData = lhsValue.GetTypedData(builder);
            Value* rhsData = rhsValue.GetTypedData(builder);
            Value* evalData = nullptr;

            switch (operandType) {
                case EValueType::Int64:
                    switch (opcode) {
                        CMP_OP(Equal, ICmpEQ)
                        CMP_OP(NotEqual, ICmpNE)
                        CMP_OP(Less, ICmpSLT)
                        CMP_OP(LessOrEqual, ICmpSLE)
                        CMP_OP(Greater, ICmpSGT)
                        CMP_OP(GreaterOrEqual, ICmpSGE)
                        default:
                            YT_ABORT();
                    }
                    break;
                case EValueType::Boolean:
                case EValueType::Uint64:
                    switch (opcode) {
                        CMP_OP(Equal, ICmpEQ)
                        CMP_OP(NotEqual, ICmpNE)
                        CMP_OP(Less, ICmpULT)
                        CMP_OP(LessOrEqual, ICmpULE)
                        CMP_OP(Greater, ICmpUGT)
                        CMP_OP(GreaterOrEqual, ICmpUGE)
                        default:
                            YT_ABORT();
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
                            YT_ABORT();
                    }
                    break;
                default:
                    YT_ABORT();
            }

            Value* anyNull = builder->CreateOr(lhsIsNull, rhsIsNull);

            if (operandType == EValueType::Double) {
                CodegenIf<TCGBaseContext>(
                    builder,
                    builder->CreateAnd(
                        builder->CreateFCmpUNO(lhsData, rhsData),
                        builder->CreateNot(anyNull)),
                    ThrowNaNException);
            }

            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                anyNull,
                [&] (TCGBaseContext& /*builder*/) {
                    return compareNulls();
                },
                [&] (TCGBaseContext& builder) {
                    return TCGValue::Create(
                        builder,
                        builder->getFalse(),
                        nullptr,
                        evalData,
                        type);
                });
        }

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
                    evalData = builder->CreateICmpSLE(cmpResult, builder->getInt32(0));
                    break;
                case EBinaryOp::GreaterOrEqual:
                    evalData = builder->CreateICmpSGE(cmpResult, builder->getInt32(0));
                    break;
                default:
                    YT_ABORT();
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
                    Value* lhsData = lhsValue.GetTypedData(builder);
                    Value* rhsData = rhsValue.GetTypedData(builder, true);
                    Value* lhsLength = lhsValue.GetLength();

                    Value* cmpResult = nullptr;

                    switch (rhsValue.GetStaticType()) {
                        case EValueType::Boolean: {
                            cmpResult = builder->CreateCall(
                                builder.Module->GetRoutine("CompareAnyBoolean"),
                                {
                                    lhsData,
                                    lhsLength,
                                    rhsData,
                                });
                            break;
                        }
                        case EValueType::Int64: {
                            cmpResult = builder->CreateCall(
                                builder.Module->GetRoutine("CompareAnyInt64"),
                                {
                                    lhsData,
                                    lhsLength,
                                    rhsData
                                });
                            break;
                        }
                        case EValueType::Uint64: {
                            cmpResult = builder->CreateCall(
                                builder.Module->GetRoutine("CompareAnyUint64"),
                                {
                                    lhsData,
                                    lhsLength,
                                    rhsData
                                });
                            break;
                        }
                        case EValueType::Double: {
                            cmpResult = builder->CreateCall(
                                builder.Module->GetRoutine("CompareAnyDouble"),
                                {
                                    lhsData,
                                    lhsLength,
                                    rhsData
                                });
                            break;
                        }
                        case EValueType::String: {
                            cmpResult = builder->CreateCall(
                                builder.Module->GetRoutine("CompareAnyString"),
                                {
                                    lhsData,
                                    lhsLength,
                                    rhsData,
                                    rhsValue.GetLength()
                                });
                            break;
                        }
                        default:
                            YT_ABORT();
                    }

                    return TCGValue::Create(
                        builder,
                        builder->getFalse(),
                        nullptr,
                        cmpResultToResult(builder, cmpResult, resultOpcode),
                        type);
                } else {
                    YT_ABORT();
                }
            }

            YT_VERIFY(lhsValue.GetStaticType() == rhsValue.GetStaticType());

            auto operandType = lhsValue.GetStaticType();

            Value* lhsData = lhsValue.GetTypedData(builder);
            Value* rhsData = rhsValue.GetTypedData(builder);
            Value* evalData = nullptr;

            switch (operandType) {
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
                            YT_ABORT();
                    }

                    break;
                }

                case EValueType::Any: {
                    Value* lhsLength = lhsValue.GetLength();
                    Value* rhsLength = rhsValue.GetLength();

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
                    YT_ABORT();
            }

            return TCGValue::Create(
                builder,
                builder->getFalse(),
                nullptr,
                evalData,
                type);
        };

        #undef CMP_OP

        return builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                builder->CreateOr(lhsIsNull, rhsIsNull),
                [&] (TCGBaseContext& /*builder*/) {
                    return compareNulls();
                },
                compare,
                nameTwine)
            : compare(builder);
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
        =,
        name = std::move(name)
    ] (TCGExprContext& builder) {
        auto nameTwine = Twine(name.c_str());

        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        YT_VERIFY(lhsValue.GetStaticType() == rhsValue.GetStaticType());
        auto operandType = lhsValue.GetStaticType();

        #define OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->Create##optype(lhsData, rhsData); \
                break;

        Value* anyNull =
            builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? builder->CreateOr(lhsValue.GetIsNull(builder), rhsValue.GetIsNull(builder))
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
                    Value* lhsData = lhsValue.GetTypedData(builder);
                    Value* rhsData = rhsValue.GetTypedData(builder);
                    Value* evalData = nullptr;

                    CodegenIf<TCGBaseContext>(
                        builder,
                        builder->CreateIsNull(rhsData),
                        [] (TCGBaseContext& builder) {
                            builder->CreateCall(
                                builder.Module->GetRoutine("ThrowQueryException"),
                                {
                                    builder->CreateGlobalStringPtr("Division by zero")
                                });
                        });

                    if (operandType == EValueType::Int64) {
                        CodegenIf<TCGBaseContext>(
                            builder,
                            builder->CreateAnd(
                                builder->CreateICmpEQ(lhsData, builder->getInt64(std::numeric_limits<i64>::min())),
                                builder->CreateICmpEQ(rhsData, builder->getInt64(-1))),
                            [] (TCGBaseContext& builder) {
                                builder->CreateCall(
                                    builder.Module->GetRoutine("ThrowQueryException"),
                                    {
                                        builder->CreateGlobalStringPtr("Division of INT_MIN by -1")
                                    });
                            });
                    }

                    switch (operandType) {
                        case EValueType::Int64:
                            switch (opcode) {
                                OP(Divide, SDiv)
                                OP(Modulo, SRem)
                                default:
                                    YT_ABORT();
                            }
                            break;
                        case EValueType::Uint64:
                            switch (opcode) {
                                OP(Divide, UDiv)
                                OP(Modulo, URem)
                                default:
                                    YT_ABORT();
                            }
                            break;
                        default:
                            YT_ABORT();
                    }

                    return TCGValue::Create(
                        builder,
                        builder->getFalse(),
                        nullptr,
                        evalData,
                        type);
                },
                nameTwine);
        } else {
            Value* lhsData = lhsValue.GetTypedData(builder);
            Value* rhsData = rhsValue.GetTypedData(builder);
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
                        OP(RightShift, AShr)
                        default:
                            YT_ABORT();
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
                            YT_ABORT();
                    }
                    break;
                case EValueType::Double:
                    switch (opcode) {
                        OP(Plus, FAdd)
                        OP(Minus, FSub)
                        OP(Multiply, FMul)
                        OP(Divide, FDiv)
                        default:
                            YT_ABORT();
                    }
                    break;
                default:
                    YT_ABORT();
            }

            return TCGValue::Create(
                builder,
                anyNull,
                nullptr,
                evalData,
                type);
        }

        #undef OP
    };
}

TCodegenExpression MakeCodegenStringBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name)
{
    YT_VERIFY(type == EValueType::String);
    YT_VERIFY(opcode == EBinaryOp::Concatenate);

    return [
        =,
        name = std::move(name)
    ] (TCGExprContext& builder) {
        auto nameTwine = Twine(name.c_str());

        auto lhsValue = CodegenFragment(builder, lhsId);
        auto rhsValue = CodegenFragment(builder, rhsId);

        YT_VERIFY(lhsValue.GetStaticType() == EValueType::String);
        YT_VERIFY(rhsValue.GetStaticType() == EValueType::String);

        Value* anyNull =
            builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? builder->CreateOr(lhsValue.GetIsNull(builder), rhsValue.GetIsNull(builder))
            : builder->getFalse();

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            anyNull,
            [&] (TCGExprContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGExprContext& builder) {
                Value* lhsData = lhsValue.GetTypedData(builder);
                Value* rhsData = rhsValue.GetTypedData(builder);

                Value* lhsLength = lhsValue.GetLength();
                Value* rhsLength = rhsValue.GetLength();
                Value* totalLength = builder->CreateAdd(lhsLength, rhsLength);

                Value* concatenation = builder->CreateCall(
                    builder.Module->GetRoutine("AllocateBytes"),
                    {
                        builder.Buffer,
                        builder->CreateIntCast(totalLength, builder->getInt64Ty(), false),
                    });

                builder->CreateMemCpy(
                    concatenation,
                    llvm::Align(1),
                    lhsData,
                    llvm::Align(1),
                    lhsLength);

                builder->CreateMemCpy(
                    builder->CreateInBoundsGEP(
                        TTypeBuilder<char>::Get(builder->getContext()),
                        concatenation,
                        lhsLength),
                    llvm::Align(1),
                    rhsData,
                    llvm::Align(1),
                    rhsLength);

                return TCGValue::Create(
                    builder,
                    anyNull,
                    totalLength,
                    concatenation,
                    type);
            },
            nameTwine);
    };
}

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name,
    bool useCanonicalNullRelations)
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
            std::move(name),
            useCanonicalNullRelations);
    } else if (IsArithmeticalBinaryOp(opcode) || IsIntegralBinaryOp(opcode)) {
        return MakeCodegenArithmeticBinaryOpExpr(
            opcode,
            lhsId,
            rhsId,
            type,
            std::move(name));
    } else if (IsStringBinaryOp(opcode)) {
        return MakeCodegenStringBinaryOpExpr(
            opcode,
            lhsId,
            rhsId,
            type,
            std::move(name));
    } else {
        YT_ABORT();
    }
}

TCodegenExpression MakeCodegenInExpr(
    std::vector<size_t> argIds,
    int arrayIndex,
    int hashtableIndex,
    TComparerManagerPtr comparerManager)
{
    return [
        =,
        argIds = std::move(argIds),
        comparerManager = std::move(comparerManager)
    ] (TCGExprContext& builder) {
        auto keySize = std::ssize(argIds);

        Value* newValues = CodegenAllocateValues(builder, keySize);

        std::vector<EValueType> keyTypes(keySize);
        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            keyTypes[index] = value.GetStaticType();
            value.StoreToValues(builder, newValues, index);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("IsRowInRowset"),
            {
                comparerManager->GetLessComparer(keyTypes, builder.Module),
                comparerManager->GetHasher(keyTypes, builder.Module),
                comparerManager->GetEqComparer(keyTypes, builder.Module),
                newValues,
                builder.GetOpaqueValue(arrayIndex),
                builder.GetOpaqueValue(hashtableIndex)
            });

        return TCGValue::Create(
            builder,
            builder->getFalse(),
            nullptr,
            builder->CreateTrunc(result, builder->getInt1Ty()),
            EValueType::Boolean);
    };
}

TCodegenExpression MakeCodegenBetweenExpr(
    std::vector<size_t> argIds,
    int arrayIndex,
    TComparerManagerPtr comparerManager)
{
    return [
        =,
        argIds = std::move(argIds),
        comparerManager = std::move(comparerManager)
    ] (TCGExprContext& builder) {
        auto keySize = std::ssize(argIds);

        Value* newValues = CodegenAllocateValues(builder, keySize);

        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            value.StoreToValues(builder, newValues, index);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("IsRowInRanges"),
            {
                builder->getInt32(keySize),
                newValues,
                builder.GetOpaqueValue(arrayIndex)
            });

        return TCGValue::Create(
            builder,
            builder->getFalse(),
            nullptr,
            builder->CreateTrunc(result, builder->getInt1Ty()),
            EValueType::Boolean);
    };
}

TCodegenExpression MakeCodegenTransformExpr(
    std::vector<size_t> argIds,
    std::optional<size_t> defaultExprId,
    int arrayIndex,
    int hashtableIndex,
    EValueType resultType,
    TComparerManagerPtr comparerManager)
{
    return [
        =,
        argIds = std::move(argIds),
        comparerManager = std::move(comparerManager)
    ] (TCGExprContext& builder) {
        auto keySize = std::ssize(argIds);

        Value* newValues = CodegenAllocateValues(builder, keySize);

        std::vector<EValueType> keyTypes(keySize);
        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            keyTypes[index] = value.GetStaticType();
            value.StoreToValues(builder, newValues, index);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("TransformTuple"),
            {
                comparerManager->GetLessComparer(keyTypes, builder.Module),
                comparerManager->GetHasher(keyTypes, builder.Module),
                comparerManager->GetEqComparer(keyTypes, builder.Module),
                newValues,
                builder.GetOpaqueValue(arrayIndex),
                builder.GetOpaqueValue(hashtableIndex)
            });

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            builder->CreateIsNotNull(result),
            [&] (TCGExprContext& builder) {
                return TCGValue::LoadFromRowValues(
                    builder,
                    result,
                    keySize,
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

namespace NDetail {

TCGValue MakeCodegenCaseExprRecursive(
    Function* eqComparer,
    Value* optionalOperand,
    std::optional<EValueType> optionalOperandType,
    Value* evaluatedCondition,
    TRange<std::pair<size_t, size_t>> whenThenExpressionIds,
    std::optional<size_t> defaultId,
    EValueType resultType,
    TCGExprContext& builder)
{
    if (whenThenExpressionIds.empty()) {
        if (defaultId) {
            auto result = CodegenFragment(builder, *defaultId);
            YT_ASSERT(result.GetStaticType() == resultType);
            return result;
        }
        return TCGValue::CreateNull(builder, resultType);
    }

    auto conditionId = whenThenExpressionIds.Front().first;
    auto resultId = whenThenExpressionIds.Front().second;
    auto condition = CodegenFragment(builder, conditionId);

    Value* canJump = nullptr;
    if (optionalOperand) {
        YT_ASSERT(condition.GetStaticType() == *optionalOperandType);

        condition.StoreToValue(builder, evaluatedCondition);
        auto optionalOperandAndWhenAreEqual = builder->CreateCall(
            eqComparer,
            {optionalOperand, evaluatedCondition});
        canJump = builder->CreateICmpEQ(
            optionalOperandAndWhenAreEqual,
            builder->getInt8(1),
            "canJump");
    } else {
        YT_ASSERT(condition.GetStaticType() == EValueType::Boolean);

        Value* predicateDataIsTrue = builder->CreateICmpEQ(
            condition.GetTypedData(builder),
            builder->getTrue());
        canJump = builder->CreateAnd(
            predicateDataIsTrue,
            builder->CreateNot(condition.GetIsNull(builder)),
            "canJump");
    }

    return CodegenIf<TCGExprContext, TCGValue>(
        builder,
        canJump,
        [&] (TCGExprContext& builder) {
            auto result = CodegenFragment(builder, resultId);
            YT_ASSERT(result.GetStaticType() == resultType);
            return result;
        },
        [&] (TCGExprContext& builder) {
            return MakeCodegenCaseExprRecursive(
                eqComparer,
                optionalOperand,
                optionalOperandType,
                evaluatedCondition,
                whenThenExpressionIds.Slice(1, whenThenExpressionIds.Size()),
                defaultId,
                resultType,
                builder);
        });
}

} // namespace NDetail

TCodegenExpression MakeCodegenCaseExpr(
    std::optional<size_t> optionalOperandId,
    std::optional<EValueType> optionalOperandType,
    std::vector<std::pair<size_t, size_t>> whenThenExpressionIds,
    std::optional<size_t> defaultId,
    EValueType resultType,
    TComparerManagerPtr comparerManager)
{
    return [
        =,
        whenThenExpressionIds = std::move(whenThenExpressionIds),
        comparerManager = std::move(comparerManager)
    ] (TCGExprContext& builder) {
        Function* eqComparer = nullptr;
        if (optionalOperandId) {
            eqComparer = comparerManager->GetEqComparer(
                std::vector{*optionalOperandType},
                builder.Module);
        }

        Value* optionalOperand = nullptr;
        if (optionalOperandId) {
            optionalOperand = builder->CreateAlignedAlloca(
                TTypeBuilder<TPIValue>::Get(builder->getContext()),
                8,
                builder->getInt64(1),
                "optionalOperand");
            CodegenFragment(builder, *optionalOperandId)
                .StoreToValue(builder, optionalOperand);
        }

        Value* evaluatedCondition = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "evaluatedCondition");

        return NDetail::MakeCodegenCaseExprRecursive(
            eqComparer,
            optionalOperand,
            optionalOperandType,
            evaluatedCondition,
            whenThenExpressionIds,
            defaultId,
            resultType,
            builder);
    };
}

TCodegenExpression MakeCodegenLikeExpr(
    size_t textId,
    EStringMatchOp opcode,
    size_t patternId,
    std::optional<size_t> escapeCharacterId,
    int contextIndex)
{
    return [
        =
    ] (TCGExprContext& builder) {
        auto text = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "text");
        CodegenFragment(builder, textId)
            .StoreToValue(builder, text);

        Value* matchOp = builder->getInt32(static_cast<int>(opcode));

        auto pattern = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "pattern");
        CodegenFragment(builder, patternId)
            .StoreToValue(builder, pattern);

        Value* useEscapeCharacter = builder->getInt8(escapeCharacterId.has_value());

        auto escapeCharacter = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "escapeCharacter");
        if (escapeCharacterId) {
            CodegenFragment(builder, *escapeCharacterId)
                .StoreToValue(builder, escapeCharacter);
        }

        auto result = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "result");

        builder->CreateCall(
            builder.Module->GetRoutine("LikeOpHelper"),
            {
                result,
                text,
                matchOp,
                pattern,
                useEscapeCharacter,
                escapeCharacter,
                builder.GetOpaqueValue(contextIndex),
            });

        return TCGValue::LoadFromRowValue(builder, result, EValueType::Boolean);
    };
}

TCodegenExpression MakeCodegenCompositeMemberAccessorExpr(
    size_t compositeId,
    int nestedStructOrTupleItemAccessorOpaqueIndex,
    std::optional<size_t> dictOrListItemAccessorId,
    EValueType resultType)
{
    return [=] (TCGExprContext& builder) {
        auto composite = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "composite");
        CodegenFragment(builder, compositeId)
            .StoreToValue(builder, composite);

        Value* hasDictOrListItemAccessor = builder->getInt8(dictOrListItemAccessorId.has_value());

        auto dictOrListItemAccessor = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "composite");
        if (dictOrListItemAccessorId) {
            CodegenFragment(builder, *dictOrListItemAccessorId)
                .StoreToValue(builder, dictOrListItemAccessor);
        }

        auto result = builder->CreateAlignedAlloca(
            TTypeBuilder<TPIValue>::Get(builder->getContext()),
            8,
            builder->getInt64(1),
            "result");

        builder->CreateCall(
            builder.Module->GetRoutine("CompositeMemberAccessorHelper"),
            {
                builder.Buffer,
                result,
                builder->getInt8(static_cast<ui8>(resultType)),
                composite,
                builder.GetOpaqueValue(nestedStructOrTupleItemAccessorOpaqueIndex),
                hasDictOrListItemAccessor,
                dictOrListItemAccessor,
            });

        return TCGValue::LoadFromRowValue(builder, result, resultType);
    };
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void CodegenEmptyOp(TCGOperatorContext& /*builder*/)
{ }

TLlvmClosure MakeConsumer(TCGOperatorContext& builder, llvm::Twine name, size_t consumerSlot)
{
    return MakeClosure<bool(TExpressionContext*, TPIValue**, i64)>(builder, name,
        [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* rows,
            Value* size
        ) {
            TCGContext innerBuilder(builder, buffer);
            Value* more = CodegenForEachRow(
                innerBuilder,
                rows,
                size,
                builder[consumerSlot]);

            Value* casted = innerBuilder->CreateIntCast(
                more,
                innerBuilder->getInt8Ty(),
                false);

            innerBuilder->CreateRet(casted);
        });
}

TLlvmClosure MakeConsumerWithPIConversion(
    TCGOperatorContext& builder,
    llvm::Twine name,
    size_t consumerSlot,
    const std::vector<int>& stringLikeColumnIndices)
{

    return MakeClosure<bool(TExpressionContext*, TValue**, i64)>(builder, name, [
            consumerSlot,
            stringLikeColumnIndices = std::move(stringLikeColumnIndices)
        ] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* rows,
            Value* size
        ) {
            TCGContext innerBuilder(builder, buffer);
            Value* more = CodegenForEachRow(
                innerBuilder,
                rows,
                size,
                builder[consumerSlot],
                stringLikeColumnIndices);

            Value* casted = innerBuilder->CreateIntCast(
                more,
                innerBuilder->getInt8Ty(),
                false);

            innerBuilder->CreateRet(casted);
        });
}

size_t MakeCodegenScanOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    const std::vector<int>& stringLikeColumnIndices,
    int rowSchemaInformationIndex,
    EExecutionBackend executionBackend)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        codegenSource = std::move(*codegenSource),
        stringLikeColumnIndices,
        rowSchemaInformationIndex,
        executionBackend
    ] (TCGOperatorContext& builder) {
        codegenSource(builder);

        auto consume = TLlvmClosure();
        if (executionBackend == EExecutionBackend::WebAssembly) {
            consume = MakeConsumer(
                builder,
                "ScanOpInner",
                consumerSlot);
        } else {
            consume = MakeConsumerWithPIConversion(
                builder,
                "ScanOpInner",
                consumerSlot,
                stringLikeColumnIndices);
        }

        builder->CreateCall(
            builder.Module->GetRoutine("ScanOpHelper"),
            {
                builder.GetExecutionContext(),
                consume.ClosurePtr,
                consume.Function,
                builder.GetOpaqueValue(rowSchemaInformationIndex),
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
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        parameters = std::move(parameters),
        primaryColumns = std::move(primaryColumns),
        comparerManager = std::move(comparerManager)
    ] (TCGOperatorContext& builder) {
        auto collectRows = MakeClosure<void(TMultiJoinClosure*, TExpressionContext*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* joinClosure,
            Value* /*buffer*/
        ) {
            Value* keyPtrs = builder->CreateAlloca(
                TTypeBuilder<TPIValue*>::Get(builder->getContext()),
                builder->getInt64(parameters.size()));

            Value* primaryValuesPtr = builder->CreateAlloca(TTypeBuilder<TPIValue*>::Get(builder->getContext()));

            builder->CreateStore(
                builder->CreateCall(
                    builder.Module->GetRoutine("AllocateJoinKeys"),
                    {
                        builder.GetExecutionContext(),
                        joinClosure,
                        keyPtrs
                    }),
                primaryValuesPtr);

            Type* closureType = TClosureTypeBuilder::Get(
                builder->getContext(),
                fragmentInfos->Functions.size());
            Value* expressionClosurePtr = builder->CreateAlloca(
                closureType,
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* joinClosureRef = builder->ViaClosure(joinClosure);
                Value* keyPtrsRef = builder->ViaClosure(keyPtrs);

                auto rowBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    values,
                    builder.Buffer,
                    builder->ViaClosure(expressionClosurePtr));

                for (size_t index = 0; index < parameters.size(); ++index) {
                    Type* type = TTypeBuilder<TValue*>::Get(builder->getContext());
                    Value* keyValuesPtr = builder->CreateConstGEP1_32(type, keyPtrsRef, index);
                    Value* keyValues = builder->CreateLoad(type, keyValuesPtr);

                    const auto& equations = parameters[index].Equations;
                    for (size_t column = 0; column < equations.size(); ++column) {
                        if (!equations[column].second) {
                            auto joinKeyValue = CodegenFragment(rowBuilder, equations[column].first);
                            joinKeyValue.StoreToValues(rowBuilder, keyValues, column);
                        }
                    }

                    TCGExprContext evaluatedColumnsBuilder(builder, TCGExprData{
                        *fragmentInfos,
                        rowBuilder.Buffer,
                        keyValues,
                        rowBuilder.ExpressionClosurePtr,
                    });

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
                Value* primaryValues = builder->CreateLoad(
                    TTypeBuilder<TValue*>::Get(builder->getContext()),
                    primaryValuesPtrRef);
                for (size_t column = 0; column < primaryColumns.size(); ++column) {
                    TCGValue::LoadFromRowValues(
                        builder,
                        values,
                        primaryColumns[column].first,
                        primaryColumns[column].second)
                        .StoreToValues(builder, primaryValues, column);
                }

                Value* finished = builder->CreateCall(
                    builder.Module->GetRoutine("StorePrimaryRow"),
                    {
                        builder.GetExecutionContext(),
                        joinClosureRef,
                        primaryValuesPtrRef,
                        keyPtrsRef
                    });

                return builder->CreateIsNotNull(finished);
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consumeJoinedRows = MakeConsumer(builder, "ConsumeJoinedRows", consumerSlot);

        const auto& module = builder.Module;

        Type* joinComparersType = TTypeBuilder<TJoinComparers>::Get(builder->getContext());

        Value* joinComparers = builder->CreateAlloca(
            joinComparersType,
            builder->getInt64(parameters.size()));

        using TFields = TTypeBuilder<TJoinComparers>::Fields;

        for (size_t index = 0; index < parameters.size(); ++index) {
            const auto& lookupKeyTypes = parameters[index].LookupKeyTypes;
            const auto& commonKeyPrefix = parameters[index].CommonKeyPrefix;
            const auto& foreignKeyPrefix = parameters[index].ForeignKeyPrefix;

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, commonKeyPrefix),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::PrefixEqComparer));

            builder->CreateStore(
                comparerManager->GetHasher(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::SuffixHasher));

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::SuffixEqComparer));

            builder->CreateStore(
                comparerManager->GetLessComparer(lookupKeyTypes, module, commonKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::SuffixLessComparer));

            builder->CreateStore(
                comparerManager->GetEqComparer(lookupKeyTypes, module, 0, foreignKeyPrefix),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::ForeignPrefixEqComparer));

            builder->CreateStore(
                comparerManager->GetLessComparer(lookupKeyTypes, module, foreignKeyPrefix, lookupKeyTypes.size()),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::ForeignSuffixLessComparer));

            builder->CreateStore(
                comparerManager->GetTernaryComparer(lookupKeyTypes, module),
                builder->CreateConstGEP2_32(joinComparersType, joinComparers, index, TFields::FullTernaryComparer));
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
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos)
    ] (TCGOperatorContext& builder) {
        Type* closureType = TClosureTypeBuilder::Get(
            builder->getContext(),
            fragmentInfos->Functions.size());
        Value* expressionClosurePtr = builder->CreateAlloca(
            closureType,
            nullptr,
            "expressionClosurePtr");

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            auto rowBuilder = TCGExprContext::Make(
                builder,
                *fragmentInfos,
                values,
                builder.Buffer,
                builder->ViaClosure(expressionClosurePtr));

            auto predicateResult = CodegenFragment(rowBuilder, predicateId);

            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            auto* notIsNull = builder->CreateNot(predicateResult.GetIsNull(builder));
            auto* isTrue = predicateResult.GetTypedData(builder);

            builder->CreateCondBr(
                builder->CreateAnd(notIsNull, isTrue),
                ifBB,
                endIfBB);

            auto* condBB = builder->GetInsertBlock();

            builder->SetInsertPoint(ifBB);
            Value* result = builder[consumerSlot](builder, values);
            ifBB = builder->GetInsertBlock();
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);

            return MakePhi(builder, ifBB, condBB, result, builder->getFalse(), "finishedPhi");
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

// Evaluate predicate over finalized row and call consumer over initial row.
size_t MakeCodegenFilterFinalizedOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    size_t predicateId,
    size_t keySize,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> stateTypes)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        codegenAggregates = std::move(codegenAggregates),
        stateTypes = std::move(stateTypes)
    ] (TCGOperatorContext& builder) {
        Type* closureType = TClosureTypeBuilder::Get(
            builder->getContext(),
            fragmentInfos->Functions.size());
        Value* expressionClosurePtr = builder->CreateAlloca(
            closureType,
            nullptr,
            "expressionClosurePtr");

        Value* finalizedValues = CodegenAllocateValues(builder, keySize + codegenAggregates.size());

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* finalizedValuesRef = builder->ViaClosure(finalizedValues);

            for (size_t index = 0; index < keySize; index++) {
                auto value = TCGValue::LoadFromRowValues(
                    builder,
                    values,
                    index,
                    stateTypes[index]);

                value.StoreToValues(builder, finalizedValuesRef, index);
            }

            for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                auto value = TCGValue::LoadFromRowValues(
                    builder,
                    values,
                    keySize + index,
                    stateTypes[index]);
                codegenAggregates[index].Finalize(builder, builder.Buffer, value)
                    .StoreToValues(builder, finalizedValuesRef, keySize + index);
            }

            auto rowBuilder = TCGExprContext::Make(
                builder,
                *fragmentInfos,
                finalizedValuesRef,
                builder.Buffer,
                builder->ViaClosure(expressionClosurePtr));

            auto predicateResult = CodegenFragment(rowBuilder, predicateId);

            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            auto* notIsNull = builder->CreateNot(predicateResult.GetIsNull(builder));
            auto* isTrue = predicateResult.GetTypedData(builder);

            builder->CreateCondBr(
                builder->CreateAnd(notIsNull, isTrue),
                ifBB,
                endIfBB);

            auto* condBB = builder->GetInsertBlock();

            builder->SetInsertPoint(ifBB);
            Value* result = builder[consumerSlot](builder, values);
            ifBB = builder->GetInsertBlock();
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);

            return MakePhi(builder, ifBB, condBB, result, builder->getFalse(), "finishedPhi");
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenAddStreamOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    size_t rowSize,
    EStreamTag value,
    const std::vector<EValueType>& stateTypes)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        Value* newValues = CodegenAllocateValues(builder, rowSize + 1);

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* newValuesRef = builder->ViaClosure(newValues);

            for (size_t index = 0; index < rowSize; index++) {
                auto value = TCGValue::LoadFromRowValues(
                    builder,
                    values,
                    index,
                    stateTypes[index]);

                value.StoreToValues(builder, newValuesRef, index);
            }

            TCGValue::Create(
                builder,
                builder->getFalse(),
                nullptr,
                builder->getInt64(static_cast<ui64>(value)),
                EValueType::Uint64,
                "streamIndex")
                .StoreToValues(builder, newValuesRef, rowSize);

            return builder[consumerSlot](builder, newValuesRef);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenArrayJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> arrayIds,
    int parametersIndex,
    size_t predicateId)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        arrayIds = std::move(arrayIds)
    ] (TCGOperatorContext& builder) {
        Type* closureType = TClosureTypeBuilder::Get(
            builder->getContext(),
            fragmentInfos->Functions.size());
        Value* expressionClosurePtr = builder->CreateAlloca(
            closureType,
            nullptr,
            "expressionClosurePtr");

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            auto predicate = MakeClosure<bool(TExpressionContext*, TPIValue*)>(builder, "arrayJoinPredicate", [&] (
                TCGOperatorContext& builder,
                Value* buffer,
                Value* unfoldedValues
            ) {
                TCGContext innerBuilder(builder, buffer);

                auto rowBuilder = TCGExprContext::Make(
                    innerBuilder,
                    *fragmentInfos,
                    unfoldedValues,
                    innerBuilder.Buffer,
                    innerBuilder->ViaClosure(expressionClosurePtr));

                auto predicateResult = CodegenFragment(rowBuilder, predicateId);

                auto* notIsNull = builder->CreateNot(predicateResult.GetIsNull(builder));
                auto* isTrue = predicateResult.GetTypedData(builder);

                Value* result = builder->CreateAnd(notIsNull, isTrue);
                Value* casted = innerBuilder->CreateIntCast(result, innerBuilder->getInt8Ty(), false);
                innerBuilder->CreateRet(casted);
            });

            int arrayCount = arrayIds.size();

            auto consumeArrayJoinedRows = MakeConsumer(builder, "ConsumeArrayJoinedRows", consumerSlot);

            Value* arrayValues = CodegenAllocateValues(builder, arrayCount);
            auto innerBuilder = TCGExprContext::Make(
                builder,
                *fragmentInfos,
                values,
                builder.Buffer,
                builder->ViaClosure(expressionClosurePtr));

            for (int index = 0; index < arrayCount; ++index) {
                CodegenFragment(innerBuilder, arrayIds[index])
                    .StoreToValues(innerBuilder, arrayValues, index);
            }

            return builder->CreateIsNotNull(
                builder->CreateCall(
                    builder.Module->GetRoutine("ArrayJoinOpHelper"),
                    {
                        builder.Buffer,
                        builder.GetOpaqueValue(parametersIndex),
                        values,
                        arrayValues,
                        consumeArrayJoinedRows.ClosurePtr,
                        consumeArrayJoinedRows.Function,
                        predicate.ClosurePtr,
                        predicate.Function,
                    }));
        };

        return codegenSource(builder);
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
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        argIds = std::move(argIds)
    ] (TCGOperatorContext& builder) {
        int projectionCount = argIds.size();

        Value* newValues = CodegenAllocateValues(builder, projectionCount);

        Type* closureType = TClosureTypeBuilder::Get(
            builder->getContext(),
            fragmentInfos->Functions.size());
        Value* expressionClosurePtr = builder->CreateAlloca(
            closureType,
            nullptr,
            "expressionClosurePtr");

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* newValuesRef = builder->ViaClosure(newValues);

            auto innerBuilder = TCGExprContext::Make(
                builder,
                *fragmentInfos,
                values,
                builder.Buffer,
                builder->ViaClosure(expressionClosurePtr));

            for (int index = 0; index < projectionCount; ++index) {
                CodegenFragment(innerBuilder, argIds[index])
                    .StoreToValues(innerBuilder, newValuesRef, index);
            }

            return builder[consumerSlot](builder, newValuesRef);
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

std::tuple<size_t, size_t> MakeCodegenDuplicateOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot)
{
    size_t firstSlot = (*slotCount)++;
    size_t secondSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* finish1 = builder[firstSlot](builder, values);
            Value* finish2 = builder[secondSlot](builder, values);
            return builder->CreateAnd(finish1, finish2);
        };

        codegenSource(builder);
    };

    return std::tuple(firstSlot, secondSlot);
}

size_t MakeCodegenMergeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t firstSlot,
    size_t secondSlot)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[firstSlot] = builder[consumerSlot];
        builder[secondSlot] = builder[consumerSlot];

        codegenSource(builder);
    };

    return consumerSlot;
}

size_t MakeCodegenOnceOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {

        using TBool = NCodegen::TTypes::i<1>;
        auto onceWrapper = MakeClosure<TBool(TExpressionContext*, TPIValue*)>(builder, "OnceWrapper", [&] (
            TCGOperatorContext& builder,
            Value* buffer,
            Value* values
        ) {
            TCGContext innerBuilder(builder, buffer);
            innerBuilder->CreateRet(builder[consumerSlot](innerBuilder, values));
        });

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            return builder->CreateCall(
                onceWrapper.Function,
                {
                    builder->ViaClosure(onceWrapper.ClosurePtr),
                    builder.Buffer,
                    values
                });
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

TGroupOpSlots MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> groupExprsIds,
    std::vector<std::vector<size_t>> aggregateExprIds,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes,
    bool allAggregatesFirst,
    bool isMerge,
    bool checkForNullGroupKey,
    size_t commonPrefixWithPrimaryKey,
    TComparerManagerPtr comparerManager)
{
    size_t intermediateSlot = (*slotCount)++;
    size_t finalSlot = (*slotCount)++;
    size_t deltaFinalSlot = (*slotCount)++;
    size_t totalsSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        groupExprsIds = std::move(groupExprsIds),
        aggregateExprIds = std::move(aggregateExprIds),
        codegenAggregates = std::move(codegenAggregates),
        keyTypes = std::move(keyTypes),
        stateTypes = std::move(stateTypes),
        comparerManager = std::move(comparerManager)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TGroupByClosure*, TExpressionContext*)>(builder, "GroupCollect", [&] (
            TCGOperatorContext& builder,
            Value* groupByClosure,
            Value* buffer
        ) {
            Value* newValuesPtr = builder->CreateAlloca(TTypeBuilder<TPIValue*>::Get(builder->getContext()));

            size_t keySize = keyTypes.size();
            size_t groupRowSize = keySize + stateTypes.size();

            builder->CreateCall(
                builder.Module->GetRoutine("AllocatePermanentRow"),
                {
                    builder.GetExecutionContext(),
                    buffer,
                    builder->getInt32(groupRowSize),
                    newValuesPtr
                });

            Type* closureType = TClosureTypeBuilder::Get(
                builder->getContext(),
                fragmentInfos->Functions.size());
            Value* expressionClosurePtr = builder->CreateAlloca(
                closureType,
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* bufferRef = builder->ViaClosure(buffer);
                Value* newValuesPtrRef = builder->ViaClosure(newValuesPtr);
                Value* newValuesRef = builder->CreateLoad(
                    TTypeBuilder<TValue*>::Get(builder->getContext()),
                    newValuesPtrRef);

                auto innerBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    values,
                    builder.Buffer,
                    builder->ViaClosure(expressionClosurePtr));

                Value* dstValues = newValuesRef;

                for (int index = 0; index < std::ssize(groupExprsIds); index++) {
                    CodegenFragment(innerBuilder, groupExprsIds[index])
                        .StoreToValues(builder, dstValues, index);
                }

                YT_VERIFY(commonPrefixWithPrimaryKey <= keySize);

                for (int index = std::ssize(groupExprsIds); index < static_cast<ssize_t>(keySize); ++index) {
                    TCGValue::CreateNull(builder, keyTypes[index])
                        .StoreToValues(builder, dstValues, index);
                }

                Value* groupByClosureRef = builder->ViaClosure(groupByClosure);

                bool shouldReadStreamTagFromRow = isMerge;
                Value* streamTag = nullptr;
                if (shouldReadStreamTagFromRow) {
                    size_t streamIndex = groupRowSize;
                    auto streamIndexValue = TCGValue::LoadFromRowValues(
                        builder,
                        values,
                        streamIndex,
                        EValueType::Uint64,
                        "reference.streamIndex");
                    streamTag = streamIndexValue.GetTypedData(builder);
                } else {
                    // We consider all incoming rows as intermediate for NodeThread and Non-Coordinated queries.
                    streamTag = builder->getInt64(static_cast<ui64>(EStreamTag::Intermediate));

                    // TODO(dtorilov): If query is disjoint, we can set Final tag here.
                }

                auto groupValues = builder->CreateCall(
                    builder.Module->GetRoutine("InsertGroupRow"),
                    {
                        builder.GetExecutionContext(),
                        groupByClosureRef,
                        newValuesRef,
                        streamTag,
                    });

                auto inserted = builder->CreateICmpEQ(
                    groupValues,
                    newValuesRef);

                CodegenIf<TCGContext>(builder, inserted, [&] (TCGContext& builder) {
                    for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                        codegenAggregates[index].Initialize(builder, bufferRef)
                            .StoreToValues(builder, groupValues, keySize + index);
                    }

                    builder->CreateCall(
                        builder.Module->GetRoutine("AllocatePermanentRow"),
                        {
                            builder.GetExecutionContext(),
                            bufferRef,
                            builder->getInt32(groupRowSize),
                            newValuesPtrRef
                        });
                });

                // Here *newRowPtrRef != groupRow.

                // Rows over limit are skipped
                auto notSkip = builder->CreateIsNotNull(groupValues);

                CodegenIf<TCGContext>(builder, notSkip, [&] (TCGContext& builder) {
                    for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                        auto aggState = TCGValue::LoadFromRowValues(
                            builder,
                            groupValues,
                            keySize + index,
                            stateTypes[index]);

                        if (isMerge) {
                            auto dstAggState = TCGValue::LoadFromRowValues(
                                builder,
                                innerBuilder.RowValues,
                                keySize + index,
                                stateTypes[index]);
                            auto mergeResult = (codegenAggregates[index].Merge)(builder, bufferRef, aggState, dstAggState);
                            mergeResult.StoreToValues(builder, groupValues, keySize + index);
                        } else {
                            std::vector<TCGValue> newValues;
                            for (size_t argId : aggregateExprIds[index]) {
                                newValues.emplace_back(CodegenFragment(innerBuilder, argId));
                            }
                            auto updateResult = (codegenAggregates[index].Update)(builder, bufferRef, aggState, newValues);
                            updateResult.StoreToValues(builder, groupValues, keySize + index);
                        }
                    }
                });

                return allAggregatesFirst ? builder->CreateIsNull(groupValues) : builder->getFalse();
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consumeIntermediate = MakeConsumer(builder, "ConsumeGroupedIntermediateRows", intermediateSlot);
        auto consumeFinal = TLlvmClosure();
        auto consumeDeltaFinal = MakeConsumer(builder, "ConsumeGroupedDeltaFinalRows", deltaFinalSlot);
        auto consumeTotals = TLlvmClosure();

        if (isMerge) {
            // Totals and final streams do not appear in inputs of NodeTread and Non-Coordinated queries.
            consumeFinal = MakeConsumer(builder, "ConsumeGroupedFinalRows", finalSlot);
            consumeTotals = MakeConsumer(builder, "ConsumeGroupedTotalsRows", totalsSlot);
        }

        builder->CreateCall(
            builder.Module->GetRoutine("GroupOpHelper"),
            {
                builder.GetExecutionContext(),

                comparerManager->GetEqComparer(keyTypes, builder.Module, 0, commonPrefixWithPrimaryKey),
                comparerManager->GetHasher(keyTypes, builder.Module, commonPrefixWithPrimaryKey, keyTypes.size()),
                comparerManager->GetEqComparer(keyTypes, builder.Module, commonPrefixWithPrimaryKey, keyTypes.size()),

                builder->getInt32(keyTypes.size()),
                builder->getInt32(stateTypes.size()),

                builder->getInt8(checkForNullGroupKey),
                builder->getInt8(allAggregatesFirst),

                collect.ClosurePtr,
                collect.Function,

                consumeIntermediate.ClosurePtr,
                consumeIntermediate.Function,

                isMerge ? consumeFinal.ClosurePtr : ConstantInt::getNullValue(builder->getPtrTy()),
                isMerge ? consumeFinal.Function : ConstantInt::getNullValue(builder->getPtrTy()),

                consumeDeltaFinal.ClosurePtr,
                consumeDeltaFinal.Function,

                isMerge ? consumeTotals.ClosurePtr : ConstantInt::getNullValue(builder->getPtrTy()),
                isMerge ? consumeTotals.Function : ConstantInt::getNullValue(builder->getPtrTy()),
            });
    };

    return {intermediateSlot, finalSlot, deltaFinalSlot, totalsSlot};
}

size_t MakeCodegenGroupTotalsOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        codegenAggregates = std::move(codegenAggregates),
        keyTypes = std::move(keyTypes),
        stateTypes = std::move(stateTypes)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TExpressionContext*)>(builder, "GroupTotalsCollect", [&] (
            TCGOperatorContext& builder,
            Value* buffer
        ) {
            Value* newValuesPtr = builder->CreateAlloca(TTypeBuilder<TPIValue*>::Get(builder->getContext()));

            auto keySize = std::ssize(keyTypes);
            auto groupRowSize = keySize + std::ssize(stateTypes);

            builder->CreateCall(
                builder.Module->GetRoutine("AllocatePermanentRow"),
                {
                    builder.GetExecutionContext(),
                    buffer,
                    builder->getInt32(groupRowSize),
                    newValuesPtr
                });

            Value* groupValues = builder->CreateLoad(TTypeBuilder<TValue*>::Get(builder->getContext()), newValuesPtr);

            for (int index = 0; index < keySize; ++index) {
                TCGValue::CreateNull(builder, keyTypes[index])
                    .StoreToValues(builder, groupValues, index);
            }

            for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                codegenAggregates[index].Initialize(builder, buffer)
                    .StoreToValues(builder, groupValues, keySize + index);
            }

            Value* hasRows = builder->CreateAlloca(builder->getInt1Ty());
            builder->CreateStore(builder->getFalse(), hasRows);

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* bufferRef = builder->ViaClosure(buffer);
                Value* groupValuesRef = builder->ViaClosure(groupValues);

                builder->CreateStore(
                    builder->getTrue(),
                    builder->ViaClosure(hasRows));

                for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                    auto aggState = TCGValue::LoadFromRowValues(
                        builder,
                        groupValuesRef,
                        keySize + index,
                        stateTypes[index]);

                    auto newValue = TCGValue::LoadFromRowValues(
                        builder,
                        values,
                        keySize + index,
                        stateTypes[index]);

                    codegenAggregates[index].Merge(builder, bufferRef, aggState, newValue)
                        .StoreToValues(builder, groupValuesRef, keySize + index);
                }

                return builder->getFalse();
            };

            codegenSource(builder);

            CodegenIf<TCGOperatorContext>(
                builder,
                builder->CreateLoad(builder->getInt1Ty(), hasRows),
                [&] (TCGOperatorContext& builder) {
                    TCGContext innerBuilder(builder, buffer);
                    builder[consumerSlot](innerBuilder, groupValues);
                });

            builder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("GroupTotalsOpHelper"),
            {
                builder.GetExecutionContext(),
                collect.ClosurePtr,
                collect.Function
            });
    };

    return consumerSlot;
}

size_t MakeCodegenFinalizeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    size_t keySize,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> stateTypes)
{
    // codegenFinalize calls the aggregates' finalize functions if needed

    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        codegenAggregates = std::move(codegenAggregates),
        stateTypes = std::move(stateTypes)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            for (int index = 0; index < std::ssize(codegenAggregates); index++) {
                auto value = TCGValue::LoadFromRowValues(
                    builder,
                    values,
                    keySize + index,
                    stateTypes[index]);

                codegenAggregates[index].Finalize(builder, builder.Buffer, value)
                    .StoreToValues(builder, values, keySize + index);
            }

            return builder[consumerSlot](builder, values);
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
    const std::vector<bool>& isDesc,
    TComparerManagerPtr comparerManager)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource),
        fragmentInfos = std::move(fragmentInfos),
        exprIds = std::move(exprIds),
        orderColumnTypes = std::move(orderColumnTypes),
        sourceSchema = std::move(sourceSchema),
        comparerManager = std::move(comparerManager)
    ] (TCGOperatorContext& builder) {
        auto schemaSize = sourceSchema.size();

        auto collectRows = MakeClosure<void(TTopCollector*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* topCollector
        ) {
            Value* newValues = CodegenAllocateValues(builder, schemaSize + exprIds.size());

            Type* closureType = TClosureTypeBuilder::Get(
                builder->getContext(),
                fragmentInfos->Functions.size());
            Value* expressionClosurePtr = builder->CreateAlloca(
                closureType,
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* topCollectorRef = builder->ViaClosure(topCollector);

                Value* newValuesRef = builder->ViaClosure(newValues);

                for (size_t index = 0; index < schemaSize; index++) {
                    auto value = TCGValue::LoadFromRowValues(
                        builder,
                        values,
                        index,
                        sourceSchema[index]);

                    value.StoreToValues(builder, newValuesRef, index);
                }

                auto innerBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    values,
                    builder.Buffer,
                    builder->ViaClosure(expressionClosurePtr));

                for (size_t index = 0; index < exprIds.size(); ++index) {
                    auto columnIndex = schemaSize + index;

                    CodegenFragment(innerBuilder, exprIds[index])
                        .StoreToValues(builder, newValuesRef, columnIndex);
                }

                builder->CreateCall(
                    builder.Module->GetRoutine("AddRowToCollector"),
                    {
                        topCollectorRef,
                        newValuesRef
                    });

                return builder->getFalse();
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consumeOrderedRows = MakeConsumer(builder, "ConsumeOrderedRows", consumerSlot);

        auto comparator = comparerManager->CodegenOrderByComparerFunction(
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
                builder->getInt64(schemaSize + exprIds.size())
            });
    };

    return consumerSlot;
}

size_t MakeCodegenOffsetLimiterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    size_t offsetId,
    size_t limitId)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {

        // index = 0
        Value* indexPtr = builder->CreateAlloca(builder->getInt64Ty(), nullptr, "indexPtr");
        builder->CreateStore(builder->getInt64(0), indexPtr);

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* indexPtrRef = builder->ViaClosure(indexPtr);

            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            Value* offsetRef = builder->CreatePointerCast(
                builder.GetOpaqueValue(offsetId),
                builder->getInt64Ty()->getPointerTo());
            Value* offset = builder->CreateLoad(builder->getInt64Ty(), offsetRef);

            Value* limitRef = builder->CreatePointerCast(
                builder.GetOpaqueValue(limitId),
                builder->getInt64Ty()->getPointerTo());
            Value* limit = builder->CreateLoad(builder->getInt64Ty(), limitRef);

            Value* end = builder->CreateAdd(offset, limit);

            Value* index = builder->CreateLoad(builder->getInt64Ty(), indexPtrRef, "index");

            // index = index + 1
            index = builder->CreateAdd(index, builder->getInt64(1));
            builder->CreateStore(index, indexPtrRef);

            Value* skip = builder->CreateOr(
                builder->CreateICmpULE(index, offset),
                builder->CreateICmpUGT(index, end));

            Value* finish = builder->CreateICmpUGE(index, end);

            builder->CreateCondBr(skip, endIfBB, ifBB);

            auto* condBB = builder->GetInsertBlock();

            builder->SetInsertPoint(ifBB);
            Value* result = builder[consumerSlot](builder, values);
            result = builder->CreateOr(result, finish);

            ifBB = builder->GetInsertBlock();
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);

            return MakePhi(builder, ifBB, condBB, result, finish, "finishedPhi");
        };

        codegenSource(builder);
    };

    return consumerSlot;
}

void MakeCodegenWriteOp(
    TCodegenSource* codegenSource,
    size_t producerSlot,
    size_t rowSize)
{
    *codegenSource = [
        =,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TWriteOpClosure*)>(builder, "WriteOpInner", [&] (
            TCGOperatorContext& builder,
            Value* writeRowClosure
        ) {
            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* writeRowClosureRef = builder->ViaClosure(writeRowClosure);

                Value* finished = builder->CreateCall(
                    builder.Module->GetRoutine("WriteRow"),
                    {builder.GetExecutionContext(), writeRowClosureRef, values});

                return builder->CreateIsNotNull(finished);
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        builder->CreateCall(
            builder.Module->GetRoutine("WriteOpHelper"),
            {
                builder.GetExecutionContext(),
                builder->getInt64(rowSize),
                collect.ClosurePtr,
                collect.Function
            });
    };
}

////////////////////////////////////////////////////////////////////////////////

template <typename TSignature, typename TPISignature>
TCallback<TSignature> BuildCGEntrypoint(TCGModulePtr module, const TString& entryFunctionName, EExecutionBackend executionBackend)
{
    if (executionBackend == EExecutionBackend::WebAssembly) {
        auto caller = New<TCGWebAssemblyCaller<TSignature, TPISignature>>(
    #ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            FROM_HERE,
    #endif
            module,
            entryFunctionName);

        auto* staticInvoke = &TCGWebAssemblyCaller<TSignature, TPISignature>::StaticInvoke;
        return TCallback<TSignature>(caller, staticInvoke);
    }

    auto piFunction = module->GetCompiledFunction<TPISignature>(entryFunctionName);
    auto caller = New<TCGPICaller<TSignature, TPISignature>>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        FROM_HERE,
#endif
        piFunction);

    auto* staticInvoke = &TCGPICaller<TSignature, TPISignature>::StaticInvoke;
    return TCallback<TSignature>(caller, staticInvoke);
}

std::unique_ptr<NWebAssembly::IWebAssemblyCompartment> BuildImage(const TCGModulePtr& module, EExecutionBackend executionBackend)
{
    if (executionBackend == EExecutionBackend::WebAssembly) {
        module->BuildWebAssembly();
        auto bytecode = module->GetWebAssemblyBytecode();
        auto compartment = NWebAssembly::CreateBaseImage();
        compartment->AddModule(bytecode);
        compartment->Strip();
        return compartment;
    }

    return {};
}

TCGQueryImage CodegenQuery(
    const TCodegenSource* codegenSource,
    size_t slotCount,
    EExecutionBackend executionBackend)
{
    auto cgModule = TCGModule::Create(GetQueryRoutineRegistry(executionBackend), executionBackend);
    const auto entryFunctionName = TString("EvaluateQuery");

    auto* queryFunction = MakeFunction<TCGPIQuerySignature>(cgModule, entryFunctionName.c_str(), [&] (
        TCGBaseContext& baseBuilder,
        Value* literals,
        Value* opaqueValuesPtr,
        Value* executionContextPtr
    ) {
        std::vector<std::shared_ptr<TCodegenConsumer>> consumers(slotCount);

        TCGOperatorContext builder(
            TCGOpaqueValuesContext(baseBuilder, literals, opaqueValuesPtr),
            executionContextPtr,
            &consumers);

        (*codegenSource)(builder);

        builder->CreateRetVoid();
    });

    cgModule->ExportSymbol(entryFunctionName);

    if (executionBackend == EExecutionBackend::WebAssembly) {
        queryFunction->addFnAttr("wasm-export-name", entryFunctionName.c_str());
    }

    return {
        BuildCGEntrypoint<TCGQuerySignature, TCGPIQuerySignature>(cgModule, entryFunctionName, executionBackend),
        BuildImage(cgModule, executionBackend),
    };
}

TCGExpressionImage CodegenStandaloneExpression(
    const TCodegenFragmentInfosPtr& fragmentInfos,
    size_t exprId,
    EExecutionBackend executionBackend)
{
    auto cgModule = TCGModule::Create(GetQueryRoutineRegistry(executionBackend), executionBackend);
    const auto entryFunctionName = TString("EvaluateExpression");

    CodegenFragmentBodies(cgModule, *fragmentInfos);

    auto* expressionFunction = MakeFunction<TCGPIExpressionSignature>(cgModule, entryFunctionName.c_str(), [&] (
        TCGBaseContext& baseBuilder,
        Value* literals,
        Value* opaqueValuesPtr,
        Value* resultPtr,
        Value* inputRow,
        Value* buffer
    ) {
        auto builder = TCGExprContext::Make(
            TCGOpaqueValuesContext(baseBuilder, literals, opaqueValuesPtr),
            *fragmentInfos,
            inputRow,
            buffer);

        CodegenFragment(builder, exprId)
            .StoreToValue(builder, resultPtr, "writeResult");
        builder->CreateRetVoid();
    });

    cgModule->ExportSymbol(entryFunctionName);

    if (executionBackend == EExecutionBackend::WebAssembly) {
        expressionFunction->addFnAttr("wasm-export-name", entryFunctionName.c_str());
    }

    return {
        BuildCGEntrypoint<TCGExpressionSignature, TCGPIExpressionSignature>(cgModule, entryFunctionName, executionBackend),
        BuildImage(cgModule, executionBackend),
    };
}

TCGAggregateImage CodegenAggregate(
    TCodegenAggregate codegenAggregate,
    std::vector<EValueType> argumentTypes,
    EValueType stateType,
    EExecutionBackend executionBackend)
{
    auto cgModule = TCGModule::Create(GetQueryRoutineRegistry(executionBackend), executionBackend);

    static const auto initName = TString("init");
    {
        auto* initFunction = MakeFunction<TCGPIAggregateInitSignature>(cgModule, initName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr
        ) {
            codegenAggregate.Initialize(builder, buffer)
                .StoreToValue(builder, resultPtr, "writeResult");
            builder->CreateRetVoid();
        });

        cgModule->ExportSymbol(initName);

        if (executionBackend == EExecutionBackend::WebAssembly) {
            initFunction->addFnAttr("wasm-export-name", initName.c_str());
        }
    }

    static const auto updateName = TString("update");
    {
        auto* updateFunction = MakeFunction<TCGPIAggregateUpdateSignature>(cgModule, updateName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* statePtr,
            Value* newValuesPtr
        ) {
            auto state = TCGValue::LoadFromAggregate(builder, statePtr, stateType);
            std::vector<TCGValue> newValues;
            newValues.reserve(argumentTypes.size());
            Type* type = TValueTypeBuilder::Get(builder->getContext());
            for (int index = 0; index < std::ssize(argumentTypes); ++index) {
                Value* newIthValuePtr = builder->CreateGEP(type, newValuesPtr, builder->getInt64(index));
                newValues.push_back(TCGValue::LoadFromAggregate(builder, newIthValuePtr, argumentTypes[index]));
            }

            codegenAggregate.Update(builder, buffer, state, std::move(newValues))
                .StoreToValue(builder, statePtr, "writeResult");
            builder->CreateRetVoid();
        });

        cgModule->ExportSymbol(updateName);

        if (executionBackend == EExecutionBackend::WebAssembly) {
            updateFunction->addFnAttr("wasm-export-name", updateName.c_str());
        }
    }

    static const auto mergeName = TString("merge");
    {
        auto* mergeFunction = MakeFunction<TCGPIAggregateMergeSignature>(cgModule, mergeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* dstStatePtr,
            Value* statePtr
        ) {
            auto state = TCGValue::LoadFromAggregate(builder, statePtr, stateType);
            auto dstState = TCGValue::LoadFromAggregate(builder, dstStatePtr, stateType);

            codegenAggregate.Merge(builder, buffer, dstState, state)
                .StoreToValue(builder, dstStatePtr, "writeResult");
            builder->CreateRetVoid();
        });

        cgModule->ExportSymbol(mergeName);

        if (executionBackend == EExecutionBackend::WebAssembly) {
            mergeFunction->addFnAttr("wasm-export-name", mergeName.c_str());
        }
    }

    static const auto finalizeName = TString("finalize");
    {
        auto* finalizeFunction = MakeFunction<TCGPIAggregateFinalizeSignature>(cgModule, finalizeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr
        ) {
            auto result = codegenAggregate.Finalize(
                builder,
                buffer,
                TCGValue::LoadFromAggregate(builder, statePtr, stateType));
            result.StoreToValue(builder, resultPtr, "writeResult");
            builder->CreateRetVoid();
        });

        cgModule->ExportSymbol(finalizeName);

        if (executionBackend == EExecutionBackend::WebAssembly) {
            finalizeFunction->addFnAttr("wasm-export-name", finalizeName.c_str());
        }
    }

    return {
        TCGAggregateCallbacks{
            BuildCGEntrypoint<TCGAggregateInitSignature, TCGPIAggregateInitSignature>(cgModule, initName, executionBackend),
            BuildCGEntrypoint<TCGAggregateUpdateSignature, TCGPIAggregateUpdateSignature>(cgModule, updateName, executionBackend),
            BuildCGEntrypoint<TCGAggregateMergeSignature, TCGPIAggregateMergeSignature>(cgModule, mergeName, executionBackend),
            BuildCGEntrypoint<TCGAggregateFinalizeSignature, TCGPIAggregateFinalizeSignature>(cgModule, finalizeName, executionBackend),
        },
        BuildImage(cgModule, executionBackend),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
