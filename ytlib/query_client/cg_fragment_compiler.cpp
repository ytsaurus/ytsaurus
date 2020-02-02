#include "cg_fragment_compiler.h"
#include "private.h"
#include "cg_ir_builder.h"
#include "cg_routines.h"
#include "llvm_folding_set.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/library/codegen/module.h>
#include <yt/library/codegen/public.h>

#include <yt/core/logging/log.h>

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

////////////////////////////////////////////////////////////////////////////////
// Operator helpers
//

Value* CodegenAllocateValues(TCGIRBuilderPtr& builder, size_t valueCount)
{
    Value* newValues = builder->CreateAlignedAlloca(
        TTypeBuilder<TValue>::Get(builder->getContext()),
        8,
        builder->getInt32(valueCount),
        "allocatedValues");

    return newValues;
}

Value* CodegenForEachRow(
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
    Value* finished = codegenConsumer(builder, row);
    builder->CreateStackRestore(stackState);
    loopBB = builder->GetInsertBlock();

    // index = index + 1
    builder->CreateStore(builder->CreateAdd(index, builder->getInt64(1)), indexPtr);
    builder->CreateCondBr(finished, endloopBB, condBB);

    builder->SetInsertPoint(endloopBB);

    return MakePhi(builder, loopBB, condBB, builder->getTrue(), builder->getFalse(),"finishedPhi");
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

        auto value = TCGValue::CreateFromLlvmValue(
            builder,
            builder->CreateInBoundsGEP(
                values,
                indexPhi),
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

    Function* UniversalComparer;
    TValueTypeLabels UniversalLables;

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
            UniversalComparer = MakeFunction<i64(char**, TValue*, TValue*, size_t, size_t)>(
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
                UniversalLables = CodegenLessComparerBody(
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

DEFINE_REFCOUNTED_TYPE(TComparerManager);

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
            Hasher = MakeFunction<ui64(char**, TValue*, size_t, size_t)>(module, "HasherImpl", [&] (
                TCGBaseContext& builder,
                Value* labelsArray,
                Value* values,
                Value* startIndex,
                Value* finishIndex
            ) {
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
                            GetLabelsArray(builder, types, UniversalLables),
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
                            GetLabelsArray(builder, types, UniversalLables),
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
                            GetLabelsArray(builder, types, UniversalLables),
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

    return MakeFunction<char(TValue*, TValue*)>(module, "OrderByComparer", [&] (
        TCGBaseContext& builder,
        Value* lhsValues,
        Value* rhsValues
    ) {
        lhsValues = builder->CreateGEP(lhsValues, builder->getInt64(offset));
        rhsValues = builder->CreateGEP(rhsValues, builder->getInt64(offset));

        std::vector<Constant*> isDescFlags;
        for (size_t index = 0; index < types.size(); ++index) {
            isDescFlags.push_back(builder->getInt8(index < isDesc.size() && isDesc[index]));
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
                    GetLabelsArray(builder, types, UniversalLables),
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
            builder->CreateLoad(builder->CreateGEP(isDescArray, {builder->getInt64(0), index})));

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
};

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
            return TCGValue::CreateFromRowValues(
                builder,
                builder.GetLiterals(),
                index,
                nullbale,
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
                builder.GetExpressionClosurePtr(),
                builder.GetLiterals(),
                builder.RowValues
            });

        return TCGValue::CreateFromLlvmValue(
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
                    TTypeBuilder<TValue*>::Get(module->GetModule()->getContext()),
                    TTypeBuilder<TValue*>::Get(module->GetModule()->getContext())
                },
                true);

            Function* function =  Function::Create(
                functionType,
                Function::ExternalLinkage,
                name.c_str(),
                module->GetModule());

            function->addFnAttr(llvm::Attribute::AttrKind::UWTable);
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
                    innerBuilder->CreateLoad(fragmentFlag),
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
            Value* operandData = operandValue.GetTypedData(builder);
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
                operandValue.GetIsNull(builder),
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

        return TCGValue::CreateFromValue(builder, isNull, nullptr, result, type);
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

        Value* lhsIsNull = lhsValue.GetIsNull(builder);
        Value* rhsIsNull = rhsValue.GetIsNull(builder);

        #define CMP_OP(opcode, optype) \
            case EBinaryOp::opcode: \
                evalData = builder->Create##optype(lhsData, rhsData); \
                break;

        auto compareNulls = [&] () {
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

            return evalData;
        };

        if (!IsStringLikeType(lhsValue.GetStaticType()) && !IsStringLikeType(rhsValue.GetStaticType())) {
            YT_VERIFY(lhsValue.GetStaticType() == rhsValue.GetStaticType());

            auto operandType = lhsValue.GetStaticType();

            Value* lhsData = lhsValue.GetTypedData(builder);
            Value* rhsData = rhsValue.GetTypedData(builder);
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
                            YT_ABORT();
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

            return TCGValue::CreateFromValue(
                builder,
                builder->getFalse(),
                nullptr,
                builder->CreateSelect(anyNull, compareNulls(), evalData),
                type);
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
                    evalData =  builder->CreateICmpSLE(cmpResult, builder->getInt32(0));
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

                        return cmpResultToResult(builder, cmpResult, resultOpcode);
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

                return evalData;
            };

            #undef CMP_OP

        auto result =
            builder.ExpressionFragments.Items[lhsId].Nullable ||
            builder.ExpressionFragments.Items[rhsId].Nullable
            ? CodegenIf<TCGBaseContext, Value*>(
                builder,
                builder->CreateOr(lhsIsNull, rhsIsNull),
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
            result,
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

                    CodegenIf<TCGBaseContext>(
                        builder,
                        builder->CreateAnd(
                            builder->CreateICmpEQ(lhsData, builder->getInt64(std::numeric_limits<i64>::min())),
                            builder->CreateICmpEQ(rhsData, builder->getInt64(-1))),
                        [] (TCGBaseContext& builder) {
                            builder->CreateCall(
                                builder.Module->GetRoutine("ThrowQueryException"),
                                {
                                    builder->CreateGlobalStringPtr("Division INT_MIN by -1")
                                });
                        });

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

                    return TCGValue::CreateFromValue(
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
                        OP(RightShift, LShr)
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
    int hashtableIndex,
    TComparerManagerPtr comparerManager)
{
    return [
        MOVE(argIds),
        MOVE(arrayIndex),
        MOVE(hashtableIndex),
        MOVE(comparerManager)
    ] (TCGExprContext& builder) {
        size_t keySize = argIds.size();

        Value* newValues = CodegenAllocateValues(builder, keySize);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            keyTypes.push_back(value.GetStaticType());
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

        return TCGValue::CreateFromValue(
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
        MOVE(argIds),
        MOVE(arrayIndex),
        MOVE(comparerManager)
    ] (TCGExprContext& builder) {
        size_t keySize = argIds.size();

        Value* newValues = CodegenAllocateValues(builder, keySize);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto value = CodegenFragment(builder, argIds[index]);
            keyTypes.push_back(value.GetStaticType());
            value.StoreToValues(builder, newValues, index);
        }

        Value* result = builder->CreateCall(
            builder.Module->GetRoutine("IsRowInRanges"),
            {
                builder->getInt32(keySize),
                newValues,
                builder.GetOpaqueValue(arrayIndex)
            });

        return TCGValue::CreateFromValue(
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
        MOVE(argIds),
        MOVE(defaultExprId),
        MOVE(arrayIndex),
        MOVE(hashtableIndex),
        resultType,
        MOVE(comparerManager)
    ] (TCGExprContext& builder) {
        size_t keySize = argIds.size();

        Value* newValues = CodegenAllocateValues(builder, keySize);

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
                return TCGValue::CreateFromRowValues(
                    builder,
                    result,
                    keyTypes.size(),
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

TLlvmClosure MakeConsumer(TCGOperatorContext& builder, llvm::Twine name, size_t consumerSlot)
{
    return MakeClosure<bool(TExpressionContext*, TValue**, i64)>(builder, name,
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

        auto consume = MakeConsumer(builder, "ScanOpInner", consumerSlot);

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

        Value* finalFinish = builder->CreateAlloca(builder->getInt1Ty());
        Value* intermediateFinish = builder->CreateAlloca(builder->getInt1Ty());
        Value* totalsFinish = builder->CreateAlloca(builder->getInt1Ty());

        builder->CreateStore(builder->getFalse(), finalFinish);
        builder->CreateStore(builder->getFalse(), intermediateFinish);
        builder->CreateStore(builder->getFalse(), totalsFinish);

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            auto* ifFinalBB = builder->CreateBBHere("ifFinal");
            auto* ifIntermediateBB = builder->CreateBBHere("ifIntermediate");
            auto* ifTotalsBB = builder->CreateBBHere("ifTotals");
            auto* endIfBB = builder->CreateBBHere("endIf");

            Value* finalFinishRef = builder->ViaClosure(finalFinish);
            Value* intermediateFinishRef = builder->ViaClosure(intermediateFinish);
            Value* totalsFinishRef = builder->ViaClosure(totalsFinish);

            auto streamIndexValue = TCGValue::CreateFromRowValues(
                builder,
                values,
                streamIndex,
                EValueType::Uint64,
                "reference.streamIndex");

            auto switcher = builder->CreateSwitch(streamIndexValue.GetTypedData(builder), endIfBB);

            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Final)), ifFinalBB);
            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Intermediate)), ifIntermediateBB);
            switcher->addCase(builder->getInt64(static_cast<ui64>(EStreamTag::Totals)), ifTotalsBB);

            builder->SetInsertPoint(ifFinalBB);
            builder->CreateStore(
                builder[finalConsumerSlot](builder, values),
                finalFinishRef);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(ifIntermediateBB);
            builder->CreateStore(
                builder[intermediateConsumerSlot](builder, values),
                intermediateFinishRef);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(ifTotalsBB);
            builder->CreateStore(
                builder[totalsConsumerSlot](builder, values),
                totalsFinishRef);
            builder->CreateBr(endIfBB);

            builder->SetInsertPoint(endIfBB);

            // FIXME(lukyan): This is logically wrong but fixes YT-11823
            return builder->CreateOr(
                builder->CreateLoad(finalFinishRef),
                builder->CreateOr(
                    builder->CreateLoad(intermediateFinishRef),
                    builder->CreateLoad(totalsFinishRef)));
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

        auto collectRows = MakeClosure<void(TJoinClosure*, TExpressionContext*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* joinClosure,
            Value* buffer
        ) {
            Value* keyPtr = builder->CreateAlloca(TTypeBuilder<TValue*>::Get(builder->getContext()));
            builder->CreateCall(
                builder.Module->GetRoutine("AllocatePermanentRow"),
                {
                    builder.GetExecutionContext(),
                    buffer,
                    builder->getInt32(lookupKeySize),
                    keyPtr
                });

            Value* expressionClosurePtr = builder->CreateAlloca(
                TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* keyPtrRef = builder->ViaClosure(keyPtr);
                Value* keyRef = builder->CreateLoad(keyPtrRef);

                auto rowBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    values,
                    builder.Buffer,
                    builder->ViaClosure(expressionClosurePtr));

                for (int column = 0; column < lookupKeySize; ++column) {
                    if (!equations[column].second) {
                        auto joinKeyValue = CodegenFragment(rowBuilder, equations[column].first);
                        lookupKeyTypes[column] = joinKeyValue.GetStaticType();
                        joinKeyValue.StoreToValues(rowBuilder, keyRef, column);
                    }
                }

                TCGExprContext evaluatedColumnsBuilder(builder, TCGExprData{
                    *fragmentInfos,
                    rowBuilder.Buffer,
                    keyRef,
                    rowBuilder.ExpressionClosurePtr});

                for (int column = 0; column < lookupKeySize; ++column) {
                    if (equations[column].second) {
                        auto evaluatedColumn = CodegenFragment(
                            evaluatedColumnsBuilder,
                            equations[column].first);
                        lookupKeyTypes[column] = evaluatedColumn.GetStaticType();
                        evaluatedColumn.StoreToValues(evaluatedColumnsBuilder, keyRef, column);
                    }
                }

                Value* joinClosureRef = builder->ViaClosure(joinClosure);

                builder->CreateCall(
                    builder.Module->GetRoutine("InsertJoinRow"),
                    {
                        builder.GetExecutionContext(),
                        joinClosureRef,
                        keyPtrRef,
                        values
                    });

                return builder->getFalse();
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto consumeJoinedRows = MakeConsumer(builder, "ConsumeJoinedRows", consumerSlot);

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
        auto collectRows = MakeClosure<void(TJoinClosure*, TExpressionContext*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* joinClosure,
            Value* buffer
        ) {
            Value* keyPtrs = builder->CreateAlloca(
                TTypeBuilder<TValue*>::Get(builder->getContext()),
                builder->getInt32(parameters.size()));

            Value* primaryValuesPtr = builder->CreateAlloca(TTypeBuilder<TValue*>::Get(builder->getContext()));

            builder->CreateStore(
                builder->CreateCall(
                    builder.Module->GetRoutine("AllocateJoinKeys"),
                    {
                        builder.GetExecutionContext(),
                        joinClosure,
                        keyPtrs
                    }),
                primaryValuesPtr);

            Value* expressionClosurePtr = builder->CreateAlloca(
                TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
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
                    Value* keyValues = builder->CreateLoad(builder->CreateConstGEP1_32(keyPtrsRef, index));

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
                for (size_t column = 0; column < primaryColumns.size(); ++column) {
                    TCGValue::CreateFromRowValues(
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

        Value* joinComparers = builder->CreateAlloca(
            TTypeBuilder<TJoinComparers>::Get(builder->getContext()),
            builder->getInt32(parameters.size()));

        typedef TTypeBuilder<TJoinComparers>::Fields TFields;

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
        Value* expressionClosurePtr = builder->CreateAlloca(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
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
        consumerSlot,
        producerSlot,
        MOVE(fragmentInfos),
        predicateId,
        keySize,
        MOVE(codegenAggregates),
        MOVE(stateTypes),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        Value* expressionClosurePtr = builder->CreateAlloca(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
            nullptr,
            "expressionClosurePtr");

        Value* finalizedValues = CodegenAllocateValues(builder, keySize + codegenAggregates.size());

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* finalizedValuesRef = builder->ViaClosure(finalizedValues);

            builder->CreateMemCpy(
                builder->CreatePointerCast(finalizedValuesRef, builder->getInt8PtrTy()),
                8,
                builder->CreatePointerCast(values, builder->getInt8PtrTy()),
                8,
                keySize * sizeof(TValue));

            for (int index = 0; index < codegenAggregates.size(); index++) {
                auto value = TCGValue::CreateFromRowValues(
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
    EStreamTag value)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        rowSize,
        value,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        Value* newValues = CodegenAllocateValues(builder, rowSize + 1);

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* newValuesRef = builder->ViaClosure(newValues);

            builder->CreateMemCpy(
                builder->CreatePointerCast(newValuesRef, builder->getInt8PtrTy()),
                8,
                builder->CreatePointerCast(values, builder->getInt8PtrTy()),
                8,
                rowSize * sizeof(TValue));

            TCGValue::CreateFromValue(
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

        Value* newValues = CodegenAllocateValues(builder, projectionCount);
        Value* expressionClosurePtr = builder->CreateAlloca(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
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
        firstSlot,
        secondSlot,
        producerSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* finish1 = builder[firstSlot](builder, values);
            Value* finish2 = builder[secondSlot](builder, values);
            return builder->CreateAnd(finish1, finish2);
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

size_t MakeCodegenOnceOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot)
{
    size_t consumerSlot = (*slotCount)++;

    *codegenSource = [
        consumerSlot,
        producerSlot,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {

        typedef NCodegen::TTypes::i<1> TBool;
        auto onceWrapper = MakeClosure<TBool(TExpressionContext*, TValue*)>(builder, "OnceWrapper", [&] (
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

std::pair<size_t, size_t> MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> groupExprsIds,
    std::vector<size_t> aggregateExprIds,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes,
    bool isMerge,
    bool checkNulls,
    size_t commonPrefixWithPrimaryKey,
    TComparerManagerPtr comparerManager)
{
    // codegenInitialize calls the aggregates' initialisation functions
    // codegenEvaluateGroups evaluates the group expressions
    // codegenEvaluateAggregateArgs evaluates the aggregates' arguments
    // codegenUpdate calls the aggregates' update or merge functions

    size_t boundaryConsumerSlot = (*slotCount)++;
    size_t innerConsumerSlot = (*slotCount)++;

    *codegenSource = [
        boundaryConsumerSlot,
        innerConsumerSlot,
        producerSlot,
        MOVE(fragmentInfos),
        MOVE(groupExprsIds),
        MOVE(aggregateExprIds),
        MOVE(codegenAggregates),
        codegenSource = std::move(*codegenSource),
        MOVE(keyTypes),
        MOVE(stateTypes),
        isMerge,
        checkNulls,
        commonPrefixWithPrimaryKey,
        MOVE(comparerManager)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TGroupByClosure*, TExpressionContext*)>(builder, "GroupCollect", [&] (
            TCGOperatorContext& builder,
            Value* groupByClosure,
            Value* buffer
        ) {
            Value* newValuesPtr = builder->CreateAlloca(TTypeBuilder<TValue*>::Get(builder->getContext()));

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

            Value* expressionClosurePtr = builder->CreateAlloca(
                TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* bufferRef = builder->ViaClosure(buffer);
                Value* newValuesPtrRef = builder->ViaClosure(newValuesPtr);
                Value* newValuesRef = builder->CreateLoad(newValuesPtrRef);

                auto innerBuilder = TCGExprContext::Make(
                    builder,
                    *fragmentInfos,
                    values,
                    builder.Buffer,
                    builder->ViaClosure(expressionClosurePtr));

                Value* dstValues = newValuesRef;

                for (int index = 0; index < groupExprsIds.size(); index++) {
                    CodegenFragment(innerBuilder, groupExprsIds[index])
                        .StoreToValues(builder, dstValues, index);
                }

                YT_VERIFY(commonPrefixWithPrimaryKey <= keySize);

                for (int index = groupExprsIds.size(); index < keySize; ++index) {
                    TCGValue::CreateNull(builder, keyTypes[index])
                        .StoreToValues(builder, dstValues, index);
                }

                Value* groupByClosureRef = builder->ViaClosure(groupByClosure);

                auto groupValues = builder->CreateCall(
                    builder.Module->GetRoutine("InsertGroupRow"),
                    {
                        builder.GetExecutionContext(),
                        groupByClosureRef,
                        newValuesRef
                    });

                auto inserted = builder->CreateICmpEQ(
                    groupValues,
                    newValuesRef);

                CodegenIf<TCGContext>(builder, inserted, [&] (TCGContext& builder) {
                    for (int index = 0; index < codegenAggregates.size(); index++) {
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
                    for (int index = 0; index < codegenAggregates.size(); index++) {
                        auto aggState = TCGValue::CreateFromRowValues(
                            builder,
                            groupValues,
                            keySize + index,
                            stateTypes[index]);

                        auto newValue = !isMerge
                            ? CodegenFragment(innerBuilder, aggregateExprIds[index])
                            : TCGValue::CreateFromRowValues(
                                builder,
                                innerBuilder.RowValues,
                                keySize + index,
                                stateTypes[index]);

                        TCodegenAggregateUpdate updateFunction;
                        if (isMerge) {
                            updateFunction = codegenAggregates[index].Merge;
                        } else {
                            updateFunction = codegenAggregates[index].Update;
                        }
                        updateFunction(builder, bufferRef, aggState, newValue)
                            .StoreToValues(builder, groupValues, keySize + index);
                    }
                });

                return stateTypes.empty() ? builder->CreateIsNull(groupValues) : builder->getFalse();
            };

            codegenSource(builder);

            builder->CreateRetVoid();
        });

        auto boundaryConsume = MakeConsumer(builder, "ConsumeGroupedRows", boundaryConsumerSlot);

        auto innerConsume = MakeConsumer(builder, "ConsumeGroupedRows", innerConsumerSlot);

        builder->CreateCall(
            builder.Module->GetRoutine("GroupOpHelper"),
            {
                builder.GetExecutionContext(),

                comparerManager->GetEqComparer(keyTypes, builder.Module, 0, commonPrefixWithPrimaryKey),
                comparerManager->GetHasher(keyTypes, builder.Module, commonPrefixWithPrimaryKey, keyTypes.size()),
                comparerManager->GetEqComparer(keyTypes, builder.Module, commonPrefixWithPrimaryKey, keyTypes.size()),
                builder->getInt32(keyTypes.size()),
                builder->getInt32(stateTypes.size()),
                builder->getInt8(checkNulls),

                collect.ClosurePtr,
                collect.Function,

                boundaryConsume.ClosurePtr,
                boundaryConsume.Function,

                innerConsume.ClosurePtr,
                innerConsume.Function,
            });

    };

    return std::make_pair(boundaryConsumerSlot, innerConsumerSlot);
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
        consumerSlot,
        producerSlot,
        MOVE(codegenAggregates),
        codegenSource = std::move(*codegenSource),
        MOVE(keyTypes),
        MOVE(stateTypes)
    ] (TCGOperatorContext& builder) {
        auto collect = MakeClosure<void(TExpressionContext*)>(builder, "GroupTotalsCollect", [&] (
            TCGOperatorContext& builder,
            Value* buffer
        ) {
            Value* newValuesPtr = builder->CreateAlloca(TTypeBuilder<TValue*>::Get(builder->getContext()));

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

            Value* groupValues = builder->CreateLoad(newValuesPtr);

            for (int index = 0; index < keySize; ++index) {
                TCGValue::CreateNull(builder, keyTypes[index])
                    .StoreToValues(builder, groupValues, index);
            }

            for (int index = 0; index < codegenAggregates.size(); index++) {
                codegenAggregates[index].Initialize(builder, buffer)
                    .StoreToValues(builder, groupValues, keySize + index);
            }

            Value* hasRows = builder->CreateAlloca(builder->getInt1Ty());
            builder->CreateStore(builder->getFalse(), hasRows);

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* bufferRef = builder->ViaClosure(buffer);
                Value* groupValuesRef = builder->ViaClosure(groupValues);

                builder->CreateStore(builder->getTrue(), builder->ViaClosure(hasRows));

                for (int index = 0; index < codegenAggregates.size(); index++) {
                    auto aggState = TCGValue::CreateFromRowValues(
                        builder,
                        groupValuesRef,
                        keySize + index,
                        stateTypes[index]);

                    auto newValue = TCGValue::CreateFromRowValues(
                        builder,
                        values,
                        keySize + index,
                        stateTypes[index]);

                    codegenAggregates[index].Merge(builder, bufferRef, aggState, newValue)
                        .StoreToValues(builder, groupValuesRef, keySize + index);
                }

                YT_VERIFY(!stateTypes.empty());

                return builder->getFalse();
            };

            codegenSource(builder);

            CodegenIf<TCGOperatorContext>(
                builder,
                builder->CreateLoad(hasRows),
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
        consumerSlot,
        producerSlot,
        keySize,
        MOVE(codegenAggregates),
        MOVE(stateTypes),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            for (int index = 0; index < codegenAggregates.size(); index++) {
                auto value = TCGValue::CreateFromRowValues(
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
        consumerSlot,
        producerSlot,
        isDesc,
        MOVE(fragmentInfos),
        MOVE(exprIds),
        MOVE(orderColumnTypes),
        MOVE(sourceSchema),
        MOVE(comparerManager),
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {
        auto schemaSize = sourceSchema.size();

        auto collectRows = MakeClosure<void(TTopCollector*)>(builder, "CollectRows", [&] (
            TCGOperatorContext& builder,
            Value* topCollector
        ) {
            Value* newValues = CodegenAllocateValues(builder, schemaSize + exprIds.size());

            Value* expressionClosurePtr = builder->CreateAlloca(
                TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos->Functions.size()),
                nullptr,
                "expressionClosurePtr");

            builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
                Value* topCollectorRef = builder->ViaClosure(topCollector);
                Value* newValuesRef = builder->ViaClosure(newValues);

                builder->CreateMemCpy(
                    builder->CreatePointerCast(newValuesRef, builder->getInt8PtrTy()),
                    8,
                    builder->CreatePointerCast(values, builder->getInt8PtrTy()),
                    8,
                    schemaSize * sizeof(TValue));

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
        consumerSlot,
        producerSlot,
        offsetId,
        limitId,
        codegenSource = std::move(*codegenSource)
    ] (TCGOperatorContext& builder) {

        // index = 0
        Value* indexPtr = builder->CreateAlloca(builder->getInt64Ty(), nullptr, "indexPtr");
        builder->CreateStore(builder->getInt64(0), indexPtr);

        builder[producerSlot] = [&] (TCGContext& builder, Value* values) {
            Value* indexPtrRef = builder->ViaClosure(indexPtr);

            auto* ifBB = builder->CreateBBHere("if");
            auto* endIfBB = builder->CreateBBHere("endIf");

            Value* offset = builder->CreateLoad(
                builder->CreatePointerCast(
                    builder.GetOpaqueValue(offsetId),
                    builder->getInt64Ty()->getPointerTo()));

            Value* limit = builder->CreateLoad(
                builder->CreatePointerCast(
                    builder.GetOpaqueValue(limitId),
                    builder->getInt64Ty()->getPointerTo()));

            Value* end = builder->CreateAdd(offset, limit);

            Value* index = builder->CreateLoad(indexPtrRef, "index");

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
        producerSlot,
        rowSize,
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

TCGQueryCallback CodegenEvaluate(
    const TCodegenSource* codegenSource,
    size_t slotCount)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    const auto entryFunctionName = TString("EvaluateQuery");

    MakeFunction<TCGQuerySignature>(module, entryFunctionName.c_str(), [&] (
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

    module->ExportSymbol(entryFunctionName);

    return module->GetCompiledFunction<TCGExpressionSignature>(entryFunctionName);
}

TCGAggregateCallbacks CodegenAggregate(
    TCodegenAggregate codegenAggregate,
    EValueType argumentType,
    EValueType stateType)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());

    static const auto initName = TString("init");
    {
        MakeFunction<TCGAggregateInitSignature>(module, initName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr
        ) {
            codegenAggregate.Initialize(builder, buffer)
                .StoreToValue(builder, resultPtr, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(initName);
    }

    static const auto updateName = TString("update");
    {
        MakeFunction<TCGAggregateUpdateSignature>(module, updateName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* statePtr,
            Value* newValuePtr
        ) {
            auto state = TCGValue::CreateFromLlvmValue(builder, statePtr, stateType);
            auto newValue = TCGValue::CreateFromLlvmValue(builder, newValuePtr, argumentType);
            codegenAggregate.Update(builder, buffer, state, newValue)
                .StoreToValue(builder, statePtr, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(updateName);
    }

    static const auto mergeName = TString("merge");
    {
        MakeFunction<TCGAggregateMergeSignature>(module, mergeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* dstStatePtr,
            Value* statePtr
        ) {
            auto state = TCGValue::CreateFromLlvmValue(builder, statePtr, stateType);
            auto dstState = TCGValue::CreateFromLlvmValue(builder, dstStatePtr, stateType);

            codegenAggregate.Merge(builder, buffer, dstState, state)
                .StoreToValue(builder, dstStatePtr, "writeResult");
            builder->CreateRetVoid();
        });

        module->ExportSymbol(mergeName);
    }

    static const auto finalizeName = TString("finalize");
    {
        MakeFunction<TCGAggregateFinalizeSignature>(module, finalizeName.c_str(), [&] (
            TCGBaseContext& builder,
            Value* buffer,
            Value* resultPtr,
            Value* statePtr
        ) {
            auto result = codegenAggregate.Finalize(
                builder,
                buffer,
                TCGValue::CreateFromLlvmValue(builder, statePtr, stateType));
            result.StoreToValue(builder, resultPtr, "writeResult");
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

} // namespace NYT::NQueryClient

