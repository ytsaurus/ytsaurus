#include "row_comparer_generator.h"
#include "dynamic_store_bits.h"
#include "sorted_dynamic_comparer.h"
#include "llvm_types.h"


#include <yt/ytlib/table_client/llvm_types.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/codegen/module.h>
#include <yt/core/codegen/llvm_migrate_helpers.h>
#include <yt/core/codegen/routine_registry.h>

#include <mutex>

#include <llvm/ADT/Twine.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/TypeBuilder.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;
using namespace NCodegen;
using namespace llvm;

////////////////////////////////////////////////////////////////////////////////

static void RegisterComparerRoutinesImpl(TRoutineRegistry* registry)
{
    registry->RegisterRoutine("memcmp", ::memcmp);
}

static TRoutineRegistry* GetComparerRoutineRegistry()
{
    static TRoutineRegistry registry;
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &RegisterComparerRoutinesImpl, &registry);
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////

class TComparerBuilder
    : public IRBuilder<>
{
public:
    TComparerBuilder(
        TCGModulePtr module,
        int keyColumnCount,
        const TTableSchema& schema);

    void BuildDDComparer(TString& functionName);
    void BuildDUComparer(TString& functionName);
    void BuildUUComparer(TString& functionName);

private:
    class IValueBuilder;
    class TValueBuilderBase;
    class TDynamicValueBuilder;
    class TUnversionedValueBuilder;
    friend class TDynamicValueBuilder;
    friend class TUnversionedValueBuilder;

    BasicBlock* CreateBB(const Twine& name = "");
    Value* CreateCmp(Value* lhs, Value* rhs, EValueType type, bool isLessThan);
    Value* CreateMin(Value* lhs, Value* rhs,  EValueType type);

    void BuildCmp(Value* lhs, Value* rhs, EValueType type);
    void BuildStringCmp(Value* lhsLength, Value* lhsData, Value* rhsLength, Value* rhsData);
    void BuildIterationLimitCheck(Value* length, int index);
    void BuildSentinelTypeCheck(Value* type);
    void BuildMainLoop(
        IValueBuilder& lhsBuilder,
        IValueBuilder& rhsBuilder,
        Value* length = nullptr);

    const int KeyColumnCount_;
    const TTableSchema& Schema_;
    const TCGModulePtr Module_;
    LLVMContext& Context_;

    // NB: temporary data which may change during the building process.
    BasicBlock* NextBB_;
    BasicBlock* LastBB_;
    Function* Function_;
};

class TComparerBuilder::IValueBuilder
{
public:
    virtual ~IValueBuilder() = default;
    virtual Value* GetType(int index) = 0;
    virtual Value* GetData(int index, EValueType type) = 0;
    virtual Value* GetStringLength(int index) = 0;
    virtual Value* GetStringData(int index) = 0;
};

class TComparerBuilder::TValueBuilderBase
    : public IValueBuilder
{
public:
    explicit TValueBuilderBase(TComparerBuilder& builder)
        : Builder_(builder)
    { }
    explicit TValueBuilderBase(TComparerBuilder& builder, Value* keyPtr)
        : Builder_(builder)
        , KeyPtr_(keyPtr)
    { }

protected:
    Value* GetElement(int index, Type* type)
    {
        return LoadElement(
            GetElementPtr(index),
            type);
    }

    Value* GetElement(int index, int indexInStruct, Type* type)
    {
        return LoadElement(
            GetElementPtr(index, indexInStruct),
            type);
    }

    Value* GetElement(int index)
    {
        return Builder_.CreateLoad(GetElementPtr(index));
    }

    Value* GetElement(int index, int indexInStruct)
    {
        return Builder_.CreateLoad(GetElementPtr(index, indexInStruct));
    }

    Value* GetElementPtr(int index)
    {
        return Builder_.CreateConstGEP1_32(KeyPtr_, index);
    }

    Value* GetElementPtr(int index, int indexInStruct)
    {
        return Builder_.CreateConstGEP2_32(nullptr, KeyPtr_, index, indexInStruct);
    }

    Value* LoadElement(Value* ptr, Type* type)
    {
        return Builder_.CreateLoad(
            Builder_.CreateBitCast(
                ptr,
                PointerType::getUnqual(type)));
    }

    TComparerBuilder& Builder_;

private:
    Value* KeyPtr_ = nullptr;
};

class TComparerBuilder::TDynamicValueBuilder
    : public TValueBuilderBase
{
public:
    TDynamicValueBuilder(TComparerBuilder& builder, Value* nullKeyMask, Value* keyPtr)
        : TValueBuilderBase(builder, keyPtr)
        , NullKeyMask_(nullKeyMask)
    {
        YCHECK(nullKeyMask->getType() == Type::getInt32Ty(Builder_.Context_));
        YCHECK((keyPtr->getType() == TypeBuilder<TDynamicValueData*, false>::get(Builder_.Context_)));
    }

    Value* GetType(int index) override
    {
        YCHECK(index < 32);
        auto* nullKeyBit = Builder_.CreateAnd(
            Builder_.getInt32(1U << index),
            NullKeyMask_);
        const auto& type = TypeBuilder<TUnversionedValue, false>::TType::get(Builder_.getContext());
        auto* nullType = ConstantInt::get(type, static_cast<int>(EValueType::Null));
        auto* schemaType = ConstantInt::get(type, static_cast<int>(Builder_.Schema_.Columns()[index].GetPhysicalType()));
        return Builder_.CreateSelect(
            Builder_.CreateICmpNE(nullKeyBit, Builder_.getInt32(0)),
            nullType,
            schemaType);
    }

    Value* GetData(int index, EValueType type) override
    {
        return GetElement(
            index,
            TypeBuilder<TDynamicValueData, false>::Fields::Any,
            GetDynamicValueDataType(type));
    }

    Value* GetStringData(int index) override
    {
        return Builder_.CreateConstGEP2_32(
            nullptr,
            GetStringPtr(index),
            0,
            TypeBuilder<TDynamicString, false>::Fields::Data);
    }

    Value* GetStringLength(int index) override
    {
        return Builder_.CreateLoad(
            Builder_.CreateConstGEP2_32(
                nullptr,
                GetStringPtr(index),
                0,
                TypeBuilder<TDynamicString, false>::Fields::Length));
    }

private:
    Value* GetStringPtr(int index)
    {
        return GetElement(
            index,
            TypeBuilder<TDynamicValueData, false>::Fields::String,
            GetDynamicValueDataType(EValueType::String));
    }

    Type* GetDynamicValueDataType(EValueType type)
    {
        switch(type) {
            case EValueType::Int64:
                return TypeBuilder<TDynamicValueData, false>::TInt64::get(Builder_.Context_);
            case EValueType::Uint64:
                return TypeBuilder<TDynamicValueData, false>::TUint64::get(Builder_.Context_);
            case EValueType::Boolean:
                return TypeBuilder<TDynamicValueData, false>::TBoolean::get(Builder_.Context_);
            case EValueType::Double:
                return TypeBuilder<TDynamicValueData, false>::TDouble::get(Builder_.Context_);
            case EValueType::String:
                return TypeBuilder<TDynamicValueData, false>::TStringType::get(Builder_.Context_);
            default:
                Y_UNREACHABLE();
        }
    }

    Value* NullKeyMask_ = nullptr;

    // NB: Here we assume that TDynamicValueData is a union.
    static_assert(
        std::is_union<NYT::NTabletNode::TDynamicValueData>::value,
        "TDynamicValueData must be a union");
};

class TComparerBuilder::TUnversionedValueBuilder
    : public TValueBuilderBase
{
public:
    TUnversionedValueBuilder(TComparerBuilder& builder, Value* keyPtr)
        : TValueBuilderBase(builder, keyPtr)
    {
        YCHECK((keyPtr->getType() == TypeBuilder<TUnversionedValue*, false>::get(Builder_.Context_)));
    }

    Value* GetType(int index) override
    {
        return GetElement(
            index,
            TypeBuilder<TUnversionedValue, false>::Fields::Type);
    }

    Value* GetData(int index, EValueType type) override
    {
        return GetData(index, GetUnversionedValueDataType(type));
    }

    Value* GetStringData(int index) override
    {
        return GetData(index, GetUnversionedValueDataType(EValueType::String));
    }

    Value* GetStringLength(int index) override
    {
        return GetElement(
            index,
            TypeBuilder<TUnversionedValue, false>::Fields::Length);
    }

private:
    Value* GetData(int index, Type* type)
    {
        return LoadElement(
            GetElementPtr(
                index,
                TypeBuilder<TUnversionedValue, false>::Fields::Data),
            type);
    }

    Type* GetUnversionedValueDataType(EValueType type)
    {
        switch(type) {
            case EValueType::Int64:
                return TypeBuilder<TUnversionedValueData, false>::TInt64::get(Builder_.Context_);
            case EValueType::Uint64:
                return TypeBuilder<TUnversionedValueData, false>::TUint64::get(Builder_.Context_);
            case EValueType::Boolean:
                return TypeBuilder<TUnversionedValueData, false>::TBoolean::get(Builder_.Context_);
            case EValueType::Double:
                return TypeBuilder<TUnversionedValueData, false>::TDouble::get(Builder_.Context_);
            case EValueType::String:
                return TypeBuilder<TUnversionedValueData, false>::TStringType::get(Builder_.Context_);
            default:
                Y_UNREACHABLE();
        }
    }

    // NB: Here we assume that TUnversionedValueData is a union.
    static_assert(
        std::is_union<TUnversionedValueData>::value,
        "TUnversionedValueData must be a union");
};

TComparerBuilder::TComparerBuilder(
    TCGModulePtr module,
    int keyColumnCount,
    const TTableSchema& schema)
    : IRBuilder(module->GetContext())
    , KeyColumnCount_(keyColumnCount)
    , Schema_(schema)
    , Module_(std::move(module))
    , Context_(Module_->GetContext())
{ }

void TComparerBuilder::BuildDDComparer(TString& functionName)
{
    Function_ = Function::Create(
        TypeBuilder<TDDComparerSignature, false>::get(Context_),
        Function::ExternalLinkage,
        functionName.c_str(),
        Module_->GetModule());
    SetInsertPoint(CreateBB("entry"));
    auto args = Function_->arg_begin();
    Value* lhsNullKeyMask = ConvertToPointer(args);
    Value* lhsKeys = ConvertToPointer(++args);
    Value* rhsNullKeyMask = ConvertToPointer(++args);
    Value* rhsKeys = ConvertToPointer(++args);
    YCHECK(++args == Function_->arg_end());
    auto lhsBuilder = TDynamicValueBuilder(*this, lhsNullKeyMask, lhsKeys);
    auto rhsBuilder = TDynamicValueBuilder(*this, rhsNullKeyMask, rhsKeys);
    BuildMainLoop(lhsBuilder, rhsBuilder);
    CreateRet(getInt32(0));
}

void TComparerBuilder::BuildDUComparer(TString& functionName)
{
    Function_ = Function::Create(
        TypeBuilder<TDUComparerSignature, false>::get(Context_),
        Function::ExternalLinkage,
        functionName.c_str(),
        Module_->GetModule());
    SetInsertPoint(CreateBB("entry"));
    auto args = Function_->arg_begin();
    Value* lhsNullKeyMask = ConvertToPointer(args);
    Value* lhsKeys = ConvertToPointer(++args);
    Value* rhsKeys = ConvertToPointer(++args);
    Value* length = ConvertToPointer(++args);
    YCHECK(++args == Function_->arg_end());
    auto lhsBuilder = TDynamicValueBuilder(*this, lhsNullKeyMask, lhsKeys);
    auto rhsBuilder = TUnversionedValueBuilder(*this, rhsKeys);
    BuildMainLoop(lhsBuilder, rhsBuilder, length);
    auto lengthDifference = CreateSub(getInt32(KeyColumnCount_), length);
    CreateRet(lengthDifference);
}

void TComparerBuilder::BuildUUComparer(TString& functionName)
{
    Function_ = Function::Create(
        TypeBuilder<TUUComparerSignature, false>::get(Context_),
        Function::ExternalLinkage,
        functionName.c_str(),
        Module_->GetModule());
    SetInsertPoint(CreateBB("entry"));
    auto args = Function_->arg_begin();
    Value* lhsKeys = ConvertToPointer(args);
    Value* lhsLength = ConvertToPointer(++args);
    Value* rhsKeys = ConvertToPointer(++args);
    Value* rhsLength = ConvertToPointer(++args);
    YCHECK(++args == Function_->arg_end());
    auto length = CreateMin(lhsLength, rhsLength, EValueType::Int64);
    auto lhsBuilder = TUnversionedValueBuilder(*this, lhsKeys);
    auto rhsBuilder = TUnversionedValueBuilder(*this, rhsKeys);
    BuildMainLoop(lhsBuilder, rhsBuilder, length);
    auto lengthDifference = CreateSub(lhsLength, rhsLength);
    CreateRet(lengthDifference);
}

BasicBlock* TComparerBuilder::CreateBB(const Twine& name)
{
    return BasicBlock::Create(Context_, name, Function_);
}

Value* TComparerBuilder::CreateCmp(Value* lhs, Value* rhs, EValueType type, bool isLessThan)
{
    switch(type) {
        case EValueType::Int64:
            return CreateICmp(isLessThan ? CmpInst::ICMP_SLT : CmpInst::ICMP_SGT, lhs, rhs);
        case EValueType::Uint64:
        case EValueType::Boolean:
            return CreateICmp(isLessThan ? CmpInst::ICMP_ULT : CmpInst::ICMP_UGT, lhs, rhs);
        case EValueType::Double:
            return CreateFCmp(isLessThan ? CmpInst::FCMP_ULT : CmpInst::FCMP_UGT, lhs, rhs);
        default:
            Y_UNREACHABLE();
    }
}

Value* TComparerBuilder::CreateMin(Value* lhs, Value* rhs, EValueType type)
{
    YCHECK(lhs->getType() == rhs->getType());
    return CreateSelect(CreateCmp(lhs, rhs, type, true), lhs, rhs);
}

void TComparerBuilder::BuildCmp(Value* lhs, Value* rhs, EValueType type)
{
    auto* trueBB = CreateBB("cmp.lower");
    auto* falseBB = CreateBB("cmp.not.lower");
    CreateCondBr(CreateCmp(lhs, rhs, type, true), trueBB, falseBB);
    SetInsertPoint(trueBB);
    CreateRet(getInt32(-1));
    SetInsertPoint(falseBB);

    trueBB = CreateBB("cmp.greater");
    falseBB = CreateBB("cmp.equal");
    CreateCondBr(CreateCmp(lhs, rhs, type, false), trueBB, falseBB);
    SetInsertPoint(trueBB);
    CreateRet(getInt32(1));
    SetInsertPoint(falseBB);
}

void TComparerBuilder::BuildStringCmp(Value* lhsLength, Value* lhsData, Value* rhsLength, Value* rhsData)
{
    auto* minLength = CreateZExt(
        CreateMin(lhsLength, rhsLength, EValueType::Int64),
        Type::getInt64Ty(Context_));
    auto* memcmpResult = CreateCall(
        Module_->GetRoutine("memcmp"),
        {
            lhsData,
            rhsData,
            minLength
        });
    auto* trueBB = CreateBB("memcmp.is.not.zero");
    auto* falseBB = CreateBB("memcmp.is.zero");
    CreateCondBr(CreateICmpNE(memcmpResult, getInt32(0)), trueBB, falseBB);
    SetInsertPoint(trueBB);
    CreateRet(memcmpResult);
    SetInsertPoint(falseBB);
    BuildCmp(lhsLength, rhsLength, EValueType::Int64);
}

void TComparerBuilder::BuildIterationLimitCheck(Value* iterationsLimit, int index)
{
    if (iterationsLimit != nullptr) {
        auto* falseBB = CreateBB("limit.check.false");
        CreateCondBr(
            CreateICmpEQ(
                iterationsLimit,
                ConstantInt::get(iterationsLimit->getType(), index)),
            LastBB_,
            falseBB);
        SetInsertPoint(falseBB);
    }
}

void TComparerBuilder::BuildSentinelTypeCheck(Value* type)
{
    auto* upperBB = CreateBB("type.is.greater.than.null");
    auto* lowerBB = CreateBB("type.is.less.than.max");
    CreateCondBr(
        CreateICmpULE(type, ConstantInt::get(type->getType(), static_cast<int>(EValueType::Null))),
        NextBB_,
        upperBB);
    SetInsertPoint(upperBB);
    CreateCondBr(
        CreateICmpUGE(type, ConstantInt::get(type->getType(), static_cast<int>(EValueType::Max))),
        NextBB_,
        lowerBB);
    SetInsertPoint(lowerBB);
}

void TComparerBuilder::BuildMainLoop(
    IValueBuilder& lhsBuilder,
    IValueBuilder& rhsBuilder,
    Value* iterationsLimit)
{
    LastBB_ = CreateBB("epilogue");
    NextBB_ = CreateBB("iteration");
    auto columnIt = Schema_.Columns().begin();
    for (int index = 0; index < KeyColumnCount_; ++index, ++columnIt) {
        BuildIterationLimitCheck(iterationsLimit, index);

        auto* lhsType = lhsBuilder.GetType(index);
        auto* rhsType = rhsBuilder.GetType(index);
        BuildCmp(lhsType, rhsType, EValueType::Uint64);
        BuildSentinelTypeCheck(lhsType);

        auto type = columnIt->GetPhysicalType();
        if (type == EValueType::String) {
            auto* lhsLength = lhsBuilder.GetStringLength(index);
            auto* rhsLength = rhsBuilder.GetStringLength(index);
            auto* lhsData = lhsBuilder.GetStringData(index);
            auto* rhsData = rhsBuilder.GetStringData(index);
            BuildStringCmp(lhsLength, lhsData, rhsLength, rhsData);
        } else {
            auto* lhs = lhsBuilder.GetData(index, type);
            auto* rhs = rhsBuilder.GetData(index, type);
            BuildCmp(lhs, rhs, type);
        }
        CreateBr(NextBB_);
        SetInsertPoint(NextBB_);
        NextBB_ = CreateBB("iteration");
    }
    NextBB_->eraseFromParent();
    CreateBr(LastBB_);
    SetInsertPoint(LastBB_);
}

////////////////////////////////////////////////////////////////////////////////

std::tuple<
    NCodegen::TCGFunction<TDDComparerSignature>,
    NCodegen::TCGFunction<TDUComparerSignature>,
    NCodegen::TCGFunction<TUUComparerSignature>>
GenerateComparers(int keyColumnCount, const TTableSchema& schema)
{
    auto module = TCGModule::Create(GetComparerRoutineRegistry());
    auto builder = TComparerBuilder(module, keyColumnCount, schema);
    auto ddComparerName = TString("DDCompare");
    auto duComparerName = TString("DUCompare");
    auto uuComparerName = TString("UUCompare");

    builder.BuildDDComparer(ddComparerName);
    builder.BuildDUComparer(duComparerName);
    builder.BuildUUComparer(uuComparerName);

    module->ExportSymbol(ddComparerName);
    module->ExportSymbol(duComparerName);
    module->ExportSymbol(uuComparerName);

    auto ddComparer = module->GetCompiledFunction<TDDComparerSignature>(ddComparerName);
    auto duComparer = module->GetCompiledFunction<TDUComparerSignature>(duComparerName);
    auto uuComparer = module->GetCompiledFunction<TUUComparerSignature>(uuComparerName);

    return std::tie(ddComparer, duComparer, uuComparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
