#include "cg_helpers.h"
#include "cg_fragment_compiler.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

StringRef ToStringRef(TStringBuf stringBuf)
{
    return StringRef(stringBuf.data(), stringBuf.length());
}

StringRef ToStringRef(TRef ref)
{
    return StringRef(ref.Begin(), ref.Size());
}

////////////////////////////////////////////////////////////////////////////////

TCGIRBuilderPtr::TCGIRBuilderPtr(TCGIRBuilder* builder)
    : Builder_(builder)
{ }

TCGIRBuilder* TCGIRBuilderPtr::operator->() const noexcept
{
    return Builder_;
}

TCGIRBuilder* TCGIRBuilderPtr::GetBuilder() const noexcept
{
    return Builder_;
}

////////////////////////////////////////////////////////////////////////////////

Type* GetABIType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType)
{
    return TDataTypeBuilder::Get(context, staticType);
}

Type* GetLLVMType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType)
{
    if (staticType == EValueType::Boolean) {
        return TTypeBuilder<NCodegen::TTypes::i<1>>::Get(context);
    }

    return GetABIType(context, staticType);
}

TCGValue::TCGValue(EValueType staticType, Value* isNull, Value* isAggregate, Value* length, Value* data, Twine name)
    : IsNull_(isNull)
    , IsAggregate_(isAggregate)
    , Length_(length)
    , Data_(data)
    , StaticType_(staticType)
    , Name_(name.str())
{
    YT_VERIFY(
        StaticType_ == EValueType::Int64 ||
        StaticType_ == EValueType::Uint64 ||
        StaticType_ == EValueType::Double ||
        StaticType_ == EValueType::Boolean ||
        StaticType_ == EValueType::String ||
        StaticType_ == EValueType::Any ||
        StaticType_ == EValueType::Composite);
}

TCGValue::TCGValue(TCGValue&& other) noexcept
    : IsNull_(other.IsNull_)
    , IsAggregate_(other.IsAggregate_)
    , Length_(other.Length_)
    , Data_(other.Data_)
    , StaticType_(other.StaticType_)
    , Name_(std::move(other.Name_))
{
    other.Reset();
}

TCGValue& TCGValue::operator=(TCGValue&& other)
{
    IsNull_ = other.IsNull_;
    IsAggregate_ = other.IsAggregate_;
    Length_ = other.Length_;
    Data_ = other.Data_;
    StaticType_ = other.StaticType_;

    other.Reset();

    return *this;
}

TCGValue&& TCGValue::Steal()
{
    return std::move(*this);
}

void TCGValue::Reset()
{
    IsNull_ = nullptr;
    IsAggregate_ = nullptr;
    Length_ = nullptr;
    Data_ = nullptr;
    StaticType_ = EValueType::TheBottom;
}

EValueType TCGValue::GetStaticType() const
{
    return StaticType_;
}

TCGValue TCGValue::Create(
    const TCGIRBuilderPtr& builder,
    Value* isNull,
    Value* isAggregate,
    Value* length,
    Value* data,
    EValueType staticType,
    Twine name)
{
    YT_VERIFY(
        isNull->getType() == builder->getInt1Ty() ||
        isNull->getType() == builder->getInt8Ty());
    if (IsStringLikeType(staticType)) {
        YT_VERIFY(length->getType() == TValueTypeBuilder::TLength::Get(builder->getContext()));
    }
    YT_VERIFY(
        data->getType() == GetLLVMType(builder->getContext(), staticType) ||
        data->getType() == TDataTypeBuilder::Get(builder->getContext()));
    return TCGValue(staticType, isNull, isAggregate, length, data, name);
}

TCGValue TCGValue::Create(
    const TCGIRBuilderPtr& builder,
    Value* isNull,
    Value* length,
    Value* data,
    EValueType staticType,
    Twine name)
{
    return Create(builder, isNull, nullptr, length, data, staticType, name);
}

TCGValue TCGValue::CreateNull(
    const TCGIRBuilderPtr& builder,
    EValueType staticType,
    Twine name)
{
    Value* length = nullptr;
    if (IsStringLikeType(staticType)) {
        length = llvm::UndefValue::get(TValueTypeBuilder::TLength::Get(builder->getContext()));
    }

    return Create(
        builder,
        builder->getTrue(),
        length,
        llvm::UndefValue::get(GetLLVMType(builder->getContext(), staticType)),
        staticType,
        name);
}

TCGValue TCGValue::LoadFromRowValues(
    const TCGIRBuilderPtr& builder,
    Value* rowValues,
    int index,
    bool nullable,
    bool aggregate,
    EValueType staticType,
    Twine name)
{
    Type* valueType = TValueTypeBuilder::Get(builder->getContext());

    Value* isNull = builder->getFalse();
    if (nullable) {
        Value* typePtr = builder->CreateConstInBoundsGEP2_32(
            valueType,
            rowValues,
            index,
            TValueTypeBuilder::Type,
            name + ".typePtr");

        isNull = builder->CreateLoad(
            TValueTypeBuilder::TType::Get(builder->getContext()),
            typePtr,
            name + ".type");
    }

    Value* isAggregate = nullptr;
    if (aggregate) {
        Value* aggregatePtr = builder->CreateConstInBoundsGEP2_32(
            valueType,
            rowValues,
            index,
            TValueTypeBuilder::Aggregate,
            name + ".aggregatePtr");

        isAggregate = builder->CreateLoad(
            TValueTypeBuilder::TAggregate::Get(builder->getContext()),
            aggregatePtr,
            name + ".aggregate");
    }

    Value* length = nullptr;
    if (IsStringLikeType(staticType)) {
        Value* lengthPtr = builder->CreateConstInBoundsGEP2_32(
            valueType,
            rowValues,
            index,
            TValueTypeBuilder::Length,
            name + ".lengthPtr");

        length = builder->CreateLoad(
            TValueTypeBuilder::TLength::Get(builder->getContext()),
            lengthPtr,
            name + ".length");
    }

    Value* dataPtr = builder->CreateConstInBoundsGEP2_32(
        valueType,
        rowValues,
        index,
        TValueTypeBuilder::Data,
        name + ".dataPtr");

    Value* data = builder->CreateLoad(
        TValueTypeBuilder::TData::Get(builder->getContext()),
        dataPtr,
        name + ".data");

    if (NTableClient::IsStringLikeType(staticType)) {
        Value* dataPtrAsInt = builder->CreatePtrToInt(dataPtr, data->getType());
        data = builder->CreateAdd(data, dataPtrAsInt, name + ".NonPIData");  // Similar to GetStringPosition.
    }

    return Create(builder, isNull, isAggregate, length, data, staticType, name);
}

TCGValue TCGValue::LoadFromRowValues(
    const TCGIRBuilderPtr& builder,
    Value* rowValues,
    int index,
    EValueType staticType,
    Twine name)
{
    return LoadFromRowValues(
        builder,
        rowValues,
        index,
        /*nullable*/ true,
        /*aggregate*/ false,
        staticType,
        name);
}

TCGValue TCGValue::LoadFromRowValue(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    bool nullable,
    EValueType staticType,
    Twine name)
{
    return LoadFromRowValues(builder, valuePtr, /*index*/ 0, nullable, /*aggregate*/ false, staticType, name);
}

TCGValue TCGValue::LoadFromRowValue(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    EValueType staticType,
    Twine name)
{
    return LoadFromRowValue(builder, valuePtr, /*nullable*/ true, staticType, name);
}

TCGValue TCGValue::LoadFromAggregate(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    EValueType staticType,
    Twine name)
{
    return LoadFromRowValues(
        builder,
        valuePtr,
        /*index*/ 0,
        /*nullable*/ true,
        /*aggregate*/ true,
        staticType,
        name);
}

void TCGValue::StoreToValues(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    size_t index,
    Twine name) const
{
    if (IsNull_->getType() == builder->getInt1Ty()) {
        Type* type = TValueTypeBuilder::TType::Get(builder->getContext());
        builder->CreateStore(
            builder->CreateSelect(
                GetIsNull(builder),
                ConstantInt::get(type, static_cast<int>(EValueType::Null)),
                ConstantInt::get(type, static_cast<int>(StaticType_))),
            builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                valuePtr,
                index,
                TValueTypeBuilder::Type,
                name + ".typePtr"));
    } else {
        builder->CreateStore(
            IsNull_,
            builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                valuePtr,
                index,
                TValueTypeBuilder::Type,
                name + ".typePtr"));
    }

    if (IsAggregate_) {
        builder->CreateStore(
            IsAggregate_,
            builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                valuePtr,
                index,
                TValueTypeBuilder::Aggregate,
                name + ".aggregatePtr"));
    }

    if (IsStringLikeType(StaticType_)) {
        builder->CreateStore(
            Length_,
            builder->CreateConstInBoundsGEP2_32(
                TValueTypeBuilder::Get(builder->getContext()),
                valuePtr,
                index,
                TValueTypeBuilder::Length,
                name + ".lengthPtr"));
    }

    Value* data = nullptr;
    auto targetType = TDataTypeBuilder::Get(builder->getContext());

    Value* dataPtr = builder->CreateConstInBoundsGEP2_32(
        TValueTypeBuilder::Get(builder->getContext()),
        valuePtr,
        index,
        TValueTypeBuilder::Data,
        name + ".dataPtr");

    if (Data_->getType()->isPointerTy()) {
        data = builder->CreatePtrToInt(Data_, targetType);
    } else if (Data_->getType()->isFloatingPointTy()) {
        data = builder->CreateBitCast(Data_, targetType);
    } else {
        data = builder->CreateIntCast(Data_, targetType, /*isSigned*/ false);
    }

    if (IsStringLikeType(StaticType_)) {
        Value* dataPtrAsInt = builder->CreatePtrToInt(dataPtr, data->getType());
        Value* stringPointer = builder->CreatePtrToInt(Data_, targetType);
        data = builder->CreateSub(stringPointer, dataPtrAsInt, name + ".PIdata");  // Similar to SetStringPosition.
    }

    builder->CreateStore(
        data,
        dataPtr);
}

void TCGValue::StoreToValues(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    size_t index) const
{
    StoreToValues(builder, valuePtr, index, Name_);
}

void TCGValue::StoreToValue(
    const TCGIRBuilderPtr& builder,
    Value* valuePtr,
    Twine name) const
{
    StoreToValues(builder, valuePtr, 0, name);
}

Value* TCGValue::IsNull() const
{
    return IsNull_;
}

Value* TCGValue::GetIsNull(const TCGIRBuilderPtr& builder) const
{
    if (IsNull_->getType() == builder->getInt1Ty()) {
        return IsNull_;
    }
    return builder->CreateICmpEQ(
        IsNull_,
        ConstantInt::get(IsNull_->getType(), static_cast<int>(EValueType::Null)));
}

Value* TCGValue::GetLength() const
{
    return Length_;
}

Value* TCGValue::GetData() const
{
    return Data_;
}

Value* TCGValue::GetTypedData(const TCGIRBuilderPtr& builder, bool isAbi) const
{
    Value* castedData = nullptr;
    Type* targetType = (isAbi ? GetABIType : GetLLVMType)(builder->getContext(), StaticType_);

    if (targetType->isPointerTy()) {
        castedData = builder->CreateIntToPtr(Data_,
            targetType);
    } else if (targetType->isFloatingPointTy()) {
        castedData = builder->CreateBitCast(Data_,
            targetType);
    } else {
        castedData = builder->CreateIntCast(Data_,
            targetType,
            false);
    }

    return castedData;
}

void CodegenThrowOnNanOrOutOfRange(TCGBaseContext& builder, Value* value, double lowerBound, double upperBound)
{
    CodegenIf<TCGBaseContext>(
        builder,
        builder->CreateFCmpUNO(value, value),
        [&] (TCGBaseContext& builder) {
            builder->CreateCall(
                builder.Module->GetRoutine("ThrowQueryException"),
                {
                    builder->CreateGlobalStringPtr("NaN is not convertible to integer")
                });
        });

    auto* isGreaterThanMax = builder->CreateFCmpUGT(
        value,
        ConstantFP::get(builder->getDoubleTy(), upperBound));
    auto* isLesserThanMin = builder->CreateFCmpULT(
        value,
        ConstantFP::get(builder->getDoubleTy(), lowerBound));
    CodegenIf<TCGBaseContext>(
        builder,
        builder->CreateOr(isGreaterThanMax, isLesserThanMin),
        [&] (TCGBaseContext& builder) {
            builder->CreateCall(
                builder.Module->GetRoutine("ThrowQueryException"),
                {
                    builder->CreateGlobalStringPtr("Floating point value out of integer range")
                });
        });
}

TCGValue TCGValue::Cast(TCGBaseContext& builder, EValueType destination) const
{
    if (destination == StaticType_) {
        return *this;
    }

    auto value = GetTypedData(builder);

    Value* result;
    switch (destination) {
        case EValueType::Int64: {
            auto destType = TDataTypeBuilder::TInt64::Get(builder->getContext());
            if (StaticType_ == EValueType::Uint64 || StaticType_ == EValueType::Boolean) {
                result = builder->CreateIntCast(value, destType, /*isSigned*/ false);
            } else if (StaticType_ == EValueType::Double) {
                constexpr auto LowerBound = std::bit_cast<double>(0xc3e0000000000000);
                static_assert(static_cast<i64>(LowerBound) == std::numeric_limits<i64>::min());
                constexpr auto UpperBound = std::bit_cast<double>(0x43dfffffffffffff);
                // i64's upper bound is not a perfect power of 2, thus a loss of precision.
                static_assert(static_cast<i64>(UpperBound) == 0x7ffffffffffffc00);
                CodegenThrowOnNanOrOutOfRange(builder, value, LowerBound, UpperBound);

                result = builder->CreateFPToSI(value, destType);
            } else {
                YT_ABORT();
            }
            break;
        }
        case EValueType::Uint64: {
            // signed/unsigned are equal to llvm
            auto destType = TDataTypeBuilder::TInt64::Get(builder->getContext());
            if (StaticType_ == EValueType::Int64 || StaticType_ == EValueType::Boolean) {
                result = builder->CreateIntCast(value, destType, /*isSigned*/ true);
            } else if (StaticType_ == EValueType::Double) {
                constexpr auto LowerBound = 0.0;
                constexpr auto UpperBound = std::bit_cast<double>(0x43efffffffffffff);
                // ui64's upper bound is not a perfect power of 2, thus a loss of precision.
                static_assert(static_cast<ui64>(UpperBound) == 0xfffffffffffff800);
                CodegenThrowOnNanOrOutOfRange(builder, value, LowerBound, UpperBound);

                result = builder->CreateFPToUI(value, destType);
            } else {
                YT_ABORT();
            }
            break;
        }
        case EValueType::Double: {
            auto destType = TDataTypeBuilder::TDouble::Get(builder->getContext());
            if (StaticType_ == EValueType::Uint64) {
                result = builder->CreateUIToFP(value, destType);
            } else if (StaticType_ == EValueType::Int64) {
                result = builder->CreateSIToFP(value, destType);
            } else {
                YT_ABORT();
            }
            break;
        }
        case EValueType::Boolean: {
            if (StaticType_ == EValueType::Int64 || StaticType_ == EValueType::Uint64) {
                result = builder->CreateIsNotNull(value);
            } else {
                YT_ABORT();
            }
            break;
        }
        default:
            YT_ABORT();
    }

    return Create(
        builder,
        // type changed, so we have to get isNull explicitly
        GetIsNull(builder),
        IsStringLikeType(StaticType_)
            ? GetLength()
            : nullptr,
        result,
        destination);
}

////////////////////////////////////////////////////////////////////////////////

TCGBaseContext::TCGBaseContext(
    const TCGIRBuilderPtr& base,
    const TCGModulePtr& cgModule)
    : TCGIRBuilderPtr(base)
    , Module(cgModule)
{ }

TCGBaseContext::TCGBaseContext(
    const TCGIRBuilderPtr& base,
    const TCGBaseContext& other)
    : TCGIRBuilderPtr(base)
    , Module(other.Module)
{ }

TCGOpaqueValuesContext::TCGOpaqueValuesContext(
    const TCGBaseContext& base,
    Value* literals,
    Value* opaqueValues)
    : TCGBaseContext(base)
    , Literals_(literals)
    , OpaqueValues_(opaqueValues)
{ }

TCGOpaqueValuesContext::TCGOpaqueValuesContext(
    const TCGBaseContext& base,
    const TCGOpaqueValuesContext& other)
    : TCGBaseContext(base)
    , Literals_(other.Literals_)
    , OpaqueValues_(other.OpaqueValues_)

{ }

Value* TCGOpaqueValuesContext::GetLiterals() const
{
    return Builder_->ViaClosure(Literals_, "literals");
}

Value* TCGOpaqueValuesContext::GetOpaqueValues() const
{
    return Builder_->ViaClosure(OpaqueValues_, "opaqueValues");
}

Value* TCGOpaqueValuesContext::GetOpaqueValue(size_t index) const
{
    Value* opaqueValues = GetOpaqueValues();
    auto opaqueValuePtr = Builder_->CreateConstGEP1_32(
        Builder_->getPtrTy(),
        opaqueValues,
        index);
    return Builder_->CreateLoad(
        Builder_->getPtrTy(),
        opaqueValuePtr,
        "opaqueValues." + Twine(index));
}

////////////////////////////////////////////////////////////////////////////////

TCGExprData::TCGExprData(
    const TCodegenFragmentInfos& expressionFragments,
    Value* buffer,
    Value* rowValues,
    Value* expressionClosurePtr)
    : ExpressionFragments(expressionFragments)
    , Buffer(buffer)
    , RowValues(rowValues)
    , ExpressionClosurePtr(expressionClosurePtr)
{ }

////////////////////////////////////////////////////////////////////////////////

TCGExprContext::TCGExprContext(
    const TCGOpaqueValuesContext& base,
    TCGExprData exprData)
    : TCGOpaqueValuesContext(base)
    , TCGExprData(exprData)
{ }

TCGExprContext::TCGExprContext(
    const TCGOpaqueValuesContext& base,
    const TCGExprContext& other)
    : TCGOpaqueValuesContext(base)
    , TCGExprData(other)
{ }

Value* TCGExprContext::GetExpressionClosurePtr()
{
    return ExpressionClosurePtr;
}

Value* TCGExprContext::GetFragmentResult(size_t index) const
{
    return Builder_->CreateInBoundsGEP(
        TClosureTypeBuilder::Get(
            Builder_->getContext(),
            ExpressionFragments.Functions.size()),
        ExpressionClosurePtr,
        {
            Builder_->getInt32(0),
            Builder_->getInt32(TClosureTypeBuilder::Fields::FragmentResults),
            Builder_->getInt32(ExpressionFragments.Items[index].Index)
        },
        Twine("fragment#") + Twine(index));
}

Value* TCGExprContext::GetFragmentFlag(size_t index) const
{
    return Builder_->CreateInBoundsGEP(
        TClosureTypeBuilder::Get(
            Builder_->getContext(),
            ExpressionFragments.Functions.size()),
        ExpressionClosurePtr,
        {
            Builder_->getInt32(0),
            Builder_->getInt32(TClosureTypeBuilder::Fields::FragmentResults),
            Builder_->getInt32(ExpressionFragments.Items[index].Index),
            Builder_->getInt32(TValueTypeBuilder::Type)
        },
        Twine("flag#") + Twine(index));
}

TCGExprContext TCGExprContext::Make(
    const TCGBaseContext& builder,
    const TCodegenFragmentInfos& fragmentInfos,
    Value* expressionClosurePtr,
    Value* literals,
    Value* rowValues)
{
    Value* opaqueValuesPtr = builder->CreateStructGEP(
        TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
        expressionClosurePtr,
        TClosureTypeBuilder::Fields::OpaqueValues);

    Value* opaqueValues = builder->CreateLoad(
        TClosureTypeBuilder::TOpaqueValues::Get(builder->getContext()),
        opaqueValuesPtr,
        "opaqueValues");

    Value* bufferPtr = builder->CreateStructGEP(
        TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
        expressionClosurePtr,
        TClosureTypeBuilder::Fields::Buffer);

    Value* buffer = builder->CreateLoad(
        TClosureTypeBuilder::TBuffer::Get(builder->getContext()),
        bufferPtr,
        "buffer");

    return TCGExprContext(
        TCGOpaqueValuesContext(builder, literals, opaqueValues),
        TCGExprData(
            fragmentInfos,
            buffer,
            rowValues,
            expressionClosurePtr));
}

TCGExprContext TCGExprContext::Make(
    const TCGOpaqueValuesContext& builder,
    const TCodegenFragmentInfos& fragmentInfos,
    Value* rowValues,
    Value* buffer,
    Value* expressionClosurePtr)
{
    if (!expressionClosurePtr) {
        expressionClosurePtr = builder->CreateAlloca(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
            nullptr,
            "expressionClosurePtr");
    }

    builder->CreateStore(
        builder.GetOpaqueValues(),
        builder->CreateConstInBoundsGEP2_32(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
            expressionClosurePtr,
            0,
            TClosureTypeBuilder::Fields::OpaqueValues));

    builder->CreateStore(
        buffer,
        builder->CreateConstInBoundsGEP2_32(
            TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
            expressionClosurePtr,
            0,
            TClosureTypeBuilder::Fields::Buffer));

    builder->CreateMemSet(
        builder->CreatePointerCast(
            builder->CreateConstInBoundsGEP2_32(
                TClosureTypeBuilder::Get(builder->getContext(), fragmentInfos.Functions.size()),
                expressionClosurePtr,
                0,
                TClosureTypeBuilder::Fields::FragmentResults),
            builder->getPtrTy()),
        builder->getInt8(static_cast<int>(EValueType::TheBottom)),
        sizeof(TValue) * fragmentInfos.Functions.size(),
        llvm::Align(8));

    return TCGExprContext(
        builder,
        TCGExprData(
            fragmentInfos,
            buffer,
            rowValues,
            expressionClosurePtr));
}

////////////////////////////////////////////////////////////////////////////////

TCGOperatorContext::TCGOperatorContext(
    const TCGOpaqueValuesContext& base,
    Value* executionContext,
    std::vector<std::shared_ptr<TCodegenConsumer>>* consumers)
    : TCGOpaqueValuesContext(base)
    , ExecutionContext_(executionContext)
    , Consumers_(consumers)
{ }

TCGOperatorContext::TCGOperatorContext(
    const TCGOpaqueValuesContext& base,
    const TCGOperatorContext& other)
    : TCGOpaqueValuesContext(base)
    , ExecutionContext_(other.ExecutionContext_)
    , Consumers_(other.Consumers_)
{ }

Value* TCGOperatorContext::GetExecutionContext() const
{
    return Builder_->ViaClosure(ExecutionContext_, "executionContext");
}

TCodegenConsumer& TCGOperatorContext::operator[] (size_t index) const
{
    if (!(*Consumers_)[index]) {
        (*Consumers_)[index] = std::make_shared<TCodegenConsumer>();
    }
    return *(*Consumers_)[index];
}

TCGContext::TCGContext(
    const TCGOperatorContext& base,
    Value* buffer)
    : TCGOperatorContext(base)
    , Buffer(buffer)
{ }

////////////////////////////////////////////////////////////////////////////////

TCGValue MakePhi(
    const TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name)
{
    BasicBlock* endBB = builder->GetInsertBlock();

    YT_VERIFY(thenValue.GetStaticType() == elseValue.GetStaticType());
    EValueType type = thenValue.GetStaticType();

    builder->SetInsertPoint(thenBB);
    Value* thenNull = thenValue.IsNull();
    Value* thenData = thenValue.GetData();

    builder->SetInsertPoint(elseBB);
    Value* elseNull = elseValue.IsNull();
    Value* elseData = elseValue.GetData();

    builder->SetInsertPoint(endBB);

    Value* phiNull = [&] () -> Value* {
        if (llvm::Constant* constantThenNull = llvm::dyn_cast<llvm::Constant>(thenNull)) {
            if (llvm::Constant* constantElseNull = llvm::dyn_cast<llvm::Constant>(elseNull)) {
                if (constantThenNull->isNullValue() && constantElseNull->isNullValue()) {
                    return builder->getFalse();
                }
            }
        }

        if (thenNull->getType() != elseNull->getType()) {
            builder->SetInsertPoint(thenBB);
            thenNull = thenValue.GetIsNull(builder);

            builder->SetInsertPoint(elseBB);
            elseNull = elseValue.GetIsNull(builder);

            builder->SetInsertPoint(endBB);
        }

        Type* targetType = thenNull->getType();

        PHINode* phiNull = builder->CreatePHI(targetType, 2, name + ".phiNull");
        phiNull->addIncoming(thenNull, thenBB);
        phiNull->addIncoming(elseNull, elseBB);
        return phiNull;
    }();

    if (thenData->getType() != elseData->getType()) {
        builder->SetInsertPoint(thenBB);
        thenData = thenValue.GetTypedData(builder);

        builder->SetInsertPoint(elseBB);
        elseData = elseValue.GetTypedData(builder);

        builder->SetInsertPoint(endBB);
    }

    YT_VERIFY(thenData->getType() == elseData->getType());

    PHINode* phiData = builder->CreatePHI(thenData->getType(), 2, name + ".phiData");
    phiData->addIncoming(thenData, thenBB);
    phiData->addIncoming(elseData, elseBB);

    PHINode* phiLength = nullptr;
    if (IsStringLikeType(type)) {
        builder->SetInsertPoint(thenBB);
        Value* thenLength = thenValue.GetLength();
        builder->SetInsertPoint(elseBB);
        Value* elseLength = elseValue.GetLength();

        builder->SetInsertPoint(endBB);

        YT_VERIFY(thenLength->getType() == elseLength->getType());

        phiLength = builder->CreatePHI(thenLength->getType(), 2, name + ".phiLength");
        phiLength->addIncoming(thenLength, thenBB);
        phiLength->addIncoming(elseLength, elseBB);
    }

    return TCGValue::Create(builder, phiNull, phiLength, phiData, type, name);
}

Value* MakePhi(
    const TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    Value* thenValue,
    Value* elseValue,
    Twine name)
{
    PHINode* phiValue = builder->CreatePHI(thenValue->getType(), 2, name + ".phiValue");
    phiValue->addIncoming(thenValue, thenBB);
    phiValue->addIncoming(elseValue, elseBB);
    return phiValue;
}

////////////////////////////////////////////////////////////////////////////////

llvm::Attribute BuildUnwindTableAttribute(llvm::LLVMContext& context)
{
    auto builder = llvm::AttrBuilder(context);
    builder.addUWTableAttr(llvm::UWTableKind::Default);
    return builder.getAttribute(llvm::Attribute::AttrKind::UWTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
