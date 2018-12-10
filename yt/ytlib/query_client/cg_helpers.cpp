#include "cg_helpers.h"
#include "cg_fragment_compiler.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

StringRef ToStringRef(TStringBuf stringBuf)
{
    return StringRef(stringBuf.c_str(), stringBuf.length());
}

StringRef ToStringRef(TRef ref)
{
    return StringRef(ref.Begin(), ref.Size());
}

////////////////////////////////////////////////////////////////////////////////

Type* GetABIType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType)
{
    return TDataTypeBuilder::get(context, staticType);
}

Type* GetLLVMType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType)
{
    if (staticType == EValueType::Boolean) {
        return TypeBuilder<llvm::types::i<1>, false>::get(context);
    }

    return GetABIType(context, staticType);
}

////////////////////////////////////////////////////////////////////////////////

Value* TCGExprContext::GetFragmentResult(size_t index) const
{
    return Builder_->CreateInBoundsGEP(
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
        ExpressionClosurePtr,
        {
            Builder_->getInt32(0),
            Builder_->getInt32(TClosureTypeBuilder::Fields::FragmentResults),
            Builder_->getInt32(ExpressionFragments.Items[index].Index),
            Builder_->getInt32(TTypeBuilder::Type)
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
    Value* opaqueValues = builder->CreateLoad(builder->CreateStructGEP(
        nullptr,
        expressionClosurePtr,
        TClosureTypeBuilder::Fields::OpaqueValues),
        "opaqueValues");

    return TCGExprContext(
        TCGOpaqueValuesContext(builder, literals, opaqueValues),
        TCGExprData(
            fragmentInfos,
            builder->CreateLoad(builder->CreateStructGEP(
                nullptr,
                expressionClosurePtr,
                TClosureTypeBuilder::Fields::Buffer),
                "buffer"),
            rowValues,
            expressionClosurePtr
        ));
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
            TClosureTypeBuilder::get(builder->getContext(), fragmentInfos.Functions.size()),
            nullptr,
            "expressionClosurePtr");
    }

    builder->CreateStore(
        builder.GetOpaqueValues(),
        builder->CreateConstInBoundsGEP2_32(
            nullptr,
            expressionClosurePtr,
            0,
            TClosureTypeBuilder::Fields::OpaqueValues));

    builder->CreateStore(
        buffer,
        builder->CreateConstInBoundsGEP2_32(
            nullptr,
            expressionClosurePtr,
            0,
            TClosureTypeBuilder::Fields::Buffer));

    builder->CreateMemSet(
        builder->CreatePointerCast(
            builder->CreateConstInBoundsGEP2_32(
                nullptr,
                expressionClosurePtr,
                0,
                TClosureTypeBuilder::Fields::FragmentResults),
            builder->getInt8PtrTy()),
        builder->getInt8(static_cast<int>(EValueType::TheBottom)),
        sizeof(TValue) * fragmentInfos.Functions.size(),
        8);

    return TCGExprContext(
        builder,
        TCGExprData(
            fragmentInfos,
            buffer,
            rowValues,
            expressionClosurePtr
        ));
}

Value* TCGExprContext::GetExpressionClosurePtr()
{
    return ExpressionClosurePtr;
}

////////////////////////////////////////////////////////////////////////////////

TCodegenConsumer& TCGOperatorContext::operator[] (size_t index) const
{
    if (!(*Consumers_)[index]) {
        (*Consumers_)[index] = std::make_shared<TCodegenConsumer>();
    }
    return *(*Consumers_)[index];
}

////////////////////////////////////////////////////////////////////////////////

TCGValue MakePhi(
    TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    BasicBlock* endBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name)
{
    YCHECK(thenValue.GetStaticType() == elseValue.GetStaticType());
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

    YCHECK(thenData->getType() == elseData->getType());

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

        YCHECK(thenLength->getType() == elseLength->getType());

        phiLength = builder->CreatePHI(thenLength->getType(), 2, name + ".phiLength");
        phiLength->addIncoming(thenLength, thenBB);
        phiLength->addIncoming(elseLength, elseBB);
    }

    return TCGValue::CreateFromValue(builder, phiNull, phiLength, phiData, type, name);
}

Value* MakePhi(
    TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    BasicBlock* endBB,
    Value* thenValue,
    Value* elseValue,
    Twine name)
{
    builder->SetInsertPoint(endBB);
    PHINode* phiValue = builder->CreatePHI(thenValue->getType(), 2, name + ".phiValue");
    phiValue->addIncoming(thenValue, thenBB);
    phiValue->addIncoming(elseValue, elseBB);
    return phiValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
