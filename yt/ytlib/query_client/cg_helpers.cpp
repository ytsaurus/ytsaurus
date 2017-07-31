#include "cg_helpers.h"
#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

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
            Builder_->getInt32(TClosureTypeBuilder::Fields::FragmentFlags),
            Builder_->getInt32(ExpressionFragments.Items[index].Index)
        },
        Twine("flag#") + Twine(index));
}

TCGExprContext TCGExprContext::Make(
    const TCGBaseContext& builder,
    const TCodegenFragmentInfos& fragmentInfos,
    Value* expressionClosurePtr)
{
    Value* expressionClosure = builder->CreateLoad(expressionClosurePtr);

    Value* opaqueValues = builder->CreateExtractValue(
        expressionClosure,
        TClosureTypeBuilder::Fields::OpaqueValues,
        "opaqueValues");

    return TCGExprContext(
        TCGOpaqueValuesContext(builder, opaqueValues),
        TCGExprData(
            fragmentInfos,
            builder->CreateExtractValue(
                expressionClosure,
                TClosureTypeBuilder::Fields::Buffer,
                "buffer"),
            builder->CreateExtractValue(
                expressionClosure,
                TClosureTypeBuilder::Fields::RowValues,
                "rowValues"),
            expressionClosurePtr
        ));
}

TCGExprContext TCGExprContext::Make(
    const TCGOpaqueValuesContext& builder,
    const TCodegenFragmentInfos& fragmentInfos,
    Value* row,
    Value* buffer)
{
    Value* rowValues = CodegenValuesPtrFromRow(builder, row);

    llvm::StructType* type = TClosureTypeBuilder::get(builder->getContext(), fragmentInfos.Functions.size());

    Value* expressionClosure = llvm::ConstantStruct::get(
        type,
        {
            llvm::UndefValue::get(TypeBuilder<TValue*, false>::get(builder->getContext())),
            llvm::UndefValue::get(TypeBuilder<void*, false>::get(builder->getContext())),
            llvm::UndefValue::get(TypeBuilder<TRowBuffer*, false>::get(builder->getContext())),
            llvm::ConstantAggregateZero::get(
                llvm::ArrayType::get(TypeBuilder<char, false>::get(
                    builder->getContext()),
                    fragmentInfos.Functions.size())),
            llvm::UndefValue::get(
                llvm::ArrayType::get(TypeBuilder<TValue, false>::get(
                    builder->getContext()),
                    fragmentInfos.Functions.size()))
        });

    expressionClosure = builder->CreateInsertValue(
        expressionClosure,
        rowValues,
        TClosureTypeBuilder::Fields::RowValues);

    expressionClosure = builder->CreateInsertValue(
        expressionClosure,
        builder.GetOpaqueValues(),
        TClosureTypeBuilder::Fields::OpaqueValues);

    expressionClosure = builder->CreateInsertValue(
        expressionClosure,
        buffer,
        TClosureTypeBuilder::Fields::Buffer);

    Value* expressionClosurePtr = builder->CreateAlloca(
        type,
        nullptr,
        "expressionClosurePtr");

    builder->CreateStore(expressionClosure, expressionClosurePtr);

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
// Row manipulation helpers
//

Value* CodegenValuesPtrFromRow(const TCGIRBuilderPtr& builder, Value* row)
{
    auto name = row->getName();

    auto headerPtr = builder->CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        Twine(name).concat(".headerPtr"));

    auto valuesPtr = builder->CreatePointerCast(
        builder->CreateConstInBoundsGEP1_32(nullptr, headerPtr, 1, "valuesPtrUncasted"),
        TypeBuilder<TValue*, false>::get(builder->getContext()),
        Twine(name).concat(".valuesPtr"));

    return valuesPtr;
}

////////////////////////////////////////////////////////////////////////////////

TCGValue MakePhi(
    TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name)
{
    Value* thenNull = thenValue.IsNull();
    Value* thenLength = thenValue.GetLength();
    Value* thenData = thenValue.GetData();

    Value* elseNull = elseValue.IsNull();
    Value* elseLength = elseValue.GetLength();
    Value* elseData = elseValue.GetData();

    PHINode* phiNull = builder->CreatePHI(builder->getInt1Ty(), 2, name + ".phiNull");
    phiNull->addIncoming(thenNull, thenBB);
    phiNull->addIncoming(elseNull, elseBB);

    YCHECK(thenValue.GetStaticType() == elseValue.GetStaticType());
    EValueType type = thenValue.GetStaticType();
    YCHECK(thenData->getType() == elseData->getType());

    PHINode* phiData = builder->CreatePHI(thenData->getType(), 2, name + ".phiData");
    phiData->addIncoming(thenData, thenBB);
    phiData->addIncoming(elseData, elseBB);

    PHINode* phiLength = nullptr;
    if (IsStringLikeType(type)) {
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

} // namespace NQueryClient
} // namespace NYT
