#include "cg_helpers.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
// Row manipulation helpers
//

Value* CodegenValuesPtrFromRow(TCGIRBuilderPtr& builder, Value* row)
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
