#include "position_independent_value_caller.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <>
void TCGPICaller<TCGExpressionSignature, TCGPIExpressionSignature>::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TValue* result,
    TRange<TValue> row,
    TRowBuffer* buffer)
{
    auto* positionIndependentLiteralValues = literalValues.Empty()
        ? nullptr
        : &literalValues.Front();

    TValue resultBuffer = *result;
    auto finallySaveResult = Finally([&] {
        *result = resultBuffer;
    });
    auto positionIndependentResult = BorrowFromNonPI(&resultBuffer);

    auto positionIndependentRow = BorrowFromNonPI(row);

    Callback_(
        positionIndependentLiteralValues,
        opaqueData.Empty() ? nullptr : &opaqueData.Front(),
        positionIndependentResult.GetPIValue(),
        positionIndependentRow.Begin(),
        buffer);
}

template <>
void TCGPICaller<TCGQuerySignature, TCGPIQuerySignature>::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TExecutionContext* context)
{
    auto* positionIndependentLiteralValues = literalValues.Empty()
        ? nullptr
        : &literalValues.Front();

    Callback_(
        positionIndependentLiteralValues,
        opaqueData.Empty() ? nullptr : &opaqueData.Front(),
        context);
}

template <>
void TCGPICaller<TCGAggregateInitSignature, TCGPIAggregateInitSignature>::Run(
    TExpressionContext* context,
    TValue* result)
{
    auto positionIndependentResult = BorrowFromNonPI(result);

    Callback_(
        context,
        positionIndependentResult.GetPIValue());
}

template <>
void TCGPICaller<TCGAggregateUpdateSignature, TCGPIAggregateUpdateSignature>::Run(
    TExpressionContext* context,
    TValue* result,
    TRange<TValue> input)
{
    TValue resultBuffer = *result;
    auto finallySaveResult = Finally([&] {
        *result = resultBuffer;
    });
    auto positionIndependentResult = BorrowFromNonPI(&resultBuffer);

    auto positionIndependentInput = BorrowFromNonPI(input);

    Callback_(
        context,
        positionIndependentResult.GetPIValue(),
        positionIndependentInput.Begin());
}

template <>
void TCGPICaller<TCGAggregateMergeSignature, TCGPIAggregateMergeSignature>::Run(
    TExpressionContext* context,
    TValue* result,
    const TValue* state)
{
    TValue resultBuffer = *result;
    resultBuffer = *result;
    auto finallySaveResult = Finally([&] {
        *result = resultBuffer;
    });
    auto positionIndependentResult = BorrowFromNonPI(&resultBuffer);

    auto positionIndependentState = BorrowFromNonPI(const_cast<TValue*>(state));

    Callback_(
        context,
        positionIndependentResult.GetPIValue(),
        positionIndependentState.GetPIValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
