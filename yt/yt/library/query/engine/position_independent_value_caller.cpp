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
    const TRowBufferPtr& buffer)
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

    auto context = TExpressionContext(buffer);

    Callback_(
        positionIndependentLiteralValues,
        opaqueData.Empty() ? nullptr : &opaqueData.Front(),
        positionIndependentResult.GetPIValue(),
        positionIndependentRow.Begin(),
        &context);
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
    const TRowBufferPtr& buffer,
    TValue* result)
{
    auto positionIndependentResult = BorrowFromNonPI(result);

    auto context = TExpressionContext(buffer);

    Callback_(
        &context,
        positionIndependentResult.GetPIValue());
}

template <>
void TCGPICaller<TCGAggregateUpdateSignature, TCGPIAggregateUpdateSignature>::Run(
    const TRowBufferPtr& buffer,
    TValue* result,
    TRange<TValue> input)
{
    TValue resultBuffer = *result;
    auto finallySaveResult = Finally([&] {
        *result = resultBuffer;
    });
    auto positionIndependentResult = BorrowFromNonPI(&resultBuffer);

    auto positionIndependentInput = BorrowFromNonPI(input);

    auto context = TExpressionContext(buffer);

    Callback_(
        &context,
        positionIndependentResult.GetPIValue(),
        positionIndependentInput.Begin());
}

template <>
void TCGPICaller<TCGAggregateMergeSignature, TCGPIAggregateMergeSignature>::Run(
    const TRowBufferPtr& buffer,
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

    auto context = TExpressionContext(buffer);

    Callback_(
        &context,
        positionIndependentResult.GetPIValue(),
        positionIndependentState.GetPIValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
