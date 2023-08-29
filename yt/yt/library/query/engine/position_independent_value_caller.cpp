#include "position_independent_value_caller.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

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
    auto positionIndependentResult = BorrowFromNonPI(result);
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
    TValue* first,
    TRange<TValue> second)
{
    auto positionIndependentFirst = BorrowFromNonPI(first);
    auto positionIndependentSecond = BorrowFromNonPI(second);

    Callback_(
        context,
        positionIndependentFirst.GetPIValue(),
        positionIndependentSecond.Begin());
}

template <>
void TCGPICaller<TCGAggregateMergeSignature, TCGPIAggregateMergeSignature>::Run(
    TExpressionContext* context,
    TValue* first,
    const TValue* second)
{
    auto positionIndependentFirst = BorrowFromNonPI(first);
    auto positionIndependentSecond = BorrowFromNonPI(const_cast<TValue*>(second));

    Callback_(
        context,
        positionIndependentFirst.GetPIValue(),
        positionIndependentSecond.GetPIValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
