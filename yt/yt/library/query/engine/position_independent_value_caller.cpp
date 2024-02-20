#include "position_independent_value_caller.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/library/web_assembly/api/compartment.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NQueryClient {

using NWebAssembly::IWebAssemblyCompartment;

////////////////////////////////////////////////////////////////////////////////

template <>
void TCGPICaller<TCGExpressionSignature, TCGPIExpressionSignature>::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    TValue* result,
    TRange<TValue> row,
    const TRowBufferPtr& buffer,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(!compartment);

    Y_UNUSED(opaqueDataSizes);

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
    TRange<size_t> opaqueDataSizes,
    TExecutionContext* context,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(!compartment);

    Y_UNUSED(opaqueDataSizes);

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
    TValue* result,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(!compartment);

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
    TRange<TValue> input,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(!compartment);

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
    const TValue* state,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(!compartment);

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
