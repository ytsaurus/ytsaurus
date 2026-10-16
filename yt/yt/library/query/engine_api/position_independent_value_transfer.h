#pragma once

#include "expression_context.h"
#include "position_independent_value.h"

#include <yt/yt/library/web_assembly/api/pointer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TMutablePIValueRange AllocatePIValueRange(
    TExpressionContext* context,
    int valueCount,
    NWebAssembly::EAddressSpace where);

void CapturePIValue(
    NWebAssembly::IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    TPIValue* value);

TSharedRange<TRange<TPIValue>> CopyAndConvertToPI(
    const TSharedRange<NTableClient::TUnversionedRow>& rows,
    bool captureValues = true);
TSharedRange<TPIRowRange> CopyAndConvertToPI(
    const TSharedRange<NTableClient::TRowRange>& range,
    bool captureValues = true);

NTableClient::TMutableUnversionedRow CopyAndConvertFromPI(
    NWebAssembly::IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    TPIValueRange values,
    bool captureValues = true);

std::vector<NTableClient::TUnversionedRow> CopyAndConvertFromPI(
    NWebAssembly::IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    const std::vector<TPIValueRange>& rows,
    bool captureValues = true);

TMutablePIValueRange CaptureUnversionedValueRange(
    TExpressionContext* context,
    TRange<TValue> range);

TMutablePIValueRange InplaceConvertToPI(NTableClient::TMutableUnversionedValueRange range);
TMutablePIValueRange InplaceConvertToPI(const NTableClient::TUnversionedRow& row);

NTableClient::TMutableUnversionedValueRange InplaceConvertFromPI(TMutablePIValueRange range);

////////////////////////////////////////////////////////////////////////////////

template <class TNonPI>
class TBorrowingPIValueGuard;

template <class TNonPI>
TBorrowingPIValueGuard<TNonPI> BorrowFromNonPI(TNonPI value);

template <class TNonPI>
class TBorrowingNonPIValueGuard;

template <class TPI>
TBorrowingNonPIValueGuard<TPI> BorrowFromPI(TPI value);

////////////////////////////////////////////////////////////////////////////////

template <>
class TBorrowingPIValueGuard<NTableClient::TUnversionedValue*>
    : public TNonCopyable
{
public:
    explicit TBorrowingPIValueGuard(NTableClient::TUnversionedValue* value);
    ~TBorrowingPIValueGuard();

    TPIValue* GetPIValue();

private:
    NTableClient::TUnversionedValue* Value_ = nullptr;
    TPIValue* PIValue_ = nullptr;
};

template <>
class TBorrowingPIValueGuard<NTableClient::TUnversionedValueRange>
    : public TNonCopyable
{
public:
    explicit TBorrowingPIValueGuard(NTableClient::TUnversionedValueRange valueRange);
    ~TBorrowingPIValueGuard();

    TPIValue* Begin();
    const TPIValue& operator[](int index) const;
    size_t Size();

private:
    NTableClient::TMutableUnversionedValueRange ValueRange_{};
    TMutablePIValueRange PIValueRange_{};
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TBorrowingNonPIValueGuard<TPIValue*>
    : public TNonCopyable
{
public:
    explicit TBorrowingNonPIValueGuard(TPIValue* piValue);
    ~TBorrowingNonPIValueGuard();

    NTableClient::TUnversionedValue* GetValue();

private:
    NTableClient::TUnversionedValue* Value_ = nullptr;
    TPIValue* PIValue_ = nullptr;
};

template <>
class TBorrowingNonPIValueGuard<TPIValueRange>
    : public TNonCopyable
{
public:
    explicit TBorrowingNonPIValueGuard(TPIValueRange valueRange);
    ~TBorrowingNonPIValueGuard();

    NTableClient::TUnversionedValue* Begin();
    size_t Size();

private:
    NTableClient::TMutableUnversionedValueRange ValueRange_{};
    TMutablePIValueRange PIValueRange_{};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H_
#include "position_independent_value_transfer-inl.h"
#undef POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H_
