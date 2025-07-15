#include "position_independent_value_transfer.h"

#include <yt/yt/client/table_client/row_buffer.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NQueryClient {

using namespace NWebAssembly;

////////////////////////////////////////////////////////////////////////////////

TMutablePIValueRange AllocatePIValueRange(TExpressionContext* context, int valueCount, NWebAssembly::EAddressSpace where)
{
    auto* data = context->AllocateAligned(sizeof(TPIValue) * valueCount, where);
    return TMutablePIValueRange(
        reinterpret_cast<TPIValue*>(data),
        static_cast<size_t>(valueCount));
}

void CapturePIValue(
    IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    TPIValue* value)
{
    auto* valueAtHost = PtrFromVM(compartment, value);
    if (IsStringLikeType(valueAtHost->Type)) {
        auto* dataCopy = context->AllocateUnaligned(valueAtHost->Length, EAddressSpace::WebAssembly);
        valueAtHost = PtrFromVM(compartment, value); // NB: Possible reallocation.
        auto* dataCopyAtHost = PtrFromVM(compartment, dataCopy, valueAtHost->Length);
        ::memcpy(dataCopyAtHost, valueAtHost->AsStringBuf().data(), valueAtHost->Length);
        valueAtHost->SetStringPosition(dataCopyAtHost);
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TRowBufferHolder)

struct TRowBufferHolder
    : public TSharedRangeHolder
{
    explicit TRowBufferHolder(TRowBufferPtr rowBuffer)
        : RowBuffer(rowBuffer)
    { }

    const TRowBufferPtr RowBuffer;
};

DEFINE_REFCOUNTED_TYPE(TRowBufferHolder)

TRowBufferHolderPtr MakeRowBufferHolder(TRowBufferPtr rowBuffer)
{
    return New<TRowBufferHolder>(rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

struct TPIValueTransferBufferTag
{ };

static TMutablePIValueRange CapturePIValueRange(
    TRowBuffer* buffer,
    TUnversionedValueRange values,
    bool captureValues)
{
    auto captured = buffer->CaptureRow(values, captureValues);
    InplaceConvertToPI(captured);
    return TMutablePIValueRange(
        reinterpret_cast<TPIValue*>(captured.Begin()),
        static_cast<size_t>(captured.GetCount()));
}

TSharedRange<TRange<TPIValue>> CopyAndConvertToPI(
    const TSharedRange<TUnversionedRow>& rows,
    bool captureValues)
{
    auto buffer = New<TRowBuffer>(TPIValueTransferBufferTag());

    auto holder = TSharedRangeHolderPtr(MakeRowBufferHolder(buffer));
    if (!captureValues) {
        holder = MakeCompositeSharedRangeHolder({holder, rows.GetHolder()});
    }

    auto rowRange = TSharedMutableRange<TRange<TPIValue>>(
        reinterpret_cast<TRange<TPIValue>*>(
            buffer->GetPool()->AllocateAligned(sizeof(TRange<TPIValue>) * rows.Size())),
        rows.Size(),
        holder);

    for (size_t rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
        auto captured = CapturePIValueRange(
            buffer.Get(),
            TUnversionedValueRange(
                rows[rowIndex].Begin(),
                rows[rowIndex].GetCount()),
            captureValues);
        rowRange[rowIndex] = captured;
    }

    return TSharedRange<TRange<TPIValue>>(
        rowRange.Begin(),
        rowRange.Size(),
        rowRange.GetHolder());
}

TSharedRange<TPIRowRange> CopyAndConvertToPI(
    const TSharedRange<TRowRange>& range,
    bool captureValues)
{
    auto buffer = New<TRowBuffer>(TPIValueTransferBufferTag());

    auto holder = TSharedRangeHolderPtr(MakeRowBufferHolder(buffer));
    if (!captureValues) {
        holder = MakeCompositeSharedRangeHolder({holder, range.GetHolder()});
    }

    auto mutableRange = TSharedMutableRange<TPIRowRange>(
        reinterpret_cast<TPIRowRange*>(
            buffer->GetPool()->AllocateAligned(
                sizeof(TPIRowRange) * range.Size())),
        range.Size(),
        holder);

    for (size_t rowIndex = 0; rowIndex < range.Size(); ++rowIndex) {
        {
            auto captured = CapturePIValueRange(
                buffer.Get(),
                TUnversionedValueRange(
                    range[rowIndex].first.Begin(),
                    range[rowIndex].first.GetCount()),
                captureValues);

            mutableRange[rowIndex].first = TRange<TPIValue>(
                captured.Begin(),
                captured.Size());
        }
        {
            auto captured = CapturePIValueRange(
                buffer.Get(),
                TUnversionedValueRange(
                    range[rowIndex].second.Begin(),
                    range[rowIndex].second.GetCount()),
                captureValues);

            mutableRange[rowIndex].second = TRange<TPIValue>(
                captured.Begin(),
                captured.Size());
        }
    }

    return TSharedRange<TPIRowRange>(
        mutableRange.Begin(),
        mutableRange.Size(),
        mutableRange.GetHolder());
}

////////////////////////////////////////////////////////////////////////////////

TMutableUnversionedRow CopyAndConvertFromPI(
    IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    TPIValueRange values,
    bool captureValues)
{
    auto* bytes = context->AllocateAligned(GetUnversionedRowByteSize(values.Size()), EAddressSpace::Host);
    auto capturedRow = TMutableUnversionedRow::Create(bytes, values.Size());

    auto* valuesAtHost = PtrFromVM(compartment, values.Begin(), values.Size());

    for (size_t index = 0; index < values.Size(); ++index) {
        MakeUnversionedFromPositionIndependent(&capturedRow[index], valuesAtHost[index]);
    }

    if (captureValues) {
        context->GetRowBuffer()->CaptureValues(capturedRow);
    }

    return capturedRow;
}

std::vector<TUnversionedRow> CopyAndConvertFromPI(
    IWebAssemblyCompartment* compartment,
    TExpressionContext* context,
    const std::vector<TPIValueRange>& rows,
    bool captureValues)
{
    std::vector<TUnversionedRow> result;
    result.reserve(rows.size());

    for (auto& row : rows) {
        result.push_back(CopyAndConvertFromPI(compartment, context, row, captureValues));
    }

    return result;
}

TMutablePIValueRange CaptureUnversionedValueRange(TExpressionContext* context, TRange<TValue> range)
{
    i64 rangeByteLength = range.size() * sizeof(TPIValue);
    i64 byteLength = rangeByteLength;
    for (auto& value : range) {
        if (IsStringLikeType(value.Type)) {
            byteLength += value.Length;
        }
    }

    auto* copyOffset = context->AllocateAligned(byteLength, EAddressSpace::WebAssembly);
    auto* destination = PtrFromVM(GetCurrentCompartment(), std::bit_cast<char*>(copyOffset), byteLength);
    auto* copiedRangeAtHost = std::bit_cast<TPIValue*>(destination);

    ::memcpy(destination, range.Begin(), rangeByteLength);
    destination += rangeByteLength;

    for (size_t index = 0; index < range.size(); ++index) {
        if (IsStringLikeType(range[index].Type)) {
            ::memcpy(destination, range[index].AsStringBuf().data(), range[index].Length);
            copiedRangeAtHost[index].SetStringPosition(destination);
            destination += range[index].Length;
        }
    }

    return TMutableRange(std::bit_cast<TPIValue*>(copyOffset), range.Size());
}

////////////////////////////////////////////////////////////////////////////////

TMutablePIValueRange InplaceConvertToPI(TMutableUnversionedValueRange range)
{
    auto positionIndependent = TMutablePIValueRange(
        reinterpret_cast<TPIValue*>(range.Begin()),
        range.Size());

    for (size_t index = 0; index < range.Size(); ++index) {
        MakePositionIndependentFromUnversioned(&positionIndependent[index], range[index]);
    }

    return positionIndependent;
}

TMutablePIValueRange InplaceConvertToPI(const TUnversionedRow& row)
{
    return InplaceConvertToPI(
        TMutableUnversionedValueRange(
            const_cast<TUnversionedValue*>(row.Begin()),
            static_cast<size_t>(row.GetCount())));
}

////////////////////////////////////////////////////////////////////////////////

TMutableUnversionedValueRange InplaceConvertFromPI(TMutablePIValueRange range)
{
    auto unversioned = TMutableUnversionedValueRange(
        reinterpret_cast<TUnversionedValue*>(range.Begin()),
        range.Size());

    for (size_t index = 0; index < range.Size(); ++index) {
        MakeUnversionedFromPositionIndependent(&unversioned[index], range[index]);
    }

    return unversioned;
}

////////////////////////////////////////////////////////////////////////////////

TBorrowingPIValueGuard<TUnversionedValue*>::TBorrowingPIValueGuard(TUnversionedValue* value)
    : Value_(value)
{
    PIValue_ = reinterpret_cast<TPIValue*>(Value_);
    MakePositionIndependentFromUnversioned(PIValue_, *Value_);
}

TBorrowingPIValueGuard<TUnversionedValue*>::~TBorrowingPIValueGuard()
{
    MakeUnversionedFromPositionIndependent(Value_, *PIValue_);
}

TPIValue* TBorrowingPIValueGuard<TUnversionedValue*>::TBorrowingPIValueGuard<TUnversionedValue*>::GetPIValue()
{
    return PIValue_;
}

////////////////////////////////////////////////////////////////////////////////

TBorrowingPIValueGuard<TUnversionedValueRange>::TBorrowingPIValueGuard(
    TUnversionedValueRange valueRange)
{
    if (valueRange.Empty()) {
        return;
    }

    ValueRange_ = TMutableUnversionedValueRange(
        const_cast<TUnversionedValue*>(&valueRange.Front()),
        valueRange.Size());

    PIValueRange_ = TMutablePIValueRange(
        reinterpret_cast<TPIValue*>(&ValueRange_.Front()),
        ValueRange_.Size());

    InplaceConvertToPI(ValueRange_);
}

TBorrowingPIValueGuard<TUnversionedValueRange>::~TBorrowingPIValueGuard()
{
    InplaceConvertFromPI(PIValueRange_);
}

TPIValue* TBorrowingPIValueGuard<TUnversionedValueRange>::Begin()
{
    if (PIValueRange_.Empty()) {
        return nullptr;
    }

    return &PIValueRange_.Front();
}

const TPIValue& TBorrowingPIValueGuard<TUnversionedValueRange>::operator[](int index) const
{
    return PIValueRange_[index];
}

size_t TBorrowingPIValueGuard<TUnversionedValueRange>::Size()
{
    return PIValueRange_.Size();
}

////////////////////////////////////////////////////////////////////////////////

TBorrowingNonPIValueGuard<TPIValue*>::TBorrowingNonPIValueGuard(TPIValue* piValue)
    : PIValue_(piValue)
{
    Value_ = reinterpret_cast<TUnversionedValue*>(PIValue_);
    MakeUnversionedFromPositionIndependent(Value_, *PIValue_);
}

TBorrowingNonPIValueGuard<TPIValue*>::~TBorrowingNonPIValueGuard()
{
    MakePositionIndependentFromUnversioned(PIValue_, *Value_);
}

TUnversionedValue* TBorrowingNonPIValueGuard<TPIValue*>::GetValue()
{
    return Value_;
}

////////////////////////////////////////////////////////////////////////////////

TBorrowingNonPIValueGuard<TPIValueRange>::TBorrowingNonPIValueGuard(
    TPIValueRange valueRange)
{
    if (valueRange.Empty()) {
        return;
    }

    PIValueRange_ = TMutablePIValueRange(
        const_cast<TPIValue*>(&valueRange.Front()),
        valueRange.Size());

    ValueRange_ = TMutableUnversionedValueRange(
        reinterpret_cast<TUnversionedValue*>(&PIValueRange_.Front()),
        PIValueRange_.Size());

    InplaceConvertFromPI(PIValueRange_);
}

TBorrowingNonPIValueGuard<TPIValueRange>::~TBorrowingNonPIValueGuard()
{
    InplaceConvertToPI(ValueRange_);
}

TUnversionedValue* TBorrowingNonPIValueGuard<TPIValueRange>::Begin()
{
    if (ValueRange_.Empty()) {
        return nullptr;
    }

    return &ValueRange_.Front();
}

size_t TBorrowingNonPIValueGuard<TPIValueRange>::Size()
{
    return PIValueRange_.Size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
