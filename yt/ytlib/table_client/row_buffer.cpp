#include "stdafx.h"
#include "row_buffer.h"
#include "versioned_row.h"
#include "unversioned_row.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TRowBuffer::TRowBuffer(
    i64 chunkSize,
    double maxSmallBlockRatio,
    TRefCountedTypeCookie tagCookie)
    : Pool_(
        chunkSize,
        maxSmallBlockRatio,
        tagCookie)
{ }

TChunkedMemoryPool* TRowBuffer::GetPool()
{
    return &Pool_;
}

void TRowBuffer::Capture(TUnversionedValue* value)
{
    if (IsStringLikeType(EValueType(value->Type))) {
        char* dst = Pool_.AllocateUnaligned(value->Length);
        memcpy(dst, value->Data.String, value->Length);
        value->Data.String = dst;
    }
}

TVersionedValue TRowBuffer::Capture(const TVersionedValue& value)
{
    auto capturedValue = value;
    Capture(&capturedValue);
    return capturedValue;
}

TUnversionedValue TRowBuffer::Capture(const TUnversionedValue& value)
{
    auto capturedValue = value;
    Capture(&capturedValue);
    return capturedValue;
}

TUnversionedRow TRowBuffer::Capture(TUnversionedRow row)
{
    if (!row) {
        return row;
    }

    int count = row.GetCount();
    auto* values = row.Begin();

    auto capturedRow = TUnversionedRow::Allocate(&Pool_, count);
    auto* capturedValues = capturedRow.Begin();

    memcpy(capturedValues, values, count * sizeof (TUnversionedValue));

    for (int index = 0; index < count; ++index) {
        Capture(&capturedValues[index]);
    }

    return capturedRow;
}

std::vector<TUnversionedRow> TRowBuffer::Capture(const std::vector<TUnversionedRow>& rows)
{
    std::vector<TUnversionedRow> capturedRows(rows.size());
    for (int index = 0; index < static_cast<int>(rows.size()); ++index) {
        capturedRows[index] = Capture(rows[index]);
    }
    return capturedRows;
}

i64 TRowBuffer::GetSize() const
{
    return Pool_.GetSize();
}

i64 TRowBuffer::GetCapacity() const
{
    return Pool_.GetCapacity();
}

void TRowBuffer::Clear()
{
    Pool_.Clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
