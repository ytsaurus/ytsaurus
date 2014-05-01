#include "stdafx.h"
#include "row_buffer.h"
#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlignedRowBufferPoolTag { };
struct TUnalignedRowBufferPoolTag { };

TRowBuffer::TRowBuffer(
    i64 alignedPoolChunkSize,
    i64 unalignedPoolChunkSize,
    double maxPoolSmallBlockRatio)
    : AlignedPool_(
    	TAlignedRowBufferPoolTag(),
        alignedPoolChunkSize,
        maxPoolSmallBlockRatio)
    , UnalignedPool_(
    	TUnalignedRowBufferPoolTag(),
        unalignedPoolChunkSize,
        maxPoolSmallBlockRatio)
{ }

TChunkedMemoryPool* TRowBuffer::GetAlignedPool()
{
    return &AlignedPool_;
}

const TChunkedMemoryPool* TRowBuffer::GetAlignedPool() const
{
    return &AlignedPool_;
}

TChunkedMemoryPool* TRowBuffer::GetUnalignedPool()
{
    return &UnalignedPool_;
}

const TChunkedMemoryPool* TRowBuffer::GetUnalignedPool() const
{
    return &UnalignedPool_;
}

TUnversionedValue TRowBuffer::Capture(const TUnversionedValue& value)
{
    TUnversionedValue result;
    Capture(&result, value);
    return result;
}

TUnversionedRow TRowBuffer::Capture(TUnversionedRow row)
{
    if (!row) {
        return row;
    }

    int count = row.GetCount();
    auto capturedRow = TUnversionedRow::Allocate(&AlignedPool_, count);
    for (int index = 0; index < count; ++index) {
        Capture(&capturedRow[index], row[index]);
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

void TRowBuffer::Capture(TUnversionedValue* dstValue, const TUnversionedValue& srcValue)
{
    *dstValue = srcValue;
    if (srcValue.Type == EValueType::String || srcValue.Type == EValueType::Any) {
        dstValue->Data.String = UnalignedPool_.AllocateUnaligned(srcValue.Length);
        memcpy(const_cast<char*>(dstValue->Data.String), srcValue.Data.String, srcValue.Length);
    }
}

i64 TRowBuffer::GetSize() const
{
    return AlignedPool_.GetSize() + UnalignedPool_.GetSize();
}

i64 TRowBuffer::GetCapacity() const
{
    return AlignedPool_.GetCapacity() + UnalignedPool_.GetCapacity();
}

void TRowBuffer::Clear()
{
    AlignedPool_.Clear();
    UnalignedPool_.Clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
