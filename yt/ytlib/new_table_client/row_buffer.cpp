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
    auto capturedValue = value;
    if (value.Type == EValueType::String || value.Type == EValueType::Any) {
        capturedValue.Data.String = UnalignedPool_.AllocateUnaligned(value.Length);
        memcpy(const_cast<char*>(value.Data.String), value.Data.String, value.Length);
    }
    return capturedValue;
}

TUnversionedRow TRowBuffer::Capture(TUnversionedRow row)
{
    if (!row) {
        return row;
    }

    auto capturedRow = TUnversionedRow::Allocate(&AlignedPool_, row.GetCount());
    for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
        capturedRow[index] = Capture(row[index]);
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
