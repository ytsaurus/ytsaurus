#include "row_buffer.h"
#include "unversioned_row.h"
#include "versioned_row.h"

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
    if (IsStringLikeType(value->Type)) {
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

TMutableUnversionedRow TRowBuffer::Capture(TUnversionedRow row, bool deep)
{
    if (!row) {
        return TMutableUnversionedRow();
    }

    int count = row.GetCount();
    auto* values = row.Begin();

    auto capturedRow = TMutableUnversionedRow::Allocate(&Pool_, count);
    auto* capturedValues = capturedRow.Begin();

    ::memcpy(capturedValues, values, count * sizeof (TUnversionedValue));

    if (deep) {
        for (int index = 0; index < count; ++index) {
            Capture(&capturedValues[index]);
        }
    }

    return capturedRow;
}

std::vector<TMutableUnversionedRow> TRowBuffer::Capture(const std::vector<TUnversionedRow>& rows, bool deep)
{
    std::vector<TMutableUnversionedRow> capturedRows(rows.size());
    for (int index = 0; index < static_cast<int>(rows.size()); ++index) {
        capturedRows[index] = Capture(rows[index], deep);
    }
    return capturedRows;
}

TMutableUnversionedRow TRowBuffer::CaptureAndPermuteRow(
    TUnversionedRow row,
    const TTableSchema& tableSchema,
    const TNameTableToSchemaIdMapping& idMapping)
{
    int keyColumnCount = tableSchema.GetKeyColumnCount();
    int columnCount = keyColumnCount;

    for (int index = 0; index < row.GetCount(); ++index) {
        int id = row[index].Id;
        YCHECK(id >= 0 && id < idMapping.size());
        id = idMapping[id];
        YCHECK(id >= 0 && id < tableSchema.Columns().size());
        if (id >= keyColumnCount) {
            ++columnCount;
        }
    }

    auto capturedRow = TMutableUnversionedRow::Allocate(&Pool_, columnCount);
    columnCount = keyColumnCount;

    for (int index = 0; index < row.GetCount(); ++index) {
        int id = idMapping[row[index].Id];
        int place = id < keyColumnCount ? id : columnCount++;
        capturedRow[place] = row[index];
        capturedRow[place].Id = id;
    }

    return capturedRow;
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
