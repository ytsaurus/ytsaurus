#include "row_buffer.h"

#include "schema.h"
#include "unversioned_row.h"
#include "versioned_row.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool* TRowBuffer::GetPool()
{
    return &Pool_;
}

TMutableUnversionedRow TRowBuffer::AllocateUnversioned(int valueCount)
{
    return TMutableUnversionedRow::Allocate(&Pool_, valueCount);
}

TMutableVersionedRow TRowBuffer::AllocateVersioned(
    int keyCount,
    int valueCount,
    int writeTimestampCount,
    int deleteTimestampCount)
{
    return TMutableVersionedRow::Allocate(
        &Pool_,
        keyCount,
        valueCount,
        writeTimestampCount,
        deleteTimestampCount);
}

void TRowBuffer::CaptureValue(TUnversionedValue* value)
{
    if (IsStringLikeType(value->Type)) {
        char* dst = Pool_.AllocateUnaligned(value->Length);
        memcpy(dst, value->Data.String, value->Length);
        value->Data.String = dst;
    }
}

TVersionedValue TRowBuffer::CaptureValue(const TVersionedValue& value)
{
    auto capturedValue = value;
    CaptureValue(&capturedValue);
    return capturedValue;
}

TUnversionedValue TRowBuffer::CaptureValue(const TUnversionedValue& value)
{
    auto capturedValue = value;
    CaptureValue(&capturedValue);
    return capturedValue;
}

TMutableUnversionedRow TRowBuffer::CaptureRow(TUnversionedRow row, bool captureValues)
{
    if (!row) {
        return TMutableUnversionedRow();
    }

    return CaptureRow(MakeRange(row.Begin(), row.GetCount()), captureValues);
}

void TRowBuffer::CaptureValues(TMutableUnversionedRow row)
{
    if (!row) {
        return;
    }

    for (int index = 0; index < row.GetCount(); ++index) {
        CaptureValue(&row[index]);
    }
}

TMutableUnversionedRow TRowBuffer::CaptureRow(TRange<TUnversionedValue> values, bool captureValues)
{
    int count = static_cast<int>(values.Size());
    auto capturedRow = TMutableUnversionedRow::Allocate(&Pool_, count);
    auto* capturedBegin = capturedRow.Begin();

    ::memcpy(capturedBegin, values.Begin(), count * sizeof (TUnversionedValue));

    if (captureValues) {
        for (int index = 0; index < count; ++index) {
            CaptureValue(&capturedBegin[index]);
        }
    }

    return capturedRow;
}

std::vector<TMutableUnversionedRow> TRowBuffer::CaptureRows(TRange<TUnversionedRow> rows, bool captureValues)
{
    int rowCount = static_cast<int>(rows.Size());
    std::vector<TMutableUnversionedRow> capturedRows(rowCount);
    for (int index = 0; index < rowCount; ++index) {
        capturedRows[index] = CaptureRow(rows[index], captureValues);
    }
    return capturedRows;
}

TMutableUnversionedRow TRowBuffer::CaptureAndPermuteRow(
    TUnversionedRow row,
    const TTableSchema& tableSchema,
    int schemafulColumnCount,
    const TNameTableToSchemaIdMapping& idMapping,
    std::vector<bool>* columnPresenceBuffer)
{
    int valueCount = schemafulColumnCount;

    if (columnPresenceBuffer) {
        ValidateDuplicateAndRequiredValueColumns(row, tableSchema, idMapping, columnPresenceBuffer);
    }

    for (const auto& value : row) {
        ui16 originalId = value.Id;
        YT_VERIFY(originalId < idMapping.size());
        int mappedId = idMapping[originalId];
        if (mappedId < 0) {
            continue;
        }
        if (mappedId >= schemafulColumnCount) {
            ++valueCount;
        }
    }

    auto capturedRow = TMutableUnversionedRow::Allocate(&Pool_, valueCount);
    for (int pos = 0; pos < schemafulColumnCount; ++pos) {
        capturedRow[pos] = MakeUnversionedNullValue(pos);
    }

    valueCount = schemafulColumnCount;

    for (const auto& value : row) {
        ui16 originalId = value.Id;
        int mappedId = idMapping[originalId];
        if (mappedId < 0) {
            continue;
        }
        int pos = mappedId < schemafulColumnCount ? mappedId : valueCount++;
        capturedRow[pos] = value;
        capturedRow[pos].Id = mappedId;
    }

    return capturedRow;
}

TMutableVersionedRow TRowBuffer::CaptureRow(TVersionedRow row, bool captureValues)
{
    if (!row) {
        return TMutableVersionedRow();
    }

    auto capturedRow = TMutableVersionedRow::Allocate(
        &Pool_,
        row.GetKeyCount(),
        row.GetValueCount(),
        row.GetWriteTimestampCount(),
        row.GetDeleteTimestampCount());
    ::memcpy(capturedRow.BeginKeys(), row.BeginKeys(), sizeof(TUnversionedValue) * row.GetKeyCount());
    ::memcpy(capturedRow.BeginValues(), row.BeginValues(), sizeof(TVersionedValue) * row.GetValueCount());
    ::memcpy(capturedRow.BeginWriteTimestamps(), row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
    ::memcpy(capturedRow.BeginDeleteTimestamps(), row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

    if (captureValues) {
        CaptureValues(capturedRow);
    }

    return capturedRow;
}

void TRowBuffer::CaptureValues(TMutableVersionedRow row)
{
    if (!row) {
        return;
    }

    for (int index = 0; index < row.GetKeyCount(); ++index) {
        CaptureValue(row.BeginKeys() + index);
    }
    for (int index = 0; index < row.GetValueCount(); ++index) {
        CaptureValue(row.BeginValues() + index);
    }
}

TMutableVersionedRow TRowBuffer::CaptureAndPermuteRow(
    TVersionedRow row,
    const TTableSchema& tableSchema,
    const TNameTableToSchemaIdMapping& idMapping,
    std::vector<bool>* columnPresenceBuffer)
{
    int keyColumnCount = tableSchema.GetKeyColumnCount();
    YT_VERIFY(keyColumnCount == row.GetKeyCount());
    YT_VERIFY(keyColumnCount <= idMapping.size());

    int valueCount = 0;
    int deleteTimestampCount = row.GetDeleteTimestampCount();

    SmallVector<TTimestamp, 64> writeTimestamps;
    for (const auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
        ui16 originalId = value->Id;
        YT_VERIFY(originalId < idMapping.size());
        int mappedId = idMapping[originalId];
        if (mappedId < 0) {
            continue;
        }
        YT_VERIFY(mappedId < tableSchema.Columns().size());
        ++valueCount;
        writeTimestamps.push_back(value->Timestamp);
    }

    std::sort(writeTimestamps.begin(), writeTimestamps.end(), std::greater<TTimestamp>());
    writeTimestamps.erase(std::unique(writeTimestamps.begin(), writeTimestamps.end()), writeTimestamps.end());
    int writeTimestampCount = static_cast<int>(writeTimestamps.size());

    if (columnPresenceBuffer) {
        ValidateDuplicateAndRequiredValueColumns(
            row,
            tableSchema,
            idMapping,
            columnPresenceBuffer,
            writeTimestamps.data(),
            writeTimestampCount);
    }

    auto capturedRow = TMutableVersionedRow::Allocate(
        &Pool_,
        keyColumnCount,
        valueCount,
        writeTimestampCount,
        deleteTimestampCount);

    ::memcpy(capturedRow.BeginWriteTimestamps(), writeTimestamps.data(), sizeof (TTimestamp) * writeTimestampCount);
    ::memcpy(capturedRow.BeginDeleteTimestamps(), row.BeginDeleteTimestamps(), sizeof (TTimestamp) * deleteTimestampCount);

    {
        int index = 0;
        auto* dstValue = capturedRow.BeginKeys();
        for (const auto* srcValue = row.BeginKeys(); srcValue != row.EndKeys(); ++srcValue, ++index) {
            YT_VERIFY(idMapping[index] == index);
            *dstValue++ = *srcValue;
        }
    }

    {
        auto* dstValue = capturedRow.BeginValues();
        for (const auto* srcValue = row.BeginValues(); srcValue != row.EndValues(); ++srcValue) {
            ui16 originalId = srcValue->Id;
            int mappedId = idMapping[originalId];
            if (mappedId < 0) {
                continue;
            }
            *dstValue = *srcValue;
            dstValue->Id = mappedId;
            ++dstValue;
        }
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

void TRowBuffer::Purge()
{
    Pool_.Purge();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
