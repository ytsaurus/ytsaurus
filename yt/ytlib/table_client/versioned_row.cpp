#include "versioned_row.h"
#include "row_buffer.h"
#include "name_table.h"

#include <yt/core/misc/format.h>

#include <numeric>

namespace NYT {
namespace NTableClient {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

size_t GetByteSize(const TVersionedValue& value)
{
    return GetByteSize(static_cast<TUnversionedValue>(value)) + MaxVarInt64Size;
}

size_t GetDataWeight(const TVersionedValue& value)
{
    return GetDataWeight(static_cast<TUnversionedValue>(value)) + sizeof(TTimestamp);
}

size_t ReadValue(const char* input, TVersionedValue* value)
{
    int result = ReadValue(input, static_cast<TUnversionedValue*>(value));
    result += ReadVarUint64(input + result, &value->Timestamp);
    return result;
}

size_t WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUint64(output + result, value.Timestamp);
    return result;
}

TString ToString(const TVersionedValue& value)
{
    return Format("%v@%llx",
        static_cast<TUnversionedValue>(value),
        value.Timestamp);
}

void Save(TStreamSaveContext& context, const TVersionedValue& value)
{
    NYT::Save(context, value.Timestamp);
    NTableClient::Save(context, static_cast<const TUnversionedValue&>(value));
}

void Load(TStreamLoadContext& context, TVersionedValue& value, TChunkedMemoryPool* pool)
{
    NYT::Load(context, value.Timestamp);
    NTableClient::Load(context, static_cast<TUnversionedValue&>(value), pool);
}

////////////////////////////////////////////////////////////////////////////////

size_t GetVersionedRowByteSize(
    int keyCount,
    int valueCount,
    int writeTimestampCount,
    int deleteTimestampCount)
{
    return
        sizeof(TVersionedRowHeader) +
        sizeof(TUnversionedValue) * keyCount +
        sizeof(TVersionedValue) * valueCount +
        sizeof(TTimestamp) * writeTimestampCount +
        sizeof(TTimestamp) * deleteTimestampCount;
}

size_t GetDataWeight(TVersionedRow row)
{
    if (!row) {
        return 0;
    }

    size_t result = 0;
    result += std::accumulate(
        row.BeginValues(),
        row.EndValues(),
        0ll,
        [] (size_t x, const TVersionedValue& value) {
            return GetDataWeight(value) + x;
        });

    result += std::accumulate(
        row.BeginKeys(),
        row.EndKeys(),
        0ll,
        [] (size_t x, const TUnversionedValue& value) {
            return GetDataWeight(value) + x;
        });

    result += row.GetWriteTimestampCount() * sizeof(TTimestamp);
    result += row.GetDeleteTimestampCount() * sizeof(TTimestamp);

    return result;
}

size_t GetHash(TVersionedRow row)
{
    size_t result = 0xdeadc0de;
    int partCount = row.GetKeyCount() + row.GetValueCount();
    for (int i = 0; i < row.GetKeyCount(); ++i) {
        result = (result * 1000003) ^ GetHash(row.BeginKeys()[i]);
    }
    for (int i = 0; i < row.GetValueCount(); ++i) {
        result = (result * 1000003) ^ GetHash(row.BeginValues()[i]);
    }
    return result ^ partCount;
}

bool AreRowsIdentical(TVersionedRow lhs, TVersionedRow rhs)
{
    if (!lhs && !rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    if (lhs.GetKeyCount() != rhs.GetKeyCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetKeyCount(); ++i) {
        if (!AreRowValuesIdentical(lhs.BeginKeys()[i], lhs.BeginKeys()[i])) {
            return false;
        }
    }

    if (lhs.GetValueCount() != rhs.GetValueCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetValueCount(); ++i) {
        if (!AreRowValuesIdentical(lhs.BeginValues()[i], rhs.BeginValues()[i])) {
            return false;
        }
    }

    if (lhs.GetWriteTimestampCount() != rhs.GetWriteTimestampCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetWriteTimestampCount(); ++i) {
        if (lhs.BeginWriteTimestamps()[i] != rhs.BeginWriteTimestamps()[i]) {
            return false;
        }
    }

    if (lhs.GetDeleteTimestampCount() != rhs.GetDeleteTimestampCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetDeleteTimestampCount(); ++i) {
        if (lhs.BeginDeleteTimestamps()[i] != rhs.BeginDeleteTimestamps()[i]) {
            return false;
        }
    }

    return true;
}

TString ToString(TVersionedRow row)
{
    if (!row) {
        return "<Null>";
    }

    TStringBuilder builder;
    builder.AppendChar('[');
    for (int index = 0; index < row.GetKeyCount(); ++index) {
        if (index > 0) {
            builder.AppendString(AsStringBuf(","));
        }
        const auto& value = row.BeginKeys()[index];
        builder.AppendFormat("%v", value);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetValueCount(); ++index) {
        if (index > 0) {
            builder.AppendString(AsStringBuf(","));
        }
        const auto& value = row.BeginValues()[index];
        builder.AppendFormat("%v", value);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetWriteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(AsStringBuf(","));
        }
        builder.AppendFormat("%llx", row.BeginWriteTimestamps()[index]);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetDeleteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(AsStringBuf(","));
        }
        builder.AppendFormat("%llx", row.BeginDeleteTimestamps()[index]);
    }
    builder.AppendChar(']');
    return builder.Flush();
}

TString ToString(TMutableVersionedRow row)
{
    return ToString(TVersionedRow(row));
}

TString ToString(const TVersionedOwningRow& row)
{
    return ToString(row.Get());
}

void ValidateClientDataRow(
    TVersionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable)
{
    int keyCount = row.GetKeyCount();
    if (keyCount != schema.GetKeyColumnCount()) {
        THROW_ERROR_EXCEPTION("Invalid key count: expected %v, got %v",
            schema.GetKeyColumnCount(),
            keyCount);
    }

    ValidateKeyColumnCount(keyCount);
    ValidateRowValueCount(row.GetValueCount());

    if (nameTable->GetSize() < keyCount) {
        THROW_ERROR_EXCEPTION("Name table size is too small to contain all keys: expected >=%v, got %v",
            row.GetKeyCount(),
            nameTable->GetSize());
    }

    for (int index = 0; index < keyCount; ++index) {
        const auto& expectedName = schema.Columns()[index].Name();
        auto actualName = nameTable->GetName(index);
        if (expectedName != actualName) {
            THROW_ERROR_EXCEPTION("Invalid key column %v in name table: expected %Qv, got %Qv",
                index,
                expectedName,
                actualName);
        }
        ValidateKeyValue(row.BeginKeys()[index]);
    }

    auto validateTimestamps = [&] (const TTimestamp* begin, const TTimestamp* end) {
        for (const auto* current = begin; current != end; ++current) {
            ValidateWriteTimestamp(*current);
            if (current != begin && *current >= *(current - 1)) {
                THROW_ERROR_EXCEPTION("Timestamps are not monotonically decreasing: %v >= %v",
                    *current,
                    *(current - 1));
            }
        }
    };
    validateTimestamps(row.BeginWriteTimestamps(), row.EndWriteTimestamps());
    validateTimestamps(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps());

    for (const auto* current = row.BeginValues(); current != row.EndValues(); ++current) {
        if (current != row.BeginValues()) {
            auto* prev = current - 1;
            if (current->Id < prev->Id) {
                THROW_ERROR_EXCEPTION("Value ids must be non-decreasing: %v < %v",
                    current->Id,
                    prev->Id);
            }
            if (current->Id == prev->Id && current->Timestamp >= prev->Timestamp) {
                THROW_ERROR_EXCEPTION("Value timestamps must be decreasing: %v >= %v",
                    current->Timestamp,
                    prev->Timestamp);
            }
        }

        const auto& value = *current;
        int mappedId = ApplyIdMapping(value, schema, &idMapping);

        if (mappedId < 0 || mappedId > schema.Columns().size()) {
            int size = nameTable->GetSize();
            if (value.Id < 0 || value.Id >= size) {
                THROW_ERROR_EXCEPTION("Expected value id in range [0:%v] but got %v",
                    size - 1,
                    value.Id);
            }

            THROW_ERROR_EXCEPTION("Unexpected column %Qv", nameTable->GetName(value.Id));
        }

        if (mappedId < keyCount) {
            THROW_ERROR_EXCEPTION("Key component %Qv appears in value part",
                schema.Columns()[mappedId].Name());
        }

        const auto& column = schema.Columns()[mappedId];
        ValidateValueType(value, schema, mappedId, /*typeAnyAcceptsAllValues*/ false);

        if (value.Aggregate && !column.Aggregate()) {
            THROW_ERROR_EXCEPTION(
                "\"aggregate\" flag is set for value in column %Qv which is not aggregating",
                column.Name());
        }

        if (mappedId < schema.GetKeyColumnCount()) {
            THROW_ERROR_EXCEPTION("Key column %Qv in values",
                column.Name());
        }

        ValidateDataValue(value);
    }

    auto dataWeight = GetDataWeight(row);
    if (dataWeight >= MaxClientVersionedRowDataWeight) {
        THROW_ERROR_EXCEPTION("Row is too large: data weight %v, limit %v",
            dataWeight,
            MaxClientVersionedRowDataWeight);
    }
}

void ValidateDuplicateAndRequiredValueColumns(
    TVersionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    std::vector<bool>* columnPresenceBuffer,
    const TTimestamp* writeTimestamps,
    int writeTimestampCount)
{
    auto& columnSeen = *columnPresenceBuffer;
    YCHECK(columnSeen.size() >= schema.GetColumnCount());
    std::fill(columnSeen.begin(), columnSeen.end(), 0);

    for (const auto *valueGroupBeginIt = row.BeginValues(), *valueGroupEndIt = valueGroupBeginIt;
        valueGroupBeginIt != row.EndValues();
        valueGroupBeginIt = valueGroupEndIt)
    {
        while (valueGroupEndIt != row.EndValues() && valueGroupBeginIt->Id == valueGroupEndIt->Id) {
            ++valueGroupEndIt;
        }

        int mappedId = ApplyIdMapping(*valueGroupBeginIt, schema, &idMapping);
        if (mappedId < 0) {
            continue;
        }
        const auto& column = schema.Columns()[mappedId];

        if (columnSeen[mappedId]) {
            THROW_ERROR_EXCEPTION("Duplicate value group %Qv in versioned row",
                column.Name());
        }
        columnSeen[mappedId] = true;

        if (column.Required()) {
            auto mismatch = std::mismatch(
                writeTimestamps,
                writeTimestamps + writeTimestampCount,
                valueGroupBeginIt,
                valueGroupEndIt,
                [] (TTimestamp expected, const TVersionedValue& actual) {
                    return expected == actual.Timestamp;
                }
            );
            if (mismatch.first == writeTimestamps + writeTimestampCount) {
                if (mismatch.second != valueGroupEndIt) {
                    THROW_ERROR_EXCEPTION(
                        "Row-wise write timestamps do not contain write timestamp %v for column %Qv",
                        *mismatch.second,
                        column.Name());
                }
            } else {
                THROW_ERROR_EXCEPTION(
                    "Required column %Qv does not contain value for timestamp %Qv",
                    column.Name(),
                    *mismatch.first);
            }
        }
    }

    for (int index = schema.GetKeyColumnCount(); index < schema.GetColumnCount(); ++index) {
        if (!columnSeen[index] && schema.Columns()[index].Required()) {
            THROW_ERROR_EXCEPTION("Missing values for required column %Qv",
                schema.Columns()[index].Name());
        }
    }
}

TOwningKey RowToKey(TVersionedRow row)
{
    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < row.GetKeyCount(); ++index) {
        builder.AddValue(row.BeginKeys()[index]);
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowBuilder::TVersionedRowBuilder(TRowBufferPtr buffer, bool compaction)
    : Buffer_(std::move(buffer))
    , Compaction_(compaction)
{ }

void TVersionedRowBuilder::AddKey(const TUnversionedValue& value)
{
    Keys_.push_back(value);
}

void TVersionedRowBuilder::AddValue(const TVersionedValue& value)
{
    WriteTimestamps_.push_back(value.Timestamp);
    Values_.push_back(Buffer_->Capture(value));
}

void TVersionedRowBuilder::AddDeleteTimestamp(TTimestamp timestamp)
{
    DeleteTimestamps_.push_back(timestamp);
}

void TVersionedRowBuilder::AddWriteTimestamp(TTimestamp timestamp)
{
    WriteTimestamps_.push_back(timestamp);
}

TMutableVersionedRow TVersionedRowBuilder::FinishRow()
{
    std::sort(
        Values_.begin(),
        Values_.end(),
        [] (const TVersionedValue& lhs, const TVersionedValue& rhs) -> bool {
            if (lhs.Id < rhs.Id) {
                return true;
            }
            if (lhs.Id > rhs.Id) {
                return false;
            }
            if (lhs.Timestamp < rhs.Timestamp) {
                return false;
            }
            if (lhs.Timestamp > rhs.Timestamp) {
                return true;
            }
            return false;
        });

    std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), std::greater<TTimestamp>());

    if (Compaction_) {
        WriteTimestamps_.erase(
            std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
            WriteTimestamps_.end());
    } else if (!WriteTimestamps_.empty()) {
        WriteTimestamps_.erase(WriteTimestamps_.begin() + 1, WriteTimestamps_.end());
    }

    std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), std::greater<TTimestamp>());
    DeleteTimestamps_.erase(
        std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
        DeleteTimestamps_.end());

    auto row = Buffer_->AllocateVersioned(
        Keys_.size(),
        Values_.size(),
        WriteTimestamps_.size(),
        DeleteTimestamps_.size());

    memcpy(row.BeginKeys(), Keys_.data(), sizeof (TUnversionedValue) * Keys_.size());
    memcpy(row.BeginValues(), Values_.data(), sizeof (TVersionedValue)* Values_.size());
    memcpy(row.BeginWriteTimestamps(), WriteTimestamps_.data(), sizeof (TTimestamp) * WriteTimestamps_.size());
    memcpy(row.BeginDeleteTimestamps(), DeleteTimestamps_.data(), sizeof (TTimestamp) * DeleteTimestamps_.size());

    Keys_.clear();
    Values_.clear();
    WriteTimestamps_.clear();
    DeleteTimestamps_.clear();

    return row;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedOwningRow::TVersionedOwningRow(TVersionedRow other)
{
    if (!other)
        return;

    size_t fixedSize = GetVersionedRowByteSize(
        other.GetKeyCount(),
        other.GetValueCount(),
        other.GetWriteTimestampCount(),
        other.GetDeleteTimestampCount());

    size_t variableSize = 0;
    auto adjustVariableSize = [&] (const TUnversionedValue& value) {
        if (IsStringLikeType(value.Type)) {
            variableSize += value.Length;
        }
    };

    for (int index = 0; index < other.GetKeyCount(); ++index) {
        adjustVariableSize(other.BeginKeys()[index]);
    }
    for (int index = 0; index < other.GetValueCount(); ++index) {
        adjustVariableSize(other.BeginValues()[index]);
    }

    Data_ = TSharedMutableRef::Allocate(fixedSize + variableSize, false);

    ::memcpy(GetMutableHeader(), other.GetHeader(), fixedSize);

    if (variableSize > 0) {
        char* current = Data_.Begin() + fixedSize;
        auto captureValue = [&] (TUnversionedValue* value) {
            if (IsStringLikeType(value->Type)) {
                ::memcpy(current, value->Data.String, value->Length);
                value->Data.String = current;
                current += value->Length;
            }
        };

        for (int index = 0; index < other.GetKeyCount(); ++index) {
            captureValue(BeginMutableKeys() + index);
        }
        for (int index = 0; index < other.GetValueCount(); ++index) {
            captureValue(BeginMutableValues() + index);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
