#include "stdafx.h"
#include "versioned_row.h"
#include "row_buffer.h"

#include <core/misc/format.h>

#include <numeric>

namespace NYT {
namespace NVersionedTableClient {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TVersionedValue& value)
{
    return GetByteSize(static_cast<TUnversionedValue>(value)) + MaxVarInt64Size;
}

int GetDataWeight(const TVersionedValue& value)
{
    return GetDataWeight(static_cast<TUnversionedValue>(value)) + sizeof(TTimestamp);
}

int ReadValue(const char* input, TVersionedValue* value)
{
    int result = ReadValue(input, static_cast<TUnversionedValue*>(value));
    result += ReadVarUint64(input + result, &value->Timestamp);
    return result;
}

int WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUint64(output + result, value.Timestamp);
    return result;
}

Stroka ToString(const TVersionedValue& value)
{
    return Format("%v@%v",
        static_cast<TUnversionedValue>(value),
        value.Timestamp);
}

void Save(TStreamSaveContext& context, const TVersionedValue& value)
{
    NYT::Save(context, value.Timestamp);
    NVersionedTableClient::Save(context, static_cast<const TUnversionedValue&>(value));
}

void Load(TStreamLoadContext& context, TVersionedValue& value, TChunkedMemoryPool* pool)
{
    NYT::Load(context, value.Timestamp);
    NVersionedTableClient::Load(context, static_cast<TUnversionedValue&>(value), pool);
}

////////////////////////////////////////////////////////////////////////////////

size_t GetVersionedRowDataSize(
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

i64 GetDataWeight(TVersionedRow row)
{
    i64 result = 0;
    result += std::accumulate(
        row.BeginValues(),
        row.EndValues(),
        0ll,
        [] (i64 x, const TVersionedValue& value) {
            return GetDataWeight(value) + x;
        });

    result += std::accumulate(
        row.BeginKeys(),
        row.EndKeys(),
        0ll,
        [] (i64 x, const TUnversionedValue& value) {
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

bool operator == (TVersionedRow lhs, TVersionedRow rhs)
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
        if (lhs.BeginKeys()[i] != rhs.BeginKeys()[i]) {
            return false;
        }
    }

    if (lhs.GetValueCount() != rhs.GetValueCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetValueCount(); ++i) {
        if (lhs.BeginValues()[i] != rhs.BeginValues()[i]) {
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

bool operator != (TVersionedRow lhs, TVersionedRow rhs)
{
    return !(lhs == rhs);
}

Stroka ToString(TVersionedRow row)
{
    if (!row) {
        return "<Null>";
    }

    TStringBuilder builder;
    builder.AppendChar('[');
    for (int index = 0; index < row.GetKeyCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(", "));
        }
        const auto& value = row.BeginKeys()[index];
        builder.AppendString(ToString(value));
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetValueCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(", "));
        }
        const auto& value = row.BeginValues()[index];
        builder.AppendFormat("%v#%v",
            value.Id,
            value);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetWriteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(", "));
        }
        builder.AppendFormat("%v", row.BeginWriteTimestamps()[index]);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetDeleteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(", "));
        }
        builder.AppendFormat("%v", row.BeginDeleteTimestamps()[index]);
    }
    builder.AppendChar(']');
    return builder.Flush();
}

Stroka ToString(const TVersionedOwningRow& row)
{
    return ToString(row.Get());
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowBuilder::TVersionedRowBuilder(TRowBuffer* buffer)
    : Buffer_(buffer)
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

TVersionedRow TVersionedRowBuilder::FinishRow()
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
                return true;
            }
            if (lhs.Timestamp > rhs.Timestamp) {
                return false;
            }
            return false;
        });

    std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), std::greater<TTimestamp>());
    WriteTimestamps_.erase(
        std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
        WriteTimestamps_.end());

    std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), std::greater<TTimestamp>());
    DeleteTimestamps_.erase(
        std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
        DeleteTimestamps_.end());

    auto row = TVersionedRow::Allocate(
        Buffer_->GetAlignedPool(), Keys_.size(), 
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

    size_t fixedSize = GetVersionedRowDataSize(
        other.GetKeyCount(),
        other.GetValueCount(),
        other.GetWriteTimestampCount(),
        other.GetDeleteTimestampCount());

    size_t variableSize = 0;
    auto adjustVariableSize = [&] (const TUnversionedValue& value) {
        if (IsStringLikeType(EValueType(value.Type))) {
            variableSize += value.Length;
        }
    };

    for (int index = 0; index < other.GetKeyCount(); ++index) {
        adjustVariableSize(other.BeginKeys()[index]);
    }
    for (int index = 0; index < other.GetValueCount(); ++index) {
        adjustVariableSize(other.BeginValues()[index]);
    }

    Data_ = TSharedRef::Allocate(fixedSize + variableSize, false);

    *GetHeader() = *other.GetHeader();

    ::memcpy(BeginKeys(), other.BeginKeys(), sizeof (TUnversionedValue) * other.GetKeyCount());
    ::memcpy(BeginValues(), other.BeginValues(), sizeof (TVersionedValue) * other.GetValueCount());
    ::memcpy(BeginWriteTimestamps(), other.BeginWriteTimestamps(), sizeof (TTimestamp) * other.GetWriteTimestampCount());
    ::memcpy(BeginDeleteTimestamps(), other.BeginDeleteTimestamps(), sizeof (TTimestamp) * other.GetDeleteTimestampCount());

    if (variableSize > 0) {
        char* current = Data_.Begin() + fixedSize;
        auto captureValue = [&] (TUnversionedValue* value) {
            if (IsStringLikeType(EValueType(value->Type))) {
                ::memcpy(current, value->Data.String, value->Length);
                value->Data.String = current;
                current += value->Length;
            }
        };

        for (int index = 0; index < other.GetKeyCount(); ++index) {
            captureValue(other.BeginKeys() + index);
        }
        for (int index = 0; index < other.GetValueCount(); ++index) {
            captureValue(other.BeginValues() + index);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
