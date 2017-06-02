#include "versioned_row.h"
#include "row_buffer.h"

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
    return Format("%v@%v",
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

TString ToString(TVersionedRow row)
{
    if (!row) {
        return "<Null>";
    }

    TStringBuilder builder;
    builder.AppendChar('[');
    for (int index = 0; index < row.GetKeyCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(","));
        }
        const auto& value = row.BeginKeys()[index];
        builder.AppendString(ToString(value));
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetValueCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(","));
        }
        const auto& value = row.BeginValues()[index];
        builder.AppendFormat("%v#%v",
            value.Id,
            value);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetWriteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(","));
        }
        builder.AppendFormat("%v", row.BeginWriteTimestamps()[index]);
    }
    builder.AppendChar('|');
    for (int index = 0; index < row.GetDeleteTimestampCount(); ++index) {
        if (index > 0) {
            builder.AppendString(STRINGBUF(","));
        }
        builder.AppendFormat("%v", row.BeginDeleteTimestamps()[index]);
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

    auto row = TMutableVersionedRow::Allocate(
        Buffer_->GetPool(),
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
