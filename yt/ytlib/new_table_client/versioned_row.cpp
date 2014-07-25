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
    result += ReadVarUInt64(input + result, &value->Timestamp);
    return result;
}

int WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUInt64(output + result, value.Timestamp);
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

size_t GetVersionedRowDataSize(int keyCount, int valueCount, int timestampCount)
{
    return sizeof(TVersionedRowHeader) +
        sizeof(TVersionedValue) * valueCount +
        sizeof(TUnversionedValue) * keyCount +
        sizeof(TTimestamp) * timestampCount;
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

    result += row.GetTimestampCount() * sizeof(TTimestamp);

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

    if (lhs.GetTimestampCount() != rhs.GetTimestampCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetTimestampCount(); ++i) {
        // TODO(babenko): get rid of IncrementalTimestampMask
        if ((lhs.BeginTimestamps()[i] & ~IncrementalTimestampMask) != (rhs.BeginTimestamps()[i] & ~IncrementalTimestampMask)) {
            return false;
        }
    }

    return true;
}

bool operator != (TVersionedRow lhs, TVersionedRow rhs)
{
    return !(lhs == rhs);
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
    // TODO(babenko): get rid of IncrementalTimestampMask
    Timestamps_.push_back(value.Timestamp | IncrementalTimestampMask);
    YASSERT((value.Timestamp & TimestampValueMask) == value.Timestamp);
    Values_.push_back(Buffer_->Capture(value));
}

void TVersionedRowBuilder::AddDeleteTimestamp(TTimestamp timestamp)
{
    YASSERT((timestamp & TimestampValueMask) == timestamp);
    Timestamps_.push_back(timestamp | TombstoneTimestampMask);
}

TVersionedRow TVersionedRowBuilder::GetRowAndReset()
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

    std::sort(
        Timestamps_.begin(),
        Timestamps_.end(),
        [] (TTimestamp lhs, TTimestamp rhs) {
            return (lhs & TimestampValueMask) < (rhs & TimestampValueMask);
        });

    Timestamps_.erase(
        std::unique(Timestamps_.begin(), Timestamps_.end()),
        Timestamps_.end());

    auto row = TVersionedRow::Allocate(Buffer_->GetAlignedPool(), Keys_.size(), Values_.size(), Timestamps_.size());
    memcpy(row.BeginKeys(), Keys_.data(), sizeof (TUnversionedValue) * Keys_.size());
    memcpy(row.BeginValues(), Values_.data(), sizeof (TVersionedValue)* Values_.size());
    memcpy(row.BeginTimestamps(), Timestamps_.data(), sizeof (TTimestamp) * Timestamps_.size());

    Keys_.clear();
    Values_.clear();
    Timestamps_.clear();

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
        other.GetTimestampCount());

    size_t variableSize = 0;
    auto adjustVariableSize = [&] (const TUnversionedValue& value) {
        if (value.Type == EValueType::String || value.Type == EValueType::Any) {
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
    ::memcpy(BeginTimestamps(), other.BeginTimestamps(), sizeof (TTimestamp) * other.GetTimestampCount());

    if (variableSize > 0) {
        char* current = Data_.Begin() + fixedSize;
        auto captureValue = [&] (TUnversionedValue* value) {
            if (value->Type == EValueType::String || value->Type == EValueType::Any) {
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
