#include "stdafx.h"
#include <numeric>
#include "versioned_row.h"

#include <core/misc/format.h>

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

int WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUInt64(output + result, value.Timestamp);
    return result;
}

int ReadValue(const char* input, TVersionedValue* value)
{
    int result = ReadValue(input, static_cast<TUnversionedValue*>(value));
    result += ReadVarUInt64(input + result, &value->Timestamp);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

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

Stroka ToString(const TVersionedValue& value)
{
    return Format("%v@%v",
        static_cast<TUnversionedValue>(value),
        value.Timestamp);
}

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

////////////////////////////////////////////////////////////////////////////////

void TVersionedRowBuilder::AddKey(const TUnversionedValue& value)
{
    Keys_.push_back(value);
}

void TVersionedRowBuilder::AddValue(const TVersionedValue& value)
{
    Timestamps_.push_back(value.Timestamp);
    YASSERT((value.Timestamp & TimestampValueMask) == value.Timestamp);

    auto valueCopy = value;
    if (value.Type == EValueType::String || value.Type == EValueType::Any) {
        valueCopy.Data.String = Pool_.AllocateUnaligned(value.Length);
        memcpy(const_cast<char*>(valueCopy.Data.String), value.Data.String, value.Length);
    }

    Values_.push_back(valueCopy);
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
            if (lhs.Timestamp > rhs.Timestamp) {
                return true;
            }
            if (lhs.Timestamp < rhs.Timestamp) {
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

    auto row = TVersionedRow::Allocate(&Pool_, Keys_.size(), Values_.size(), Timestamps_.size());
    memcpy(row.BeginKeys(), Keys_.data(), sizeof (TUnversionedValue) * Keys_.size());
    memcpy(row.BeginValues(), Values_.data(), sizeof (TVersionedValue)* Values_.size());
    memcpy(row.BeginTimestamps(), Timestamps_.data(), sizeof (TTimestamp) * Timestamps_.size());

    Keys_.clear();
    Values_.clear();
    Timestamps_.clear();

    return row;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
