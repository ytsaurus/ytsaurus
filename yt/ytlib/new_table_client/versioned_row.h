#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <core/misc/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TVersionedValue
    : public TUnversionedValue
{
    TTimestamp Timestamp;
};

static_assert(
    sizeof(TVersionedValue) == 24,
    "TVersionedValue has to be exactly 24 bytes.");

////////////////////////////////////////////////////////////////////////////////

inline TVersionedValue MakeVersionedValue(const TUnversionedValue& value, TTimestamp timestamp)
{
    TVersionedValue result;
    static_cast<TUnversionedValue&>(result) = value;
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedSentinelValue(EValueType type, TTimestamp timestamp, int id = 0)
{
    auto result = MakeSentinelValue<TVersionedValue>(type, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedIntegerValue(i64 value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeInt64Value<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedDoubleValue(double value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeDoubleValue<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedStringValue(const TStringBuf& value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeStringValue<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedAnyValue(const TStringBuf& value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeAnyValue<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! Header which precedes row values in memory layout.
struct TVersionedRowHeader
{
    ui32 ValueCount;
    ui16 KeyCount;
    ui16 TimestampCount;
};

static_assert(
    sizeof(TVersionedRowHeader) == 8,
    "TVersionedRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TVersionedValue& value);
int GetDataWeight(const TVersionedValue& value);

int ReadValue(const char* input, TVersionedValue* value);
int WriteValue(char* output, const TVersionedValue& value);

Stroka ToString(const TVersionedValue& value);

void Save(TStreamSaveContext& context, const TVersionedValue& value);
void Load(TStreamLoadContext& context, TVersionedValue& value, TChunkedMemoryPool* pool);

////////////////////////////////////////////////////////////////////////////////

size_t GetVersionedRowDataSize(int keyCount, int valueCount, int timestampCount = 1);

class TVersionedRow
{
public:
    TVersionedRow()
        : Header(nullptr)
    { }

    explicit TVersionedRow(TVersionedRowHeader* header)
        : Header(header)
    { }

    static TVersionedRow Allocate(
        TChunkedMemoryPool* pool, 
        int keyCount,
        int valueCount,
        int timestampCount)
    {
        auto* header = reinterpret_cast<TVersionedRowHeader*>(pool->Allocate(GetVersionedRowDataSize(
            keyCount,
            valueCount,
            timestampCount)));
        header->KeyCount = keyCount;
        header->ValueCount = valueCount;
        header->TimestampCount = timestampCount;
        return TVersionedRow(header);
    }

    explicit operator bool()
    {
        return Header != nullptr;
    }

    TVersionedRowHeader* GetHeader()
    {
        return Header;
    }

    const TVersionedRowHeader* GetHeader() const
    {
        return Header;
    }

    const TTimestamp* BeginTimestamps() const
    {
        return reinterpret_cast<const TTimestamp*>(Header + 1);
    }

    TTimestamp* BeginTimestamps()
    {
        return reinterpret_cast<TTimestamp*>(Header + 1);
    }

    const TTimestamp* EndTimestamps() const
    {
        return BeginTimestamps() + GetTimestampCount();
    }

    TTimestamp* EndTimestamps()
    {
        return BeginTimestamps() + GetTimestampCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return reinterpret_cast<const TUnversionedValue*>(EndTimestamps());
    }

    TUnversionedValue* BeginKeys()
    {
        return reinterpret_cast<TUnversionedValue*>(EndTimestamps());
    }

    const TUnversionedValue* EndKeys() const
    {
        return BeginKeys() + GetKeyCount();
    }

    TUnversionedValue* EndKeys()
    {
        return BeginKeys() + GetKeyCount();
    }

    const TVersionedValue* BeginValues() const
    {
        return reinterpret_cast<const TVersionedValue*>(EndKeys());
    }

    TVersionedValue* BeginValues()
    {
        return reinterpret_cast<TVersionedValue*>(EndKeys());
    }

    const TVersionedValue* EndValues() const
    {
        return BeginValues() + GetValueCount();
    }

    TVersionedValue* EndValues()
    {
        return BeginValues() + GetValueCount();
    }

    int GetKeyCount() const
    {
        return Header->KeyCount;
    }

    int GetValueCount() const
    {
        return Header->ValueCount;
    }

    int GetTimestampCount() const
    {
        return Header->TimestampCount;
    }

private:
    TVersionedRowHeader* Header;

};

static_assert(
    sizeof(TVersionedRow) == sizeof(intptr_t),
    "TVersionedRow size must match that of a pointer.");

i64 GetDataWeight(TVersionedRow row);
size_t GetHash(TVersionedRow row);

//! Compares versioned rows for equality.
//! Row values must be canonically sorted.
bool operator == (TVersionedRow lhs, TVersionedRow rhs);

//! Compares versioned rows for nonequality.
bool operator != (TVersionedRow lhs, TVersionedRow rhs);

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TVersionedRow instances.
//! Not very efficient, only useful in tests.
class TVersionedRowBuilder
{
public:
    explicit TVersionedRowBuilder(TRowBuffer* buffer);

    void AddKey(const TUnversionedValue& value);
    void AddValue(const TVersionedValue& value);
    void AddDeleteTimestamp(TTimestamp timestamp);

    TVersionedRow GetRowAndReset();

private:
    TRowBuffer* Buffer_;

    std::vector<TUnversionedValue> Keys_;
    std::vector<TVersionedValue> Values_;
    std::vector<TTimestamp> Timestamps_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
