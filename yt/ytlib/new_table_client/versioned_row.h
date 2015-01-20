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

inline TVersionedValue MakeVersionedInt64Value(i64 value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeInt64Value<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedUint64Value(ui64 value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeUint64Value<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedDoubleValue(double value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeDoubleValue<TVersionedValue>(value, id);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedBooleanValue(bool value, TTimestamp timestamp, int id = 0)
{
    auto result = MakeBooleanValue<TVersionedValue>(value, id);
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

struct TVersionedRowHeader
{
    ui16 ValueCount;
    ui16 KeyCount;
    ui16 WriteTimestampCount;
    ui16 DeleteTimestampCount;
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

size_t GetVersionedRowDataSize(
    int keyCount,
    int valueCount,
    int writeTimestampCount,
    int deleteTimestampCount);

//! A row with versioned data.
/*!
 *  A lightweight wrapper around |TVersionedRowHeader*|.
 *
 *  Provides access to the following parts:
 *  1) write timestamps, sorted in decreasing order;
 *     at most one if a specific revision is requested;
 *  2) delete timestamps, sorted in decreasing order;
 *     at most one if a specific revision is requested;
 *  3) unversioned keys;
 *  4) versioned values, sorted in ascending order by |(id,timestamp)|;
 *     no position-to-id matching is ever assumed.
 *
 *  Memory layout:
 *  1) TVersionedRowHeader
 *  2) TTimestamp per each write timestamp (#TVersionedRowHeader::WriteTimestampCount)
 *  3) TTimestamp per each delete timestamp (#TVersionedRowHeader::DeleteTimestampCount)
 *  4) TUnversionedValue per each key (#TVersionedRowHeader::KeyCount)
 *  5) TVersionedValue per each value (#TVersionedRowHeader::ValueCount)
 */
class TVersionedRow
{
public:
    TVersionedRow()
        : Header_(nullptr)
    { }

    explicit TVersionedRow(TVersionedRowHeader* header)
        : Header_(header)
    { }

    static TVersionedRow Allocate(
        TChunkedMemoryPool* pool, 
        int keyCount,
        int valueCount,
        int writeTimestampCount,
        int deleteTimestampCount)
    {
        auto* header = reinterpret_cast<TVersionedRowHeader*>(pool->AllocateAligned(
            GetVersionedRowDataSize(
                keyCount,
                valueCount,
                writeTimestampCount,
                deleteTimestampCount)));
        header->KeyCount = keyCount;
        header->ValueCount = valueCount;
        header->WriteTimestampCount = writeTimestampCount;
        header->DeleteTimestampCount = deleteTimestampCount;
        return TVersionedRow(header);
    }

    explicit operator bool()
    {
        return Header_ != nullptr;
    }

    TVersionedRowHeader* GetHeader()
    {
        return Header_;
    }

    const TVersionedRowHeader* GetHeader() const
    {
        return Header_;
    }

    const TTimestamp* BeginWriteTimestamps() const
    {
        return reinterpret_cast<const TTimestamp*>(Header_ + 1);
    }

    TTimestamp* BeginWriteTimestamps()
    {
        return reinterpret_cast<TTimestamp*>(Header_ + 1);
    }

    const TTimestamp* EndWriteTimestamps() const
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    TTimestamp* EndWriteTimestamps()
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    const TTimestamp* BeginDeleteTimestamps() const
    {
        return EndWriteTimestamps();
    }

    TTimestamp* BeginDeleteTimestamps()
    {
        return EndWriteTimestamps();
    }

    const TTimestamp* EndDeleteTimestamps() const
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    TTimestamp* EndDeleteTimestamps()
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return reinterpret_cast<const TUnversionedValue*>(EndDeleteTimestamps());
    }

    TUnversionedValue* BeginKeys()
    {
        return reinterpret_cast<TUnversionedValue*>(EndDeleteTimestamps());
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
        return Header_->KeyCount;
    }

    int GetValueCount() const
    {
        return Header_->ValueCount;
    }

    int GetWriteTimestampCount() const
    {
        return Header_->WriteTimestampCount;
    }

    int GetDeleteTimestampCount() const
    {
        return Header_->DeleteTimestampCount;
    }

    TTimestamp GetLatestTimestamp() const
    {
        auto deleteTimestamp = GetDeleteTimestampCount() == 0 ? NullTimestamp : EndDeleteTimestamps()[-1];
        auto writeTimestamp = GetWriteTimestampCount() == 0 ? NullTimestamp : EndWriteTimestamps()[-1];
        return std::max(deleteTimestamp, writeTimestamp);
    }

private:
    TVersionedRowHeader* Header_;

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

Stroka ToString(TVersionedRow row);
Stroka ToString(const TVersionedOwningRow& row);

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

    TVersionedRow FinishRow();

private:
    TRowBuffer* Buffer_;

    std::vector<TUnversionedValue> Keys_;
    std::vector<TVersionedValue> Values_;
    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

};

////////////////////////////////////////////////////////////////////////////////

//! An immutable owning version of TVersionedRow.
/*!
 *  Instances of TVersionedOwningRow are lightweight ref-counted handles.
 *  All data is stored in a (shared) blob.
 */
class TVersionedOwningRow
{
public:
    TVersionedOwningRow()
    { }

    explicit TVersionedOwningRow(TVersionedRow other);

    TVersionedOwningRow(const TVersionedOwningRow& other)
        : Data_(other.Data_)
    { }

    TVersionedOwningRow(TVersionedOwningRow&& other)
        : Data_(std::move(other.Data_))
    { }

    explicit operator bool() const
    {
        return static_cast<bool>(Data_);
    }

    TVersionedRow Get() const
    {
        return TVersionedRow(const_cast<TVersionedOwningRow*>(this)->GetHeader());
    }

    const TVersionedRowHeader* GetHeader() const
    {
        return Data_ ? reinterpret_cast<const TVersionedRowHeader*>(Data_.Begin()) : nullptr;
    }

    const TTimestamp* BeginWriteTimestamps() const
    {
        return reinterpret_cast<const TTimestamp*>(GetHeader() + 1);
    }

    TTimestamp* BeginWriteTimestamps()
    {
        return reinterpret_cast<TTimestamp*>(GetHeader() + 1);
    }

    const TTimestamp* EndWriteTimestamps() const
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    TTimestamp* EndWriteTimestamps()
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    const TTimestamp* BeginDeleteTimestamps() const
    {
        return EndWriteTimestamps();
    }

    TTimestamp* BeginDeleteTimestamps()
    {
        return EndWriteTimestamps();
    }

    const TTimestamp* EndDeleteTimestamps() const
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    TTimestamp* EndDeleteTimestamps()
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return reinterpret_cast<const TUnversionedValue*>(EndDeleteTimestamps());
    }

    TUnversionedValue* BeginKeys()
    {
        return reinterpret_cast<TUnversionedValue*>(EndDeleteTimestamps());
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
        return GetHeader()->KeyCount;
    }

    int GetValueCount() const
    {
        return GetHeader()->ValueCount;
    }

    int GetWriteTimestampCount() const
    {
        return GetHeader()->WriteTimestampCount;
    }

    int GetDeleteTimestampCount() const
    {
        return GetHeader()->DeleteTimestampCount;
    }


    friend void swap(TVersionedOwningRow& lhs, TVersionedOwningRow& rhs)
    {
        using std::swap;
        swap(lhs.Data_, rhs.Data_);
    }

    TVersionedOwningRow& operator=(const TVersionedOwningRow& other)
    {
        Data_ = other.Data_;
        return *this;
    }

    TVersionedOwningRow& operator=(TVersionedOwningRow&& other)
    {
        Data_ = std::move(other.Data_);
        return *this;
    }

private:
    TSharedRef Data_;

    TVersionedRowHeader* GetHeader()
    {
        return Data_ ? reinterpret_cast<TVersionedRowHeader*>(Data_.Begin()) : nullptr;
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
