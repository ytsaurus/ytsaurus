#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTableClient {

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

inline TVersionedValue MakeVersionedSentinelValue(EValueType type, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeSentinelValue<TVersionedValue>(type, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedInt64Value(i64 value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeInt64Value<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedUint64Value(ui64 value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeUint64Value<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedDoubleValue(double value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeDoubleValue<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedBooleanValue(bool value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeBooleanValue<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedStringValue(const TStringBuf& value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeStringValue<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedAnyValue(const TStringBuf& value, TTimestamp timestamp, int id = 0, bool aggregate = false)
{
    auto result = MakeAnyValue<TVersionedValue>(value, id, aggregate);
    result.Timestamp = timestamp;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TVersionedRowHeader
{
    ui32 ValueCount;
    ui32 KeyCount;
    ui32 WriteTimestampCount;
    ui32 DeleteTimestampCount;
};

static_assert(
    sizeof(TVersionedRowHeader) == 16,
    "TVersionedRowHeader has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

size_t GetByteSize(const TVersionedValue& value);
size_t GetDataWeight(const TVersionedValue& value);

size_t ReadValue(const char* input, TVersionedValue* value);
size_t WriteValue(char* output, const TVersionedValue& value);

TString ToString(const TVersionedValue& value);

void Save(TStreamSaveContext& context, const TVersionedValue& value);
void Load(TStreamLoadContext& context, TVersionedValue& value, TChunkedMemoryPool* pool);

////////////////////////////////////////////////////////////////////////////////

//! Returns the number of bytes needed to store a versioned row (not including string data).
size_t GetVersionedRowByteSize(
    int keyCount,
    int valueCount,
    int writeTimestampCount,
    int deleteTimestampCount);

//! A row with versioned data.
/*!
 *  A lightweight wrapper around |TVersionedRowHeader*|.
 *
 *  Provides access to the following parts:
 *  1) write timestamps, sorted in descending order;
 *     at most one if a specific revision is requested;
 *  2) delete timestamps, sorted in descending order;
 *     at most one if a specific revision is requested;
 *  3) unversioned keys;
 *  4) versioned values, sorted by |id| (in ascending order) and then by |timestamp| (in descending order);
 *     note that no position-to-id matching is ever assumed.
 *
 *  The order of values described in 4) is typically referred to as "canonical".
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
    TVersionedRow() = default;

    explicit TVersionedRow(const TVersionedRowHeader* header)
        : Header_(header)
    { }

    explicit TVersionedRow(TTypeErasedRow erased)
        : Header_(reinterpret_cast<const TVersionedRowHeader*>(erased.OpaqueHeader))
    { }

    explicit operator bool()
    {
        return Header_ != nullptr;
    }

    TTypeErasedRow ToTypeErasedRow() const
    {
        return {Header_};
    }

    const TVersionedRowHeader* GetHeader() const
    {
        return Header_;
    }

    const TTimestamp* BeginWriteTimestamps() const
    {
        return reinterpret_cast<const TTimestamp*>(Header_ + 1);
    }

    const TTimestamp* EndWriteTimestamps() const
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    const TTimestamp* BeginDeleteTimestamps() const
    {
        return EndWriteTimestamps();
    }

    const TTimestamp* EndDeleteTimestamps() const
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return reinterpret_cast<const TUnversionedValue*>(EndDeleteTimestamps());
    }

    const TUnversionedValue* EndKeys() const
    {
        return BeginKeys() + GetKeyCount();
    }

    const TVersionedValue* BeginValues() const
    {
        return reinterpret_cast<const TVersionedValue*>(EndKeys());
    }

    const TVersionedValue* EndValues() const
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

    const char* GetMemoryBegin() const
    {
        return reinterpret_cast<const char*>(Header_);
    }

    const char* GetMemoryEnd() const
    {
        return GetMemoryBegin() + GetVersionedRowByteSize(
            GetKeyCount(),
            GetValueCount(),
            GetWriteTimestampCount(),
            GetDeleteTimestampCount());
    }

private:
    const TVersionedRowHeader* Header_ = nullptr;
};

static_assert(
    sizeof(TVersionedRow) == sizeof(intptr_t),
    "TVersionedRow size must match that of a pointer.");

size_t GetDataWeight(TVersionedRow row);
size_t GetHash(TVersionedRow row);

//! Compares versioned rows for equality taking aggregate flags into account.
//! Row values must be canonically sorted.
bool AreRowsIdentical(TVersionedRow lhs, TVersionedRow rhs);

TString ToString(TVersionedRow row);
TString ToString(TMutableVersionedRow row);
TString ToString(const TVersionedOwningRow& row);

//! Checks that #row is a valid client-side versioned data row. Throws on failure.
/*!
 *  Value ids in the row are first mapped via #idMapping.
 *  The row must obey the following properties:
 *  1. Its value count must pass #ValidateRowValueCount checks.
 *  2. Its key count must match the number of keys in #schema.
 *  3. Name table must contain all key columns in the same order as in the schema.
 *  4. Write and delete timestamps must pass #ValidateWriteTimestamp test and must be decreasing.
 *  5. Value part must not contain key components.
 *  6. Value types must either be null or match those given in #schema.
 *  7. For values marked with #TUnversionedValue::Aggregate flag, the corresponding columns in #schema must
 *  be aggregating.
 */
void ValidateClientDataRow(
    TVersionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable);

TOwningKey RowToKey(TVersionedRow row);

////////////////////////////////////////////////////////////////////////////////

//! A variant of TVersionedRow that enables mutating access to its content.
class TMutableVersionedRow
    : public TVersionedRow
{
public:
    TMutableVersionedRow() = default;

    explicit TMutableVersionedRow(TVersionedRowHeader* header)
        : TVersionedRow(header)
    { }

    explicit TMutableVersionedRow(TTypeErasedRow row)
        : TVersionedRow(reinterpret_cast<const TVersionedRowHeader*>(row.OpaqueHeader))
    { }

    static TMutableVersionedRow Allocate(
        TChunkedMemoryPool* pool,
        int keyCount,
        int valueCount,
        int writeTimestampCount,
        int deleteTimestampCount)
    {
        size_t byteSize = GetVersionedRowByteSize(
            keyCount,
            valueCount,
            writeTimestampCount,
            deleteTimestampCount);
        auto* header = reinterpret_cast<TVersionedRowHeader*>(pool->AllocateAligned(byteSize));
        header->KeyCount = keyCount;
        header->ValueCount = valueCount;
        header->WriteTimestampCount = writeTimestampCount;
        header->DeleteTimestampCount = deleteTimestampCount;
        return TMutableVersionedRow(header);
    }

    TVersionedRowHeader* GetHeader()
    {
        return const_cast<TVersionedRowHeader*>(TVersionedRow::GetHeader());
    }

    TTimestamp* BeginWriteTimestamps()
    {
        return reinterpret_cast<TTimestamp*>(GetHeader() + 1);
    }

    TTimestamp* EndWriteTimestamps()
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    TTimestamp* BeginDeleteTimestamps()
    {
        return EndWriteTimestamps();
    }

    TTimestamp* EndDeleteTimestamps()
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    TUnversionedValue* BeginKeys()
    {
        return reinterpret_cast<TUnversionedValue*>(EndDeleteTimestamps());
    }

    TUnversionedValue* EndKeys()
    {
        return BeginKeys() + GetKeyCount();
    }

    TVersionedValue* BeginValues()
    {
        return reinterpret_cast<TVersionedValue*>(EndKeys());
    }

    TVersionedValue* EndValues()
    {
        return BeginValues() + GetValueCount();
    }

    void SetKeyCount(int count)
    {
        GetHeader()->KeyCount = count;
    }

    void SetValueCount(int count)
    {
        GetHeader()->ValueCount = count;
    }

    void SetWriteTimestampCount(int count)
    {
        GetHeader()->WriteTimestampCount = count;
    }

    void SetDeleteTimestampCount(int count)
    {
        GetHeader()->DeleteTimestampCount = count;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TVersionedRow instances.
/*!
 *  Not very efficient, only useful in tests.
 *  The resulting row is canonically ordered.
 */
class TVersionedRowBuilder
{
public:
    /*!
     *  \param compaction - if unset, builder creates only one, latest write timestamp.
     */
    explicit TVersionedRowBuilder(TRowBufferPtr buffer, bool compaction = true);

    void AddKey(const TUnversionedValue& value);
    void AddValue(const TVersionedValue& value);
    void AddDeleteTimestamp(TTimestamp timestamp);

    // Sometimes versioned row have write timestamps without corresponding values,
    // when reading with column filter.
    void AddWriteTimestamp(TTimestamp timestamp);

    TMutableVersionedRow FinishRow();

private:
    const TRowBufferPtr Buffer_;
    const bool Compaction_;

    std::vector<TUnversionedValue> Keys_;
    std::vector<TVersionedValue> Values_;
    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

};

////////////////////////////////////////////////////////////////////////////////

//! An owning variant of TVersionedRow.
/*!
 *  Instances of TVersionedOwningRow are lightweight handles.
 *  All data is stored in shared ref-counted blobs.
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
        return TVersionedRow(GetHeader());
    }

    operator TVersionedRow() const
    {
        return Get();
    }

    const TTimestamp* BeginWriteTimestamps() const
    {
        return reinterpret_cast<const TTimestamp*>(GetHeader() + 1);
    }

    const TTimestamp* EndWriteTimestamps() const
    {
        return BeginWriteTimestamps() + GetWriteTimestampCount();
    }

    const TTimestamp* BeginDeleteTimestamps() const
    {
        return EndWriteTimestamps();
    }

    const TTimestamp* EndDeleteTimestamps() const
    {
        return BeginDeleteTimestamps() + GetDeleteTimestampCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return reinterpret_cast<const TUnversionedValue*>(EndDeleteTimestamps());
    }

    const TUnversionedValue* EndKeys() const
    {
        return BeginKeys() + GetKeyCount();
    }

    const TVersionedValue* BeginValues() const
    {
        return reinterpret_cast<const TVersionedValue*>(EndKeys());
    }

    const TVersionedValue* EndValues() const
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

    size_t GetByteSize() const
    {
        return Data_.Size();
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
    TSharedMutableRef Data_;


    const TVersionedRowHeader* GetHeader() const
    {
        return Data_ ? reinterpret_cast<const TVersionedRowHeader*>(Data_.Begin()) : nullptr;
    }

    TVersionedRowHeader* GetMutableHeader()
    {
        return Data_ ? reinterpret_cast<TVersionedRowHeader*>(Data_.Begin()) : nullptr;
    }

    TUnversionedValue* BeginMutableKeys()
    {
        return const_cast<TUnversionedValue*>(BeginKeys());
    }

    TVersionedValue* BeginMutableValues()
    {
        return const_cast<TVersionedValue*>(BeginValues());
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
