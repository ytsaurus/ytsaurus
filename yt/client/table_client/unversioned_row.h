#pragma once

#include "public.h"
#include "row_base.h"
#include "schema.h"
#include "unversioned_value.h"

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/varint.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TUnversionedOwningValue
{
public:
    TUnversionedOwningValue() = default;

    TUnversionedOwningValue(TUnversionedOwningValue&& other)
    {
        std::swap(Value_, other.Value_);
    }

    TUnversionedOwningValue(const TUnversionedOwningValue& other)
    {
        Assign(other.Value_);
    }

    TUnversionedOwningValue(const TUnversionedValue& other)
    {
        Assign(other);
    }

    ~TUnversionedOwningValue()
    {
        Clear();
    }

    operator TUnversionedValue() const
    {
        return Value_;
    }

    TUnversionedOwningValue& operator = (TUnversionedOwningValue&& other)
    {
        std::swap(Value_, other.Value_);
        return *this;
    }

    TUnversionedOwningValue& operator = (const TUnversionedOwningValue& other)
    {
        Clear();
        Assign(other.Value_);
        return *this;
    }

    TUnversionedOwningValue& operator = (const TUnversionedValue& other)
    {
        Clear();
        Assign(other);
        return *this;
    }

    void Clear()
    {
        if (Value_.Type == EValueType::Any || Value_.Type == EValueType::String) {
            delete [] Value_.Data.String;
        }
        Value_.Type = EValueType::TheBottom;
        Value_.Length = 0;
    }

private:
    TUnversionedValue Value_ = {0, EValueType::TheBottom, false, 0, {0}};

    void Assign(const TUnversionedValue& other)
    {
        Value_ = other;
        if (Value_.Type == EValueType::Any || Value_.Type == EValueType::String) {
            auto newString = new char[Value_.Length];
            ::memcpy(newString, Value_.Data.String, Value_.Length);
            Value_.Data.String = newString;
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

inline TUnversionedValue MakeUnversionedSentinelValue(EValueType type, int id = 0, bool aggregate = false)
{
    return MakeSentinelValue<TUnversionedValue>(type, id, aggregate);
}

inline TUnversionedValue MakeUnversionedNullValue(int id = 0, bool aggregate = false)
{
    return MakeNullValue<TUnversionedValue>(id, aggregate);
}

inline TUnversionedValue MakeUnversionedInt64Value(i64 value, int id = 0, bool aggregate = false)
{
    return MakeInt64Value<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedUint64Value(ui64 value, int id = 0, bool aggregate = false)
{
    return MakeUint64Value<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedDoubleValue(double value, int id = 0, bool aggregate = false)
{
    return MakeDoubleValue<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedBooleanValue(bool value, int id = 0, bool aggregate = false)
{
    return MakeBooleanValue<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedStringValue(TStringBuf value, int id = 0, bool aggregate = false)
{
    return MakeStringValue<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedAnyValue(TStringBuf value, int id = 0, bool aggregate = false)
{
    return MakeAnyValue<TUnversionedValue>(value, id, aggregate);
}

inline TUnversionedValue MakeUnversionedValueHeader(EValueType type, int id = 0, bool aggregate = false)
{
    return MakeSentinelValue<TUnversionedValue>(type, id, aggregate);
}

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedRowHeader
{
    ui32 Count;
    ui32 Capacity;
};

static_assert(
    sizeof(TUnversionedRowHeader) == 8,
    "TUnversionedRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

inline size_t GetDataWeight(const EValueType type)
{
    switch (type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return 0;

        case EValueType::Int64:
            return sizeof(i64);

        case EValueType::Uint64:
            return sizeof(ui64);

        case EValueType::Double:
            return sizeof(double);

        case EValueType::Boolean:
            return 1;

        default:
            Y_UNREACHABLE();
    }
}

inline size_t GetDataWeight(const TUnversionedValue& value)
{
    if (IsStringLikeType(value.Type)) {
        return value.Length;
    } else {
        return GetDataWeight(value.Type);
    }
}

size_t GetByteSize(const TUnversionedValue& value);
size_t WriteValue(char* output, const TUnversionedValue& value);
size_t ReadValue(const char* input, TUnversionedValue* value);

void Save(TStreamSaveContext& context, const TUnversionedValue& value);
void Load(TStreamLoadContext& context, TUnversionedValue& value, TChunkedMemoryPool* pool);

TString ToString(const TUnversionedValue& value);

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
//! Note that this ignores aggregate flags.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Derived comparison operators.
//! Note that these ignore aggregate flags.
bool operator == (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator != (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator <= (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator <  (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator >= (const TUnversionedValue& lhs, const TUnversionedValue& rhs);
bool operator >  (const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Similar to operator == but takes aggregate flags into account.
bool AreRowValuesIdentical(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Ternary comparison predicate for ranges of TUnversionedValue-s.
//! Note that this ignores aggregate flags.
int CompareRows(
    const TUnversionedValue* lhsBegin,
    const TUnversionedValue* lhsEnd,
    const TUnversionedValue* rhsBegin,
    const TUnversionedValue* rhsEnd);

//! Ternary comparison predicate for TUnversionedRow-s stripped to a given number of
//! (leading) values.
//! Note that this ignores aggregate flags.
int CompareRows(
    TUnversionedRow lhs,
    TUnversionedRow rhs,
    ui32 prefixLength = std::numeric_limits<ui32>::max());

//! Derived comparison operators.
//! Note that these ignore aggregate flags.
bool operator == (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator != (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator <= (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator <  (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator >= (TUnversionedRow lhs, TUnversionedRow rhs);
bool operator >  (TUnversionedRow lhs, TUnversionedRow rhs);

//! Similar to operator == but takes aggregate flags into account.
bool AreRowsIdentical(TUnversionedRow lhs, TUnversionedRow rhs);

//! Sets all value types of |row| to |EValueType::Null|. Ids are not changed.
void ResetRowValues(TMutableUnversionedRow* row);

//! Computes hash for a given TUnversionedRow.
ui64 GetHash(TUnversionedRow row, ui32 keyColumnCount = std::numeric_limits<ui32>::max());

//! Computes FarmHash forever-fixed fingerprint for a given TUnversionedRow.
TFingerprint GetFarmFingerprint(TUnversionedRow row, ui32 keyColumnCount = std::numeric_limits<ui32>::max());

//! Returns the number of bytes needed to store an unversioned row (not including string data).
size_t GetUnversionedRowByteSize(ui32 valueCount);

//! Returns the storage-invariant data weight of a given row.
size_t GetDataWeight(TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

//! A row with unversioned data.
/*!
 *  A lightweight wrapper around |TUnversionedRowHeader*|.
 *
 *  Provides access to a sequence of unversioned values.
 *  If data is schemaful then the positions of values must exactly match their ids.
 *
 *  Memory layout:
 *  1) TUnversionedRowHeader
 *  2) TUnversionedValue per each value (#TUnversionedRowHeader::ValueCount)
 */
class TUnversionedRow
{
public:
    TUnversionedRow() = default;

    explicit TUnversionedRow(const TUnversionedRowHeader* header)
        : Header_(header)
    { }

    explicit TUnversionedRow(TTypeErasedRow erased)
        : Header_(reinterpret_cast<const TUnversionedRowHeader*>(erased.OpaqueHeader))
    { }

    explicit operator bool() const
    {
        return Header_ != nullptr;
    }

    TTypeErasedRow ToTypeErasedRow() const
    {
        return {Header_};
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return Header_;
    }

    const TUnversionedValue* Begin() const
    {
        return reinterpret_cast<const TUnversionedValue*>(Header_ + 1);
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    const TUnversionedValue& operator[] (int index) const
    {
        Y_ASSERT(index >= 0 && static_cast<ui32>(index) < GetCount());
        return Begin()[index];
    }

    ui32 GetCount() const
    {
        return Header_->Count;
    }

    // STL interop.
    const TUnversionedValue* begin() const
    {
        return Begin();
    }

    const TUnversionedValue* end() const
    {
        return End();
    }

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    const TUnversionedRowHeader* Header_ = nullptr;

};

// For TKeyComparer.
inline ui32 GetKeyComparerValueCount(TUnversionedRow row, ui32 prefixLength)
{
    return std::min(row.GetCount(), prefixLength);
}

static_assert(
    sizeof(TUnversionedRow) == sizeof(intptr_t),
    "TUnversionedRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! Checks that #value type is compatible with the schema column type.
/*
 * If #typeAnyAcceptsAllValues is false columns of type EValueType::ANY accept only values of same type.
 * If #typeAnyAcceptsAllValues is true then columns of type EValueType::ANY accept all values.
 *
 * \note Function throws an error if column has `Any' type and value has `non-Any' type.
 */
void ValidateValueType(
    const TUnversionedValue& value,
    const TTableSchema& schema,
    int schemaId,
    bool typeAnyAcceptsAllValues);
void ValidateValueType(
    const TUnversionedValue& value,
    const TColumnSchema& columnSchema,
    bool typeAnyAcceptsAllValues);

//! Checks that #value is allowed to appear in static tables' data. Throws on failure.
void ValidateStaticValue(const TUnversionedValue& value);

//! Checks that #value is allowed to appear in dynamic tables' data. Throws on failure.
void ValidateDataValue(const TUnversionedValue& value);

//! Checks that #value is allowed to appear in dynamic tables' keys. Throws on failure.
void ValidateKeyValue(const TUnversionedValue& value);

//! Checks that #count represents an allowed number of values in a row. Throws on failure.
void ValidateRowValueCount(int count);

//! Checks that #count represents an allowed number of components in a key. Throws on failure.
void ValidateKeyColumnCount(int count);

//! Checks that #count represents an allowed number of rows in a rowset. Throws on failure.
void ValidateRowCount(int count);

//! Checks that #row is a valid client-side data row. Throws on failure.
/*!
 *  Value ids in the row are first mapped via #idMapping.
 *  The row must obey the following properties:
 *  1. Its value count must pass #ValidateRowValueCount checks.
 *  2. It must contain all key components (values with ids in range [0, #schema.GetKeyColumnCount() - 1]).
 *  3. Value types must either be null or match those given in #schema.
 *  4. For values marked with #TUnversionedValue::Aggregate flag, the corresponding columns in #schema must
 *  be aggregating.
 *  5. Versioned values must be sorted by |id| (in ascending order) and then by |timestamp| (in descending order).
 *  6. At least one non-key column must be specified.
 */
void ValidateClientDataRow(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    TNullable<int> tabletIndexColumnId = TNullable<int>());

//! Checks that #row contains no duplicate non-key columns and that all required columns are present. Skip values that map to negative ids via #idMapping.
/*! It is assumed that ValidateClientDataRow was called before. */
void ValidateDuplicateAndRequiredValueColumns(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    std::vector<bool>* columnPresenceBuffer);

//! Checks that #key is a valid client-side key. Throws on failure.
/*! The components must pass #ValidateKeyValue check. */
void ValidateClientKey(TKey key);

//! Checks that #key is a valid client-side key. Throws on failure.
/*! The key must obey the following properties:
 *  1. It cannot be null.
 *  2. It must contain exactly #schema.GetKeyColumnCount() components.
 *  3. Value ids must be a permutation of {0, ..., #schema.GetKeyColumnCount() - 1}.
 *  4. Value types must either be null of match those given in schema.
 */
void ValidateClientKey(
    TKey key,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable);

//! Checks if #timestamp is sane and can be used for data.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp] plus some sentinels
//! (SyncLastCommittedTimestamp and AsyncLastCommittedTimestamp).
void ValidateReadTimestamp(TTimestamp timestamp);

//! Checks if #timestamp is sane and can be used for replica synchronization.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp].
void ValidateSyncTimestamp(TTimestamp timestamp);

//! Checks if #timestamp is sane and can be used for writing (versioned) data.
//! Allows timestamps in range [MinTimestamp, MaxTimestamp].
void ValidateWriteTimestamp(TTimestamp timestamp);

//! An internal helper used by validators.
int ApplyIdMapping(
    const TUnversionedValue& value,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping* idMapping);

//! Returns the successor of |key|, i.e. the key obtained from |key|
//! by appending a |EValueType::Min| sentinel.
TOwningKey GetKeySuccessor(TKey key);
TKey GetKeySuccessor(TKey key, const TRowBufferPtr& rowBuffer);

//! Returns the successor of |key| trimmed to a given length, i.e. the key
//! obtained by trimming |key| to |prefixLength| and appending
//! a |EValueType::Max| sentinel.
TOwningKey GetKeyPrefixSuccessor(TKey key, ui32 prefixLength);
TKey GetKeyPrefixSuccessor(TKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer);

//! Returns key of a strict lenght (either trimmed key or widen key)
TKey GetStrictKey(TKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);
TKey GetStrictKeySuccessor(TKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! If #key has more than #prefixLength values then trims it this limit.
TOwningKey GetKeyPrefix(TKey key, ui32 prefixLength);
TKey GetKeyPrefix(TKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer);

//! Makes a new, wider key padded with given sentinel values.
TOwningKey WidenKey(const TOwningKey& key, ui32 keyColumnCount, EValueType sentinelType = EValueType::Null);
TKey WidenKey(const TKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);
TKey WidenKeySuccessor(const TKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! Takes prefix of a key and makes it wider.
TOwningKey WidenKeyPrefix(const TOwningKey& key, ui32 prefixLength, ui32 keyColumnCount, EValueType sentinelType = EValueType::Null);
TKey WidenKeyPrefix(TKey key, ui32 prefixLength, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType = EValueType::Null);

//! Returns the key with no components.
const TOwningKey EmptyKey();

//! Returns the key with a single |Min| component.
const TOwningKey MinKey();

//! Returns the key with a single |Max| component.
const TOwningKey MaxKey();

//! Compares two keys, |a| and |b|, and returns a smaller one.
//! Ties are broken in favour of the first argument.
const TOwningKey& ChooseMinKey(const TOwningKey& a, const TOwningKey& b);

//! Compares two keys, |a| and |b|, and returns a bigger one.
//! Ties are broken in favour of the first argument.
const TOwningKey& ChooseMaxKey(const TOwningKey& a, const TOwningKey& b);

TString SerializeToString(TUnversionedRow row);
TString SerializeToString(const TUnversionedValue* begin, const TUnversionedValue* end);

void ToProto(TProtoStringType* protoRow, TUnversionedRow row);
void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);
void ToProto(TProtoStringType* protoRow, const TUnversionedValue* begin, const TUnversionedValue* end);

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);
void FromProto(TUnversionedRow* row, const TProtoStringType& protoRow, const TRowBufferPtr& rowBuffer);

void Serialize(const TUnversionedValue& value, NYson::IYsonConsumer* consumer);
void Serialize(TKey key, NYson::IYsonConsumer* consumer);
void Serialize(const TOwningKey& key, NYson::IYsonConsumer* consumer);

void Deserialize(TOwningKey& key, NYTree::INodePtr node);

size_t GetYsonSize(const TUnversionedValue& value);
size_t WriteYson(char* buffer, const TUnversionedValue& unversionedValue);

TString ToString(TUnversionedRow row);
TString ToString(TMutableUnversionedRow row);
TString ToString(const TUnversionedOwningRow& row);

TOwningKey RowToKey(
    const NTableClient::TTableSchema& schema,
    TUnversionedRow row);

//! Constructs a shared range of rows from a non-shared one.
/*!
 *  The values contained in the rows are also captured.
 *  The underlying storage allocation has just the right size to contain the captured
 *  data and is marked with #tagCookie. The size of allocated space is returned via
 *  second value.
 */
std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedRow> rows,
    TRefCountedTypeCookie tagCookie);

std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedOwningRow> rows,
    TRefCountedTypeCookie tagCookie);

template <class TTag, class TRow>
std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(TRange<TRow> rows)
{
    return CaptureRows(rows, GetRefCountedTypeCookie<TTag>());
}

////////////////////////////////////////////////////////////////////////////////

//! A variant of TUnversionedRow that enables mutating access to its content.
class TMutableUnversionedRow
    : public TUnversionedRow
{
public:
    TMutableUnversionedRow() = default;

    explicit TMutableUnversionedRow(TUnversionedRowHeader* header)
        : TUnversionedRow(header)
    { }

    explicit TMutableUnversionedRow(TTypeErasedRow row)
        : TUnversionedRow(reinterpret_cast<const TUnversionedRowHeader*>(row.OpaqueHeader))
    { }

    static TMutableUnversionedRow Allocate(
        TChunkedMemoryPool* pool,
        ui32 valueCount);

    static TMutableUnversionedRow Create(
        void* buffer,
        ui32 valueCount);

    TUnversionedRowHeader* GetHeader()
    {
        return const_cast<TUnversionedRowHeader*>(TUnversionedRow::GetHeader());
    }

    TUnversionedValue* Begin()
    {
        return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1);
    }

    TUnversionedValue* End()
    {
        return Begin() + GetCount();
    }

    const TUnversionedValue* Begin() const
    {
        return reinterpret_cast<const TUnversionedValue*>(TUnversionedRow::GetHeader() + 1);
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    void SetCount(ui32 count)
    {
        Y_ASSERT(count <= GetHeader()->Capacity);
        GetHeader()->Count = count;
    }

    TUnversionedValue& operator[] (ui32 index)
    {
        Y_ASSERT(index < GetHeader()->Count);
        return Begin()[index];
    }

    // STL interop.
    TUnversionedValue* begin()
    {
        return Begin();
    }

    TUnversionedValue* end()
    {
        return End();
    }

    const TUnversionedValue* begin() const
    {
        return Begin();
    }

    const TUnversionedValue* end() const
    {
        return End();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! An owning variant of TUnversionedRow.
/*!
 *  Instances of TUnversionedOwningRow are lightweight handles.
 *  Fixed part is stored in shared ref-counted blobs.
 *  Variable part is stored in shared strings.
 */
class TUnversionedOwningRow
{
public:
    TUnversionedOwningRow()
    { }

    TUnversionedOwningRow(const TUnversionedValue* begin, const TUnversionedValue* end)
    {
        Init(begin, end);
    }

    explicit TUnversionedOwningRow(TUnversionedRow other)
    {
        if (!other)
            return;

        Init(other.Begin(), other.End());
    }

    TUnversionedOwningRow(const TUnversionedOwningRow& other)
        : RowData_(other.RowData_)
        , StringData_(other.StringData_)
    { }

    TUnversionedOwningRow(TUnversionedOwningRow&& other)
        : RowData_(std::move(other.RowData_))
        , StringData_(std::move(other.StringData_))
    { }

    explicit operator bool() const
    {
        return static_cast<bool>(RowData_);
    }

    operator TUnversionedRow() const
    {
        return Get();
    }

    TUnversionedRow Get() const
    {
        return TUnversionedRow(GetHeader());
    }

    const TUnversionedValue* Begin() const
    {
        const auto* header = GetHeader();
        return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    const TUnversionedValue& operator[] (int index) const
    {
        Y_ASSERT(index >= 0 && index < GetCount());
        return Begin()[index];
    }

    int GetCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->Count) : 0;
    }

    size_t GetByteSize() const
    {
        return StringData_.length() + RowData_.Size();
    }

    size_t GetSpaceUsed() const
    {
        return StringData_.capacity() + RowData_.Size();
    }


    friend void swap(TUnversionedOwningRow& lhs, TUnversionedOwningRow& rhs)
    {
        using std::swap;

        swap(lhs.RowData_, rhs.RowData_);
        swap(lhs.StringData_, rhs.StringData_);
    }

    TUnversionedOwningRow& operator=(const TUnversionedOwningRow& other)
    {
        RowData_ = other.RowData_;
        StringData_ = other.StringData_;
        return *this;
    }

    TUnversionedOwningRow& operator=(TUnversionedOwningRow&& other)
    {
        RowData_ = std::move(other.RowData_);
        StringData_ = std::move(other.StringData_);
        return *this;
    }


    // STL interop.
    const TUnversionedValue* begin()
    {
        return Begin();
    }

    const TUnversionedValue* end()
    {
        return End();
    }


    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    friend TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType);
    friend TUnversionedOwningRow DeserializeFromString(const TString& data);

    friend class TUnversionedOwningRowBuilder;

    TSharedMutableRef RowData_; // TRowHeader plus TValue-s
    TString StringData_;         // Holds string data


    TUnversionedOwningRow(TSharedMutableRef rowData, TString stringData)
        : RowData_(std::move(rowData))
        , StringData_(std::move(stringData))
    { }

    void Init(const TUnversionedValue* begin, const TUnversionedValue* end);

    TUnversionedRowHeader* GetHeader()
    {
        return RowData_ ? reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return RowData_ ? reinterpret_cast<const TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
    }
};

// For TKeyComparer.
inline int GetKeyComparerValueCount(const TUnversionedOwningRow& row, int prefixLength)
{
    return std::min(row.GetCount(), prefixLength);
}

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TUnversionedRow instances.
//! Only row values are kept, strings are only referenced.
class TUnversionedRowBuilder
{
public:
    static const int DefaultValueCapacity = 16;

    explicit TUnversionedRowBuilder(int initialValueCapacity = DefaultValueCapacity);

    int AddValue(const TUnversionedValue& value);
    TMutableUnversionedRow GetRow();
    void Reset();

private:
    static const int DefaultBlobCapacity =
        sizeof(TUnversionedRowHeader) +
        DefaultValueCapacity * sizeof(TUnversionedValue);

    SmallVector<char, DefaultBlobCapacity> RowData_;

    TUnversionedRowHeader* GetHeader();
    TUnversionedValue* GetValue(ui32 index);

};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TUnversionedOwningRow instances.
//! Keeps both row values and strings.
class TUnversionedOwningRowBuilder
{
public:
    static const int DefaultValueCapacity = 16;

    explicit TUnversionedOwningRowBuilder(int initialValueCapacity = DefaultValueCapacity);

    int AddValue(const TUnversionedValue& value);
    TUnversionedValue* BeginValues();
    TUnversionedValue* EndValues();

    TUnversionedOwningRow FinishRow();

private:
    int InitialValueCapacity_;

    TBlob RowData_;
    TString StringData_;

    TUnversionedRowHeader* GetHeader();
    TUnversionedValue* GetValue(ui32 index);
    void Reset();

};

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> MakeSingletonRowRange(TKey lowerBound, TKey upperBound);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

//! A hasher for TUnversionedValue.
template <>
struct hash<NYT::NTableClient::TUnversionedValue>
{
    inline size_t operator()(const NYT::NTableClient::TUnversionedValue& value) const
    {
        return GetHash(value);
    }
};

//! A hasher for TUnversionedRow.
template <>
struct hash<NYT::NTableClient::TUnversionedRow>
{
    inline size_t operator()(const NYT::NTableClient::TUnversionedRow& row) const
    {
        return GetHash(row);
    }
};
