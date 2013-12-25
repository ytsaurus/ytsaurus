#pragma once

#include "public.h"
#include "row_base.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/serialize.h>

#include <core/ytree/public.h>

#include <core/yson/public.h>

#include <ytlib/chunk_client/schema.pb.h>

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/varint.h>
#include <core/misc/serialize.h>
#include <core/misc/string.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedValue
{
    //! Column id obtained from a name table.
    ui16 Id;
    //! Column type from EValueType.
    ui16 Type;
    //! Length of a variable-sized value (only meaningful for |String| and |Any| types).
    ui32 Length;

    union
    {
        //! Integral value.
        i64 Integer;
        //! Floating-point value.
        double Double;
        //! String value for |String| type or YSON-encoded value for |Any| type.
        const char* String;
    } Data;
};

static_assert(
    sizeof(TUnversionedValue) == 16,
    "TUnversionedValue has to be exactly 16 bytes.");
static_assert(
    std::is_pod<TUnversionedValue>::value,
    "TUnversionedValue must be a POD type.");

////////////////////////////////////////////////////////////////////////////////

inline TUnversionedValue MakeUnversionedSentinelValue(EValueType type, int id = 0)
{
    return MakeSentinelValue<TUnversionedValue>(type, id);
}

inline TUnversionedValue MakeUnversionedIntegerValue(i64 value, int id = 0)
{
    return MakeIntegerValue<TUnversionedValue>(value, id);
}

inline TUnversionedValue MakeUnversionedDoubleValue(double value, int id = 0)
{
    return MakeDoubleValue<TUnversionedValue>(value, id);
}

inline TUnversionedValue MakeUnversionedStringValue(const TStringBuf& value, int id = 0)
{
    return MakeStringValue<TUnversionedValue>(value, id);
}

inline TUnversionedValue MakeUnversionedAnyValue(const TStringBuf& value, int id = 0)
{
    return MakeAnyValue<TUnversionedValue>(value, id);
}

////////////////////////////////////////////////////////////////////////////////

//! Header which precedes row values in memory layout.
struct TUnversionedRowHeader
{
    ui32 Count;
    ui32 Padding;
};

static_assert(
    sizeof(TUnversionedRowHeader) == 8,
    "TUnversionedRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TUnversionedValue& value);
int WriteValue(char* output, const TUnversionedValue& value);
int ReadValue(const char* input, TUnversionedValue* value);
Stroka ToString(const TUnversionedValue& value);

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Computes the value successor.
void AdvanceToValueSuccessor(TUnversionedValue& value);

//! Checks that two given values form a valid predecessor-successor pair.
bool IsValueSuccessor(
    const TUnversionedValue& value,
    const TUnversionedValue& successor);

//! Ternary comparison predicate for TUnversionedRow-s stripped to a given number of
//! (leading) values.
int CompareRows(
    TUnversionedRow lhs,
    TUnversionedRow rhs,
    int prefixLength = std::numeric_limits<int>::max());

bool operator == (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator != (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator <= (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator <  (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator >= (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator >  (const TUnversionedRow& lhs, const TUnversionedRow& rhs);

//! Ternary comparison predicate for TUnversionedOwningRow-s stripped to a given number of
//! (leading) values.
int CompareRows(
    const TUnversionedOwningRow& lhs,
    const TUnversionedOwningRow& rhs,
    int prefixLength = std::numeric_limits<int>::max());

bool operator == (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);
bool operator != (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);
bool operator <= (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);
bool operator <  (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);
bool operator >= (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);
bool operator >  (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs);

//! Sets all value types of |row| to |EValueType::Null|. Ids are not changed.
void ResetRowValues(TUnversionedRow* row);

//! Computes hash for a given TUnversionedValue.
size_t GetHash(const TUnversionedValue& value);

//! Computes hash for a given TUnversionedRow.
size_t GetHash(TUnversionedRow row);

//! Returns the number of bytes needed to store the fixed part of the row (header + values).
size_t GetUnversionedRowDataSize(int valueCount);

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TUnversionedRowHeader* plus an array of values.
class TUnversionedRow
{
public:
    TUnversionedRow()
        : Header(nullptr)
    { }

    explicit TUnversionedRow(TUnversionedRowHeader* header)
        : Header(header)
    { }

    static TUnversionedRow Allocate(
        TChunkedMemoryPool* pool, 
        int valueCount)
    {
        auto* header = reinterpret_cast<TUnversionedRowHeader*>(pool->Allocate(GetUnversionedRowDataSize(valueCount)));
        header->Count = valueCount;
        header->Padding = 0;
        return TUnversionedRow(header);
    }

    explicit operator bool() const
    {
        return Header != nullptr;
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return Header;
    }

    TUnversionedRowHeader* GetHeader()
    {
        return Header;
    }

    const TUnversionedValue* Begin() const
    {
        return reinterpret_cast<const TUnversionedValue*>(Header + 1);
    }

    TUnversionedValue* Begin()
    {
        return reinterpret_cast<TUnversionedValue*>(Header + 1);
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    TUnversionedValue* End()
    {
        return Begin() + GetCount();
    }

    int GetCount() const
    {
        return Header->Count;
    }

    const TUnversionedValue& operator[] (int index) const
    {
        return Begin()[index];
    }

    TUnversionedValue& operator[] (int index)
    {
        return Begin()[index];
    }

private:
    TUnversionedRowHeader* Header;

};

static_assert(
    sizeof(TUnversionedRow) == sizeof(intptr_t),
    "TUnversionedRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! Returns the successor of |key|, i.e. the key obtained from |key|
// by appending a |EValueType::Min| sentinel.
//
// TODO(sandello): Alter this function to use AdvanceToValueSuccessor().
TOwningKey GetKeySuccessor(const TOwningKey& key);

//! Returns the successor of |key| trimmed to a given length, i.e. the key
//! obtained by triming |key| to |prefixLength| and appending
//! a |EValueType::Max| sentinel.
TOwningKey GetKeyPrefixSuccessor(const TOwningKey& key, int prefixLength);

//! Returns the key with no components.
TOwningKey EmptyKey();

//! Returns the key with a single |Min| component.
TOwningKey MinKey();

//! Returns the key with a single |Max| component.
TOwningKey MaxKey();

//! Compares two keys, |a| and |b|, and returns a smaller one.
//! Ties are broken in favour of the first argument.
const TOwningKey& ChooseMinKey(const TOwningKey& a, const TOwningKey& b);

//! Compares two keys, |a| and |b|, and returns a bigger one.
//! Ties are broken in favour of the first argument.
const TOwningKey& ChooseMaxKey(const TOwningKey& a, const TOwningKey& b);

void ToProto(TProtoStringType* protoRow, TUnversionedRow row);
void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);
void FromProto(TUnversionedOwningRow* row, const NChunkClient::NProto::TKey& protoKey);

void Serialize(const TKey& key, NYson::IYsonConsumer* consumer);
void Serialize(const TOwningKey& key, NYson::IYsonConsumer* consumer);

void Deserialize(TOwningKey& key, NYTree::INodePtr node);

Stroka ToString(TUnversionedRow row);
Stroka ToString(const TUnversionedOwningRow& row);

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag { };

////////////////////////////////////////////////////////////////////////////////

//! An immutable owning version of TUnversionedRow.
/*!
 *  Instances of TOwningRow are lightweight ref-counted handles.
 *  Fixed part is stored in a (shared) blob.
 *  Variable part is stored in a (shared) string.
 */
class TUnversionedOwningRow
{
public:
    TUnversionedOwningRow()
    { }

    explicit TUnversionedOwningRow(TUnversionedRow other)
    {
        if (!other)
            return;

        size_t fixedSize = GetUnversionedRowDataSize(other.GetCount());
        RowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
        ::memcpy(RowData.Begin(), other.GetHeader(), fixedSize);

        size_t variableSize = 0;
        for (int index = 0; index < other.GetCount(); ++index) {
            const auto& otherValue = other[index];
            if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                variableSize += otherValue.Length;
            }
        }

        if (variableSize != 0) {
            StringData.resize(variableSize);
            char* current = const_cast<char*>(StringData.data());

            for (int index = 0; index < other.GetCount(); ++index) {
                const auto& otherValue = other[index];
                auto& value = reinterpret_cast<TUnversionedValue*>(GetHeader() + 1)[index];;
                if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                    ::memcpy(current, otherValue.Data.String, otherValue.Length);
                    value.Data.String = current;
                    current += otherValue.Length;
                }
            }
        }
    }

    TUnversionedOwningRow(const TUnversionedOwningRow& other)
        : RowData(other.RowData)
        , StringData(other.StringData)
    { }

    TUnversionedOwningRow(TUnversionedOwningRow&& other)
        : RowData(std::move(other.RowData))
        , StringData(std::move(other.StringData))
    { }

    explicit operator bool() const
    {
        return static_cast<bool>(RowData);
    }

    const TUnversionedValue* Begin() const
    {
        const auto* header = GetHeader();
        return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
    }

    TUnversionedValue* Begin()
    {
        RowData.EnsureNonShared<TOwningRowTag>();
        auto* header = GetHeader();
        return header ? reinterpret_cast<TUnversionedValue*>(header + 1) : nullptr;
    }

    const TUnversionedValue* End() const
    {
        return Begin() + GetCount();
    }

    TUnversionedValue* End()
    {
        RowData.EnsureNonShared<TOwningRowTag>();
        return Begin() + GetCount();
    }

    int GetCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->Count) : 0;
    }

    const TUnversionedValue& operator[] (int index) const
    {
        return Begin()[index];
    }

    TUnversionedValue& operator[] (int index)
    {
        RowData.EnsureNonShared<TOwningRowTag>();
        return Begin()[index];
    }

    const TUnversionedRow Get() const
    {
        return TUnversionedRow(const_cast<TUnversionedRowHeader*>(GetHeader()));
    }

    TUnversionedRow Get()
    {
        RowData.EnsureNonShared<TOwningRowTag>();
        return TUnversionedRow(GetHeader());
    }

    friend void swap(TUnversionedOwningRow& lhs, TUnversionedOwningRow& rhs)
    {
        using std::swap;
        swap(lhs.RowData, rhs.RowData);
        swap(lhs.StringData, rhs.StringData);
    }

    TUnversionedOwningRow& operator=(const TUnversionedOwningRow& other)
    {
        RowData = other.RowData;
        StringData = other.StringData;
        return *this;
    }

    TUnversionedOwningRow& operator=(TUnversionedOwningRow&& other)
    {
        RowData = std::move(other.RowData);
        StringData = std::move(other.StringData);
        return *this;
    }

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    friend void FromProto(TUnversionedOwningRow* row, const NChunkClient::NProto::TKey& protoKey);
    friend TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType);
    friend TUnversionedOwningRow DeserializeFromString(const Stroka& data);

    friend class TUnversionedOwningRowBuilder;

    TSharedRef RowData; // TRowHeader plus TValue-s
    Stroka StringData;  // Holds string data

    TUnversionedOwningRow(TSharedRef rowData, Stroka stringData)
        : RowData(std::move(rowData))
        , StringData(std::move(stringData))
    { }

    TUnversionedRowHeader* GetHeader()
    {
        return reinterpret_cast<TUnversionedRowHeader*>(RowData.Begin());
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return reinterpret_cast<const TUnversionedRowHeader*>(RowData.Begin());
    }

};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TUnversionedRow instances.
//! Only row values are kept, strings are only referenced.
class TUnversionedRowBuilder
{
public:
    explicit TUnversionedRowBuilder(int initialValueCapacity = 16)
    {
        ValueCapacity_ = initialValueCapacity;
        RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));

        auto* header = GetHeader();
        header->Count = 0;
    }

    void AddValue(const TUnversionedValue& value)
    {
        if (GetHeader()->Count == ValueCapacity_) {
            ValueCapacity_ = 2 * std::max(1, ValueCapacity_);
            RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
        }

        auto* header = GetHeader();
        *GetValue(header->Count) = value;
        ++header->Count;
    }

    TUnversionedRow GetRow()
    {
        return TUnversionedRow(GetHeader());
    }

private:
    int ValueCapacity_;
    TBlob RowData_;


    TUnversionedRowHeader* GetHeader()
    {
        return reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin());
    }

    TUnversionedValue* GetValue(int index)
    {
        return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
    }

};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TUnversionedOwningRow instances.
//! Keeps both row values and strings.
class TUnversionedOwningRowBuilder
{
public:
    explicit TUnversionedOwningRowBuilder(int initialValueCapacity = 16)
        : InitialValueCapacity_(initialValueCapacity)
    {
        Init();
    }

    void AddValue(const TUnversionedValue& value)
    {
        if (GetHeader()->Count == ValueCapacity_) {
            ValueCapacity_ *= 2;
            RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
        }

        auto* header = GetHeader();
        auto* newValue = GetValue(header->Count);
        *newValue = value;

        if (value.Type == EValueType::String || value.Type == EValueType::Any) {
            if (StringData_.length() + value.Length > StringData_.capacity()) {
                char* oldStringData = const_cast<char*>(StringData_.begin());
                StringData_.reserve(std::max(
                    StringData_.capacity() * 2,
                    StringData_.length() + value.Length));
                char* newStringData = const_cast<char*>(StringData_.begin());
                for (int index = 0; index < header->Count; ++index) {
                    auto* existingValue = GetValue(index);
                    if (existingValue->Type == EValueType::String || existingValue->Type == EValueType::Any) {
                        existingValue->Data.String = newStringData + (existingValue->Data.String - oldStringData);
                    }
                }
            }
            newValue->Data.String = const_cast<char*>(StringData_.end());
            StringData_.append(value.Data.String, value.Data.String + value.Length);
        }

        ++header->Count;
    }

    TUnversionedOwningRow Finish()
    {
        auto row = TUnversionedOwningRow(
            TSharedRef::FromBlob<TOwningRowTag>(std::move(RowData_)),
            std::move(StringData_));
        Init();
        return row;
    }

private:
    int InitialValueCapacity_;
    int ValueCapacity_;

    TBlob RowData_;
    Stroka StringData_;

    void Init()
    {
        ValueCapacity_ = InitialValueCapacity_;
        RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));

        auto* header = GetHeader();
        header->Count = 0;
    }

    TUnversionedRowHeader* GetHeader()
    {
        return reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin());
    }

    TUnversionedValue* GetValue(int index)
    {
        return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

//! A hasher for TUnversionedValue.
template <>
struct hash<NYT::NVersionedTableClient::TUnversionedValue>
{
    inline size_t operator()(const NYT::NVersionedTableClient::TUnversionedValue& value) const
    {
        return GetHash(value);
    }
};

