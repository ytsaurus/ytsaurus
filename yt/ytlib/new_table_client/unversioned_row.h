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
    ui32 ValueCount;
    ui16 KeyCount;
    ui16 Padding;
};

static_assert(sizeof(TUnversionedRowHeader) == 8, "TUnversionedRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TUnversionedValue& value);
int WriteValue(char* output, const TUnversionedValue& value);
int ReadValue(const char* input, TUnversionedValue* value);
Stroka ToString(const TUnversionedValue& value);

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Ternary comparison predicate for TUnversionedRow-s stripped to a given number of
//! (leading) values.
int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, int prefixLength = std::numeric_limits<int>::max());

bool operator == (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator != (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator <= (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator <  (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator >= (const TUnversionedRow& lhs, const TUnversionedRow& rhs);
bool operator >  (const TUnversionedRow& lhs, const TUnversionedRow& rhs);

//! Sets all value types of |row| to |EValueType::Null|. Ids are not changed.
void ResetRowValues(TUnversionedRow* row);

//! Computes hash for a given TUnversionedValue.
size_t GetHash(const TUnversionedValue& value);

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
        int valueCount,
        int keyCount = 0)
    {
        auto* header = reinterpret_cast<TUnversionedRowHeader*>(pool->Allocate(GetUnversionedRowDataSize(valueCount)));
        header->ValueCount = valueCount;
        header->KeyCount = keyCount;
        return TUnversionedRow(header);
    }

    explicit operator bool()
    {
        return Header != nullptr;
    }

    TUnversionedRowHeader* GetHeader()
    {
        return Header;
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return Header;
    }

    const TUnversionedValue* BeginValues() const
    {
        return reinterpret_cast<const TUnversionedValue*>(Header + 1);
    }

    TUnversionedValue* BeginValues()
    {
        return reinterpret_cast<TUnversionedValue*>(Header + 1);
    }

    
    const TUnversionedValue* EndValues() const
    {
        return BeginValues() + GetValueCount();
    }

    TUnversionedValue* EndValues()
    {
        return BeginValues() + GetValueCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return BeginValues();
    }

    TUnversionedValue* BeginKeys()
    {
        return BeginValues();
    }

    const TUnversionedValue* EndKeys() const
    {
        return BeginKeys() + GetKeyCount();
    }

    TUnversionedValue* EndKeys()
    {
        return BeginKeys() + GetKeyCount();
    }

    void SetKey(int index, const TUnversionedValue& value)
    {
        BeginKeys()[index] = value;
    }

    const TUnversionedValue& GetKey(int index) const
    {
        return BeginKeys()[index];
    }

    void SetValue(int index, const TUnversionedValue& value)
    {
        BeginValues()[index] = value;
    }

    const TUnversionedValue& GetValue(int index) const
    {
        return BeginValues()[index];
    }

    int GetValueCount() const
    {
        return Header->ValueCount;
    }

    int GetKeyCount() const
    {
        return Header->KeyCount;
    }

    const TUnversionedValue& operator[] (int index) const
    {
        return GetValue(index);
    }

    TUnversionedValue& operator[] (int index)
    {
        return BeginValues()[index];
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
TOwningKey GetKeySuccessor(const TOwningKey& key);

//! Returns the successor of |key| trimmed to a given length, i.e. the key
//! obtained by triming |key| to |prefixLength| and appending
//! a |EValueType::Max| sentinel.
TOwningKey GetKeyPrefixSuccessor(const TOwningKey& key, int prefixLength);

//! Returns the key with no components.
TKey EmptyKey();

//! Returns the key with a single |Min| component.
TKey MinKey();

//! Returns the  key with a single |Max| component.
TKey MaxKey();

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);
void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);
void FromProto(TUnversionedOwningRow* row, const NChunkClient::NProto::TKey& protoKey);

void Serialize(TKey key, NYson::IYsonConsumer* consumer);
void Deserialize(TOwningKey& key, NYTree::INodePtr node);

Stroka ToString(const TUnversionedOwningRow& row);
Stroka ToString(const TUnversionedRow& row);

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag { };

Stroka SerializeToString(const TUnversionedRow& row);
TUnversionedOwningRow DeserializeFromString(const Stroka& data);

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

    TUnversionedOwningRow(TUnversionedRow other)
    {
        if (!other)
            return;

        size_t fixedSize = GetUnversionedRowDataSize(other.GetValueCount());
        RowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
        ::memcpy(RowData.Begin(), other.GetHeader(), fixedSize);

        size_t variableSize = 0;
        for (int index = 0; index < other.GetValueCount(); ++index) {
            const auto& otherValue = other[index];
            if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                variableSize += otherValue.Length;
            }
        }

        if (variableSize != 0) {
            StringData.resize(variableSize);
            char* current = const_cast<char*>(StringData.data());

            for (int index = 0; index < other.GetValueCount(); ++index) {
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


    explicit operator bool()
    {
        return static_cast<bool>(RowData);
    }

    int GetValueCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->ValueCount) : 0;
    }

    int GetKeyCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->KeyCount) : 0;
    }

    const TUnversionedValue* BeginValues() const
    {
        const auto* header = GetHeader();
        return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
    }

    TUnversionedValue* BeginValues()
    {
        auto* header = GetHeader();
        return header ? reinterpret_cast<TUnversionedValue*>(header + 1) : nullptr;
    }

    const TUnversionedValue* EndValues() const
    {
        return BeginValues() + GetValueCount();
    }

    TUnversionedValue* EndValues()
    {
        return BeginValues() + GetValueCount();
    }

    const TUnversionedValue* BeginKeys() const
    {
        return BeginValues();
    }

    TUnversionedValue* BeginKeys()
    {
        return BeginValues();
    }

    const TUnversionedValue* EndKeys() const
    {
        return BeginKeys() + GetKeyCount();
    }

    TUnversionedValue* EndKeys()
    {
        return BeginKeys() + GetKeyCount();
    }

    const TUnversionedValue& GetKey(int index) const
    {
        return BeginKeys()[index];
    }

    const TUnversionedValue& GetValue(int index) const
    {
        return BeginValues()[index];
    }

    const TUnversionedValue& operator[] (int index) const
    {
        return GetValue(index);
    }

    TUnversionedValue& operator[] (int index)
    {
        return BeginValues()[index];
    }

    operator const TUnversionedRow() const
    {
        return TUnversionedRow(const_cast<TUnversionedRowHeader*>(GetHeader()));
    }

    friend void swap(TUnversionedOwningRow& lhs, TUnversionedOwningRow& rhs)
    {
        using std::swap;
        swap(lhs.RowData, rhs.RowData);
        swap(lhs.StringData, rhs.StringData);
    }

    TUnversionedOwningRow& operator = (TUnversionedOwningRow other)
    {
        swap(*this, other);
        return *this;
    }

    void Save(TStreamSaveContext& context) const
    {
        using NYT::Save;
        Save(context, SerializeToString(*this));
    }

    void Load(TStreamLoadContext& context)
    {
        using NYT::Load;
        Stroka data;
        Load(context, data);
        *this = DeserializeFromString(data);
    }

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
        header->ValueCount = 0;
        header->Padding = 0;
    }

    void AddValue(const TUnversionedValue& value)
    {
        if (GetHeader()->ValueCount == ValueCapacity_) {
            ValueCapacity_ = 2 * std::max(1, ValueCapacity_);
            RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
        }

        auto* header = GetHeader();
        *GetValue(header->ValueCount) = value;
        ++header->ValueCount;
    }

    TUnversionedRow GetRow(int keyCount = 0)
    {
        auto* header = GetHeader();
        header->KeyCount = keyCount;
        return TUnversionedRow(header);
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
        if (GetHeader()->ValueCount == ValueCapacity_) {
            ValueCapacity_ *= 2;
            RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
        }

        auto* header = GetHeader();
        auto* newValue = GetValue(header->ValueCount);
        *newValue = value;

        if (value.Type == EValueType::String || value.Type == EValueType::Any) {
            if (StringData_.length() + value.Length > StringData_.capacity()) {
                char* oldStringData = const_cast<char*>(StringData_.begin());
                StringData_.reserve(std::max(
                    StringData_.capacity() * 2,
                    StringData_.length() + value.Length));
                char* newStringData = const_cast<char*>(StringData_.begin());
                for (int index = 0; index < header->ValueCount; ++index) {
                    auto* existingValue = GetValue(index);
                    if (existingValue->Type == EValueType::String || existingValue->Type == EValueType::Any) {
                        existingValue->Data.String = newStringData + (existingValue->Data.String - oldStringData);
                    }
                }
            }
            newValue->Data.String = const_cast<char*>(StringData_.end());
            StringData_.append(value.Data.String, value.Data.String + value.Length);
        }

        ++header->ValueCount;
    }

    TUnversionedOwningRow Finish(int keyCount = 0)
    {
        GetHeader()->KeyCount = keyCount;
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
        header->ValueCount = 0;
        header->Padding = 0;
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

