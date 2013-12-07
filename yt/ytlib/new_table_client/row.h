#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/serialize.h>

#include <core/ytree/public.h>

#include <core/yson/public.h>

#include <ytlib/chunk_client/schema.pb.h>

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

struct TVersionedValue
    : public TUnversionedValue
{
    TTimestamp Timestamp;
};

static_assert(
    sizeof(TVersionedValue) == 24,
    "TVersionedValue has to be exactly 24 bytes.");

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TValue MakeSentinelValue(EValueType type, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = type;
    return result;
}

template <class TValue>
TValue MakeIntegerValue(i64 value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Integer;
    result.Data.Integer = value;
    return result;
}

template <class TValue>
TValue MakeDoubleValue(double value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Double;
    result.Data.Double = value;
    return result;
}

template <class TValue>
TValue MakeStringValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::String;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

template <class TValue>
TValue MakeAnyValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Any;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

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

inline TVersionedValue MakeVersionedValue(const TUnversionedValue& value, TTimestamp timestamp)
{
    TVersionedValue result;
    static_cast<TUnversionedValue&>(result) = value;
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedSentinelValue(EValueType type, TTimestamp timestamp, int id = 0)
{
    TVersionedValue result;
    result.Id = id;
    result.Type = EValueType::Null;
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedIntegerValue(i64 value, TTimestamp timestamp, int id = 0)
{
    TVersionedValue result;
    result.Id = id;
    result.Type = EValueType::Integer;
    result.Data.Integer = value;
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedDoubleValue(double value, TTimestamp timestamp, int id = 0)
{
    TVersionedValue result;
    result.Id = id;
    result.Type = EValueType::Double;
    result.Data.Double = value;
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedStringValue(const TStringBuf& value, TTimestamp timestamp, int id = 0)
{
    TVersionedValue result;
    result.Id = id;
    result.Type = EValueType::String;
    result.Length = value.length();
    result.Data.String = value.begin();
    result.Timestamp = timestamp;
    return result;
}

inline TVersionedValue MakeVersionedAnyValue(const TStringBuf& value, TTimestamp timestamp, int id = 0)
{
    TVersionedValue result;
    result.Id = id;
    result.Type = EValueType::Any;
    result.Length = value.length();
    result.Data.String = value.begin();
    result.Timestamp = timestamp;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! Header which precedes row values in memory layout.
struct TRowHeader
{
    ui32 ValueCount;
    ui32 Padding;
};

static_assert(sizeof(TRowHeader) == 8, "TRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Ternary comparison predicate for TRow-s stripped to a given number of
//! (leading) values.
int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, int prefixLength = std::numeric_limits<int>::max());

//! Computes hash for a given TUnversionedValue.
size_t GetHash(const TUnversionedValue& value);

//! Returns the number of bytes needed to store the fixed part of the row (header + values).
template <class TValue>
size_t GetRowDataSize(int valueCount)
{
    return sizeof(TRowHeader) + sizeof(TValue) * valueCount;
}

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowHeader* plus an array of values.
template <class TValue>
class TRow
{
public:
    TRow()
        : Header(nullptr)
    { }

    explicit TRow(TRowHeader* header)
        : Header(header)
    { }

    static TRow Allocate(
        TChunkedMemoryPool* pool, 
        int valueCount)
    {
        auto* header = reinterpret_cast<TRowHeader*>(pool->Allocate(GetRowDataSize<TValue>(valueCount)));
        header->ValueCount = valueCount;
        return TRow(header);
    }

    explicit operator bool()
    {
        return Header != nullptr;
    }

    TRowHeader* GetHeader()
    {
        return Header;
    }

    const TRowHeader* GetHeader() const
    {
        return Header;
    }


    TValue& operator[](int index)
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TValue*>(Header + 1)[index];
    }

    const TValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TValue*>(Header + 1)[index];
    }


    const TValue* Begin() const
    {
        return &(*this)[0];
    }

    TValue* Begin()
    {
        return &(*this)[0];
    }

    
    const TValue* End() const
    {
        return Begin() + GetValueCount();
    }

    TValue* End()
    {
        return Begin() + GetValueCount();
    }


    int GetValueCount() const
    {
        return Header->ValueCount;
    }

    //! For better interoperability with std.
    int size() const
    {
        return GetValueCount();
    }


private:
    TRowHeader* Header;

};

static_assert(
    sizeof(TVersionedRow) == sizeof(intptr_t),
    "TVersionedRow size must match that of a pointer.");
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

void Serialize(TKey key, NYson::IYsonConsumer* consumer);
void Deserialize(TOwningKey& key, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag { };

//! An immutable owning version of TRow.
/*!
 *  Instances of TOwningRow are lightweight ref-counted handles.
 *  Fixed part is stored in a (shared) blob.
 *  Variable part is stored in a (shared) string.
 */
template <class TValue>
class TOwningRow
{
public:
    TOwningRow()
    { }

    TOwningRow(TRow<TValue> other)
    {
        if (!other)
            return;

        size_t fixedSize = GetRowDataSize<TValue>(other.GetValueCount());
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
                auto& value = reinterpret_cast<TValue*>(GetHeader() + 1)[index];;
                if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                    ::memcpy(current, otherValue.Data.String, otherValue.Length);
                    value.Data.String = current;
                    current += otherValue.Length;
                }
            }
        }
    }

    TOwningRow(const TOwningRow& other)
        : RowData(other.RowData)
        , StringData(other.StringData)
    { }

    TOwningRow(TOwningRow&& other)
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

    //! For better interoperability with std.
    int size() const
    {
        return GetValueCount();
    }


    const TValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<const TValue*>(GetHeader() + 1)[index];
    }

    TValue& operator[](int index)
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TValue*>(GetHeader() + 1)[index];
    }


    const TValue* Begin() const
    {
        return &(*this)[0];
    }

    TValue* Begin()
    {
        return &(*this)[0];
    }

    
    const TValue* End() const
    {
        return Begin() + GetValueCount();
    }

    TValue* End()
    {
        return Begin() + GetValueCount();
    }


    operator TRow<TValue> () const
    {
        return TRow<TValue>(const_cast<TRowHeader*>(GetHeader()));
    }


    friend void swap(TOwningRow& lhs, TOwningRow& rhs)
    {
        using std::swap;
        swap(lhs.RowData, rhs.RowData);
        swap(lhs.StringData, rhs.StringData);
    }

    TOwningRow& operator = (TOwningRow other)
    {
        swap(*this, other);
        return *this;
    }


    void Save(TStreamSaveContext& context) const
    {
        TProtoStringType str;
        ToProto(&str, *this);
        using NYT::Save;
        Save(context, str);
    }

    void Load(TStreamLoadContext& context)
    {
        using NYT::Load;
        auto str = Load<Stroka>(context);
        FromProto(this, str);
    }

private:
    friend void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);
    friend void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);
    friend TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType);
    friend class TOwningRowBuilder<TValue>;


    TSharedRef RowData; // TRowHeader plus TValue-s
    Stroka StringData;  // Holds string data


    TOwningRow(TSharedRef rowData, Stroka stringData)
        : RowData(std::move(rowData))
        , StringData(std::move(stringData))
    { }

    TRowHeader* GetHeader()
    {
        return reinterpret_cast<TRowHeader*>(RowData.Begin());
    }

    const TRowHeader* GetHeader() const
    {
        return reinterpret_cast<const TRowHeader*>(RowData.Begin());
    }

};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TRow instances.
//! Only row values are kept, strings are only referenced.
template <class TValue>
class TRowBuilder
{
public:
    explicit TRowBuilder(int initialValueCapacity = 16)
    {
        ValueCapacity_ = initialValueCapacity;
        RowData_.Resize(GetRowDataSize<TValue>(ValueCapacity_));

        auto* header = GetHeader();
        header->ValueCount = 0;
        header->Padding = 0;
    }

    void AddValue(const TValue& value)
    {
        if (GetHeader()->ValueCount == ValueCapacity_) {
            ValueCapacity_ *= 2;
            RowData_.Resize(GetRowDataSize<TValue>(ValueCapacity_));
        }

        auto* header = GetHeader();
        *GetValue(header->ValueCount) = value;
        ++header->ValueCount;
    }

    TRow<TValue> GetRow()
    {
        return TRow<TValue>(GetHeader());
    }

private:
    int ValueCapacity_;
    TBlob RowData_;


    TRowHeader* GetHeader()
    {
        return reinterpret_cast<TRowHeader*>(RowData_.Begin());
    }

    TValue* GetValue(int index)
    {
        return reinterpret_cast<TValue*>(GetHeader() + 1) + index;
    }

};

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TOwningRow instances.
//! Keeps both row values and strings.
template <class TValue>
class TOwningRowBuilder
{
public:
    explicit TOwningRowBuilder(int initialValueCapacity = 16)
        : InitialValueCapacity_(initialValueCapacity)
    {
        Init();
    }

    void AddValue(const TValue& value)
    {
        if (GetHeader()->ValueCount == ValueCapacity_) {
            ValueCapacity_ *= 2;
            RowData_.Resize(GetRowDataSize<TValue>(ValueCapacity_));
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

    TOwningRow<TValue> Finish()
    {
        auto row = TOwningRow<TValue>(
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
        RowData_.Resize(GetRowDataSize<TValue>(ValueCapacity_));

        auto* header = GetHeader();
        header->ValueCount = 0;
        header->Padding = 0;
    }

    TRowHeader* GetHeader()
    {
        return reinterpret_cast<TRowHeader*>(RowData_.Begin());
    }

    TValue* GetValue(int index)
    {
        return reinterpret_cast<TValue*>(GetHeader() + 1) + index;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TKeyPrefixComparer
{
public:
    explicit TKeyPrefixComparer(int prefixLength)
        : PrefixLength_(prefixLength)
    { }

    template <class TLhs, class TRhs>
    int operator () (TLhs lhs, TRhs rhs) const
    {
        for (int index = 0; index < PrefixLength_; ++index) {
            int result = CompareRowValues(lhs[index], rhs[index]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

private:
    int PrefixLength_;

};

////////////////////////////////////////////////////////////////////////////////

class TKeyComparer
{
public:
    explicit TKeyComparer(int prefixLength = std::numeric_limits<int>::max())
        : PrefixLength_(prefixLength)
    { }

    template <class TLhs, class TRhs>
    int operator () (TLhs lhs, TRhs rhs) const
    {
        int lhsLength = std::min(lhs.size(), PrefixLength_);
        int rhsLength = std::min(rhs.size(), PrefixLength_);
        int minLength = std::min(lhsLength, rhsLength);
        for (int index = 0; index < minLength; ++index) {
            int result = CompareRowValues(lhs[index], rhs[index]);
            if (result != 0) {
                return result;
            }
        }

        return lhsLength - rhsLength;
    }

private:
    int PrefixLength_;

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

