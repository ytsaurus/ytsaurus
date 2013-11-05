#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

#ifdef _MSC_VER
    #define PACK
#else
    #define PACK __attribute__((aligned(16), packed))
#endif

struct TRowValue
{
    union
    {
        i64         Integer;
        double      Double;
        const char* String;  // String itself for |String| type
                             // YSON-encoded value for |Any| type
    } Data;      // Holds the value
    ui32 Length; // For variable-sized values
    ui16 Type;   // EColumnType
    ui16 Index;  // Name Table index

    static FORCED_INLINE TRowValue MakeNull(int index)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Null;
        return result;
    }

    static FORCED_INLINE TRowValue MakeInteger(int index, i64 value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Integer;
        result.Data.Integer = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeDouble(int index, double value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Double;
        result.Data.Double = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeString(int index, const TStringBuf& value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::String;
        result.Data.String = value.begin();
        result.Length = value.length();
        return result;
    }

    static FORCED_INLINE TRowValue MakeAny(int index, const TStringBuf& value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Any;
        result.Data.String = value.begin();
        result.Length = value.length();
        return result;
    }
} PACK;

#undef PACK

static_assert(sizeof (TRowValue) == 16, "TRowValue has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
int CompareRowValues(TRowValue lhs, TRowValue rhs);
int CompareSameTypeValues(TRowValue lhs, TRowValue rhs);

//! Ternary comparison predicate for TRowValue-s.
//! Returns zero, positive or negative value depending on the outcome.
inline int CompareRowValues(TRowValue lhs, TRowValue rhs)
{
    if (LIKELY(lhs.Type == rhs.Type)) {
        return CompareSameTypeValues(lhs, rhs);
    } else {
        return lhs.Type - rhs.Type;
    }
}

//! Same as #CompareRowValues but presumes that the values are of the same type.
inline int CompareSameTypeValues(TRowValue lhs, TRowValue rhs)
{
    switch (lhs.Type) {
        case EColumnType::Integer: {
            auto lhsValue = lhs.Data.Integer;
            auto rhsValue = rhs.Data.Integer;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EColumnType::String: {
            size_t lhsLength = lhs.Length;
            size_t rhsLength = rhs.Length;
            size_t minLength = std::min(lhsLength, rhsLength);
            int result = memcmp(lhs.Data.String, rhs.Data.String, minLength);
            if (result == 0) {
                return lhsLength - rhsLength;
            } else {
                return result;
            }
        }

        case EColumnType::Any:
            return 0; // NB: Cannot actually compare composite values.

        case EColumnType::Null:
            return 0;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TRowHeader
{
    TTimestamp Timestamp;
    i32 ValueCount;
    bool Deleted;
    // ValueCount instances of TRowValue follow.
};

static_assert(sizeof (TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowHeader*.
class TRow
{
public:
    FORCED_INLINE explicit TRow(TRowHeader* header)
        : Header(header)
    { }

    FORCED_INLINE TRow(
        TChunkedMemoryPool* pool, 
        int valueCount, 
        TTimestamp timestamp = NullTimestamp, 
        bool deleted = false)
        : Header(reinterpret_cast<TRowHeader*>(pool->Allocate(sizeof(TRowHeader) + valueCount * sizeof(TRowValue))))
    {
        Header->Timestamp = timestamp;
        Header->ValueCount = valueCount;
        Header->Deleted = deleted;
    }


    FORCED_INLINE TRowValue& operator[](int index)
    {
        return *reinterpret_cast<TRowValue*>(
            reinterpret_cast<char*>(Header) +
            sizeof(TRowHeader) +
            index * sizeof(TRowValue));
    }

    FORCED_INLINE const TRowValue& operator[](int index) const
    {
        return *reinterpret_cast<TRowValue*>(
            reinterpret_cast<char*>(Header) +
            sizeof(TRowHeader) +
            index * sizeof(TRowValue));
    }


    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        return Header->Timestamp;
    }

    FORCED_INLINE void SetTimestamp(TTimestamp timestamp)
    {
        Header->Timestamp = timestamp;
    }


    FORCED_INLINE bool GetDeleted() const
    {
        return Header->Deleted;
    }

    FORCED_INLINE void SetDeleted(bool deleted = true)
    {
        Header->Deleted = deleted;
    }


    FORCED_INLINE int GetValueCount() const
    {
        return Header->ValueCount;
    }

private:
    TRowHeader* Header;

};

static_assert(sizeof (TRow) == sizeof (intptr_t), "TRow has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
