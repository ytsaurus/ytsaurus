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
    //! Column id obtained from a name table.
    ui16 Id;
    //! Column type (compact EColumnType).
    ui16 Type;
    //! Length of variable-sized value (meaningful only for |String| and |Any| types).
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

    static FORCED_INLINE TRowValue MakeNull(int id)
    {
        TRowValue result;
        result.Id = id;
        result.Type = NVersionedTableClient::EColumnType::Null;
        return result;
    }

    static FORCED_INLINE TRowValue MakeInteger(int id, i64 value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = NVersionedTableClient::EColumnType::Integer;
        result.Data.Integer = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeDouble(int id, double value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = NVersionedTableClient::EColumnType::Double;
        result.Data.Double = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeString(int id, const TStringBuf& value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = NVersionedTableClient::EColumnType::String;
        result.Length = value.length();
        result.Data.String = value.begin();
        return result;
    }

    static FORCED_INLINE TRowValue MakeAny(int id, const TStringBuf& value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = NVersionedTableClient::EColumnType::Any;
        result.Length = value.length();
        result.Data.String = value.begin();
        return result;
    }
} PACK;

#undef PACK

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes.");

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

        case EColumnType::Double: {
            auto lhsValue = lhs.Data.Double;
            auto rhsValue = lhs.Data.Double;
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
            int result = ::memcmp(lhs.Data.String, rhs.Data.String, minLength);
            if (result == 0) {
                if (lhsLength < rhsLength) {
                    return -1;
                } else if (lhsLength > rhsLength) {
                    return +1;
                } else {
                    return 0;
                }
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

//! Header which preceeds row values in memory layout.
struct TRowHeader
{
    i32 ValueCount;
    bool Deleted;
    TTimestamp Timestamp;
};

static_assert(sizeof(TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes.");

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
        bool deleted = false,
        TTimestamp timestamp = NullTimestamp)
        : Header(reinterpret_cast<TRowHeader*>(
            pool->Allocate(sizeof(TRowHeader) + valueCount * sizeof(TRowValue))))
    {
        Header->ValueCount = valueCount;
        Header->Deleted = deleted;
        Header->Timestamp = timestamp;
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

    FORCED_INLINE int GetValueCount() const
    {
        return Header->ValueCount;
    }

    FORCED_INLINE bool GetDeleted() const
    {
        return Header->Deleted;
    }

    FORCED_INLINE void SetDeleted(bool deleted = true)
    {
        Header->Deleted = deleted;
    }

    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        return Header->Timestamp;
    }

    FORCED_INLINE void SetTimestamp(TTimestamp timestamp)
    {
        Header->Timestamp = timestamp;
    }

private:
    TRowHeader* Header;

};

static_assert(sizeof (TRow) == sizeof (intptr_t), "TRow has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
