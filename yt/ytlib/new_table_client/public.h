#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ERowsetType,
    (Simple)
    (Versioned)
);

DECLARE_ENUM(EColumnType,
    (TheBottom)
    (Integer)
    (Double)
    (String)
    (Any)
    (Null)
);

typedef i64 TTimestamp;
const TTimestamp NullTimestamp = 0;

#ifdef _MSC_VER
    #define PACK
#else
    #define PACK __attribute__((aligned(16), packed))
#endif

struct TRowValue
{
    typedef union {
        i64 Integer;
        double Double;
        const char* String;
        const char* Any;
    } TUnionType;

    TUnionType Data; // Holds the value.
    ui32 Length; // For variable-sized values.
    ui16 Type; // EColumnType
    ui16 Index; // TNameTable
} PACK;

#undef PACK

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes");

class TNameTable;
typedef TIntrusivePtr<TNameTable> TNameTablePtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

