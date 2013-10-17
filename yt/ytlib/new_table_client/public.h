#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ERowsetType,
    (Versioned)   // With timestamps and tombstones
    (Simple)
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

typedef std::vector<Stroka> TKeyColumns;

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
};

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes");

class TNameTable;
typedef TIntrusivePtr<TNameTable> TNameTablePtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
