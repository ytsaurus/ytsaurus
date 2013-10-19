#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ERowsetType,
    (Simple)
    (Versioned)   // With timestamps and tombstones
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
const TTimestamp LastCommittedTimestamp = -1;

#ifdef _MSC_VER
    #define PACK
#else
    #define PACK __attribute__((aligned(16), packed))
#endif

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
} PACK;

#undef PACK

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes");

class TNameTable;
typedef TIntrusivePtr<TNameTable> TNameTablePtr;

class TBlockWriter;

class TChunkWriter;
typedef TIntrusivePtr<TChunkWriter> TChunkWriterPtr;

struct IReader;
typedef TIntrusivePtr<IReader> IReaderPtr;

class TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TChunkReaderConfig;
typedef TIntrusivePtr<TChunkReaderConfig> TChunkReaderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
