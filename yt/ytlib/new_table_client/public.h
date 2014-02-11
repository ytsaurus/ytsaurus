#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/table_client/public.h>
#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::MinTimestamp;
using NTransactionClient::MaxTimestamp;
using NTransactionClient::LastCommittedTimestamp;
using NTransactionClient::AllCommittedTimestamp;

using NTableClient::TKeyColumns;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EValueType,
    ((Min)         (0))
    ((TheBottom)   (1))
    ((Null)        (2))
    ((Integer)     (3))
    ((Double)      (4))
    ((String)      (5))
    ((Any)         (6))
    ((Max)        (64))
);

DECLARE_ENUM(ETableChunkFormat,
    ((Old)                (0))
    ((SimpleVersioned)    (1))
);

static const int TypicalColumnCount = 64;

////////////////////////////////////////////////////////////////////////////////

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedValue;
struct TVersionedValue;

struct TUnversionedRowHeader;
struct TVersionedRowHeader;

class TUnversionedRow;
class TUnversionedOwningRow;

class TVersionedRow;

typedef TUnversionedRow       TKey;
typedef TUnversionedOwningRow TOwningKey;

class TUnversionedRowBuilder;
class TUnversionedOwningRowBuilder;

class TKeyComparer;

struct TColumnSchema;
class TTableSchema;

class TNameTable;
typedef TIntrusivePtr<TNameTable> TNameTablePtr;

class TBlockWriter;

class TChunkWriter;
typedef TIntrusivePtr<TChunkWriter> TChunkWriterPtr;

// TODO(babenko): deprecated, remove
struct IReader;
typedef TIntrusivePtr<IReader> IReaderPtr;

// TODO(babenko): deprecated, remove
struct IWriter;
typedef TIntrusivePtr<IWriter> IWriterPtr;

DECLARE_REFCOUNTED_STRUCT(ISchemedReader)
DECLARE_REFCOUNTED_STRUCT(ISchemedWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)
DECLARE_REFCOUNTED_STRUCT(IVersionedWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedChunkWriter)
DECLARE_REFCOUNTED_CLASS(TVersionedChunkWriterProvider)
DECLARE_REFCOUNTED_CLASS(TVersionedMultiChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)

class TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

typedef NChunkClient::TEncodingWriterOptions TChunkWriterOptions;
typedef TIntrusivePtr<TChunkWriterOptions> TChunkWriterOptionsPtr;

class TChunkReaderConfig;
typedef TIntrusivePtr<TChunkReaderConfig> TChunkReaderConfigPtr;

class TCachedVersionedChunkMeta;
typedef TIntrusivePtr<TCachedVersionedChunkMeta> TCachedVersionedChunkMetaPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
