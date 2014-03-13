#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/table_client/public.h>
#include <ytlib/chunk_client/public.h>

// TODO(babenko): remove after migration
namespace NYT { namespace NTableClient { namespace NProto {
    class TKeyColumnsExt;
} } }

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNameTableExt;
class TColumnSchema;
class TTableSchemaExt;
using NTableClient::NProto::TKeyColumnsExt; // TODO(babenko): remove after migration
class TBoundaryKeysExt;
class TBlockIndexesExt;
class TBlockMetaExt;
class TBlockMeta;
class TSimpleVersionedBlockMeta;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::MinTimestamp;
using NTransactionClient::MaxTimestamp;
using NTransactionClient::LastCommittedTimestamp;
using NTransactionClient::AllCommittedTimestamp;

using NTableClient::TKeyColumns; // TODO(babenko): remove after migration

////////////////////////////////////////////////////////////////////////////////

static const int TypicalColumnCount = 64;

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

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

struct TColumnFilter
{
    TColumnFilter()
        : All(true)
    { }

    bool All;
    SmallVector<int, TypicalColumnCount> Indexes;
};

union TUnversionedValueData;

struct TUnversionedValue;
struct TVersionedValue;

struct TUnversionedRowHeader;
struct TVersionedRowHeader;

class TUnversionedRow;
class TUnversionedOwningRow;

class TVersionedRow;

typedef TUnversionedRow TKey;
typedef TUnversionedOwningRow TOwningKey;

class TUnversionedRowBuilder;
class TUnversionedOwningRowBuilder;

class TKeyComparer;

struct TColumnSchema;
class TTableSchema;

DECLARE_REFCOUNTED_CLASS(TNameTable)

class TRowBuffer;

class TBlockWriter;

// TODO(babenko): deprecated, remove
struct IReader;
typedef TIntrusivePtr<IReader> IReaderPtr;

// TODO(babenko): deprecated, remove
struct IWriter;
typedef TIntrusivePtr<IWriter> IWriterPtr;

DECLARE_REFCOUNTED_STRUCT(ISchemafulReader)
DECLARE_REFCOUNTED_STRUCT(ISchemafulWriter)
DECLARE_REFCOUNTED_CLASS(TSchemafulPipe)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)
DECLARE_REFCOUNTED_STRUCT(IVersionedWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedChunkWriter)
DECLARE_REFCOUNTED_CLASS(TVersionedChunkWriterProvider)
DECLARE_REFCOUNTED_CLASS(TVersionedMultiChunkWriter)
DECLARE_REFCOUNTED_CLASS(TCachedVersionedChunkMeta)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)

typedef NChunkClient::TEncodingWriterOptions    TChunkWriterOptions;
typedef NChunkClient::TEncodingWriterOptionsPtr TChunkWriterOptionsPtr;

DECLARE_REFCOUNTED_CLASS(TChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
