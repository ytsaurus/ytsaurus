#pragma once

#include <core/misc/public.h>
#include <core/misc/enum.h>
#include <core/misc/small_vector.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <initializer_list>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNameTableExt;
class TColumnSchema;
class TTableSchemaExt;
class TKeyColumnsExt;
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
using NTransactionClient::SyncLastCommittedTimestamp;
using NTransactionClient::AsyncLastCommittedTimestamp;
using NTransactionClient::AllCommittedTimestamp;
using NTransactionClient::NotPreparedTimestamp;

using TKeyColumns = std::vector<Stroka>;

////////////////////////////////////////////////////////////////////////////////

const int TypicalColumnCount = 64;
const int MaxKeyColumnCount = 32;
const int MaxColumnLockCount = 32;
extern const Stroka PrimaryLockName;
const int MaxValuesPerRow = 1024;
const int MaxRowsPerRowset = 1024 * 1024;
const i64 MaxStringValueLength = (i64) 1024 * 1024;
const i64 MaxRowWeightLimit = (i64) 128 * 1024 * 1024;

const int DefaultPartitionTag = -1;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((MasterCommunicationFailed)  (300))
    ((SortOrderViolation)         (301))
    ((InvalidDoubleValue)         (302))
    ((IncomparableType)          (303))
    ((UnhashableType)             (304))
);

DEFINE_ENUM(ETableChunkFormat,
    ((Old)                  (1))
    ((VersionedSimple)      (2))
    ((Schemaful)            (3))
    ((SchemalessHorizontal) (4))
);

DEFINE_ENUM(EControlAttribute,
    (TableIndex)
    (KeySwitch)
    (RangeIndex)
    (RowIndex)
);

// COMPAT(psushin): Legacy enum for old chunks.
DEFINE_ENUM(ELegacyKeyPartType,
    // A special sentinel used by #GetKeySuccessor.
    ((MinSentinel)(-1))
    // Denotes a missing (null) component in a composite key.
    ((Null)(0))
    // Integer value.
    ((Int64)(1))
    // Floating-point value.
    ((Double)(2))
    // String value.
    ((String)(3))
    // Any structured value.
    ((Composite)(4))

    // A special sentinel used by #GetKeyPrefixSuccessor.
    ((MaxSentinel)(100))
);

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

typedef SmallVector<int, TypicalColumnCount> TNameTableToSchemaIdMapping;

union TUnversionedValueData;

enum class EValueType : ui16;

struct TColumnFilter;

struct TUnversionedValue;
struct TVersionedValue;

class TUnversionedOwningValue;

struct TUnversionedRowHeader;
struct TVersionedRowHeader;

class TUnversionedRow;
class TUnversionedOwningRow;

class TVersionedRow;
class TVersionedOwningRow;

typedef TUnversionedRow TKey;
typedef TUnversionedOwningRow TOwningKey;

class TUnversionedRowBuilder;
class TUnversionedOwningRowBuilder;

class TKeyComparer;

struct TColumnSchema;
class TTableSchema;

struct IBlockWriter;

class TBlockWriter;

class THorizontalSchemalessBlockReader;

struct IPartitioner;

class TSchemafulRowMerger;
class TVersionedRowMerger;

DECLARE_REFCOUNTED_CLASS(TNameTable)

DECLARE_REFCOUNTED_CLASS(TRowBuffer)

DECLARE_REFCOUNTED_CLASS(TSamplesFetcher)
DECLARE_REFCOUNTED_CLASS(TChunkSplitsFetcher)

DECLARE_REFCOUNTED_STRUCT(ISchemafulReader)
DECLARE_REFCOUNTED_STRUCT(ISchemafulWriter)
DECLARE_REFCOUNTED_CLASS(TSchemafulPipe)

DECLARE_REFCOUNTED_STRUCT(ISchemalessReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkWriter)

DECLARE_REFCOUNTED_CLASS(TPartitionChunkReader)
DECLARE_REFCOUNTED_CLASS(TPartitionMultiChunkReader)

DECLARE_REFCOUNTED_CLASS(TControlAttributesConfig)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)
DECLARE_REFCOUNTED_STRUCT(IVersionedWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IVersionedMultiChunkWriter)

DECLARE_REFCOUNTED_CLASS(TCachedVersionedChunkMeta)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderPerformanceCounters)

DECLARE_REFCOUNTED_STRUCT(IValueConsumer)
DECLARE_REFCOUNTED_CLASS(TBuildingValueConsumer)
DECLARE_REFCOUNTED_CLASS(TWritingValueConsumer)

DECLARE_REFCOUNTED_CLASS(TTableWriterOptions)

DECLARE_REFCOUNTED_CLASS(TChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TChunkWriterOptions)

DECLARE_REFCOUNTED_CLASS(TChunkReaderConfig)

DECLARE_REFCOUNTED_CLASS(TTableWriterConfig)
DECLARE_REFCOUNTED_CLASS(TTableReaderConfig)

DECLARE_REFCOUNTED_CLASS(TBufferedTableWriterConfig)

DECLARE_REFCOUNTED_CLASS(TRetentionConfig)

DECLARE_REFCOUNTED_CLASS(TLegacyChannelReader)
DECLARE_REFCOUNTED_CLASS(TLegacyTableChunkReader)

DECLARE_REFCOUNTED_CLASS(TSchemafulRowMerger);
DECLARE_REFCOUNTED_CLASS(TUnversionedRowMerger);
DECLARE_REFCOUNTED_CLASS(TVersionedRowMerger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
