#pragma once

#include <yt/client/chunk_client/public.h>

#include <yt/client/cypress_client/public.h>

#include <yt/client/transaction_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

#include <initializer_list>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNameTableExt;
class TColumnSchema;
class TTableSchemaExt;
class TKeyColumnsExt;
class TBoundaryKeysExt;
class TBlockIndexesExt;
class TBlockMetaExt;
class TColumnarStatisticsExt;
class TBlockMeta;
class TSimpleVersionedBlockMeta;
class TSchemaDictionary;
class TColumnFilter;
class TReqLookupRows;
class TColumnRenameDescriptor;

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

using TKeyColumns = std::vector<TString>;

////////////////////////////////////////////////////////////////////////////////

// Keep values below consistent with https://wiki.yandex-team.ru/yt/userdoc/tables.
constexpr int MaxKeyColumnCount = 256;
constexpr int TypicalColumnCount = 64;
constexpr int MaxColumnLockCount = 32;
constexpr int MaxColumnNameLength = 256;
constexpr int MaxColumnLockLength = 256;
constexpr int MaxColumnGroupLength = 256;

// Only for dynamic tables.
constexpr int MaxValuesPerRow = 1024;
constexpr int MaxRowsPerRowset = 5 * 1024 * 1024;
constexpr i64 MaxStringValueLength = 16_MB;
constexpr i64 MaxAnyValueLength = 16_MB;
constexpr i64 MaxServerVersionedRowDataWeight = 512_MB;
constexpr i64 MaxClientVersionedRowDataWeight = 128_MB;

// Only for static tables.
constexpr i64 MaxRowWeightLimit = 128_MB;
constexpr i64 MaxKeyWeightLimit = 256_KB;

// NB(psushin): increasing this parameter requires rewriting all chunks,
// so one probably should never want to do it.
constexpr int MaxSampleSize = 64_KB;

// This is a hard limit for static tables,
// imposed Id field size (16-bit) in TUnversionedValue.
constexpr int MaxColumnId = 32 * 1024;

extern const TString SystemColumnNamePrefix;
extern const TString TableIndexColumnName;
extern const TString RowIndexColumnName;
extern const TString RangeIndexColumnName;
extern const TString TabletIndexColumnName;
extern const TString TimestampColumnName;
extern const TString PrimaryLockName;

////////////////////////////////////////////////////////////////////////////////

// Do not change these values since they are stored in the master snapshot.
DEFINE_ENUM(ETableSchemaMode,
    ((Weak)      (0))
    ((Strong)    (1))
);

DEFINE_ENUM(EOptimizeFor,
    ((Lookup)  (0))
    ((Scan)    (1))
);

DEFINE_ENUM(EErrorCode,
    ((SortOrderViolation)         (301))
    ((InvalidDoubleValue)         (302))
    ((IncomparableType)           (303))
    ((UnhashableType)             (304))
    // E.g. name table with more than #MaxColumnId columns (may come from legacy chunks).
    ((CorruptedNameTable)         (305))
    ((UniqueKeyViolation)         (306))
    ((SchemaViolation)            (307))
    ((RowWeightLimitExceeded)     (308))
    ((InvalidColumnFilter)        (309))
    ((InvalidColumnRenaming)      (310))
    ((IncompatibleKeyColumns)     (311))
);

DEFINE_ENUM(EControlAttribute,
    (TableIndex)
    (KeySwitch)
    (RangeIndex)
    (RowIndex)
);

DEFINE_ENUM(EUnavailableChunkStrategy,
    ((ThrowError)   (0))
    ((Restore)      (1))
    ((Skip)         (2))
);

using TTableId = NCypressClient::TNodeId;

//! NB: |int| is important since we use negative values to indicate that
//! certain values need to be dropped. Cf. #TRowBuffer::CaptureAndPermuteRow.
using TNameTableToSchemaIdMapping = SmallVector<int, TypicalColumnCount>;

union TUnversionedValueData;

enum class EValueType : ui8;

class TColumnFilter;

struct TUnversionedValue;
struct TVersionedValue;

class TUnversionedOwningValue;

struct TUnversionedRowHeader;
struct TVersionedRowHeader;

class TUnversionedRow;
class TMutableUnversionedRow;
class TUnversionedOwningRow;

class TVersionedRow;
class TMutableVersionedRow;
class TVersionedOwningRow;

using TKey = TUnversionedRow;
using TMutableKey = TMutableUnversionedRow;
using TOwningKey = TUnversionedOwningRow;
using TRowRange = std::pair<TUnversionedRow, TUnversionedRow>;

class TUnversionedRowBuilder;
class TUnversionedOwningRowBuilder;

using TKeyComparer = std::function<int(TKey, TKey)>;

struct TColumnRenameDescriptor;
using TColumnRenameDescriptors = std::vector<TColumnRenameDescriptor>;

class TColumnSchema;
class TTableSchema;

DECLARE_REFCOUNTED_CLASS(TNameTable)
class TNameTableReader;
class TNameTableWriter;

DECLARE_REFCOUNTED_CLASS(TRowBuffer)

DECLARE_REFCOUNTED_STRUCT(ISchemalessReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemafulReader)
DECLARE_REFCOUNTED_STRUCT(ISchemafulWriter)

DECLARE_REFCOUNTED_STRUCT(IVersionedReader)
DECLARE_REFCOUNTED_STRUCT(IVersionedWriter)

DECLARE_REFCOUNTED_CLASS(TChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TChunkWriterConfig)

DECLARE_REFCOUNTED_CLASS(TTableReaderConfig)
DECLARE_REFCOUNTED_CLASS(TTableWriterConfig)

DECLARE_REFCOUNTED_CLASS(TRetentionConfig)

DECLARE_REFCOUNTED_CLASS(TTypeConversionConfig)

DECLARE_REFCOUNTED_CLASS(TChunkReaderOptions)
DECLARE_REFCOUNTED_CLASS(TChunkWriterOptions)

class TSaveContext;
class TLoadContext;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;

class TWireProtocolReader;
class TWireProtocolWriter;

using TSchemaData = std::vector<ui32>;

DECLARE_REFCOUNTED_STRUCT(IWireProtocolRowsetReader)
DECLARE_REFCOUNTED_STRUCT(IWireProtocolRowsetWriter)

struct IValueConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
