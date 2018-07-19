#pragma once

#include <yt/client/table_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultPartitionTag = -1;

DEFINE_ENUM(ETableChunkFormat,
    ((Old)                  (1))
    ((VersionedSimple)      (2))
    ((Schemaful)            (3))
    ((SchemalessHorizontal) (4))
    ((VersionedColumnar)    (5))
    ((UnversionedColumnar)  (6))
);

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

class TSchemaDictionary;

struct IBlockWriter;
class TBlockWriter;

class THorizontalSchemalessBlockReader;

DECLARE_REFCOUNTED_CLASS(TSamplesFetcher)
DECLARE_REFCOUNTED_CLASS(TDataSliceFetcher)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcher)

DECLARE_REFCOUNTED_CLASS(TSchemafulPipe)

DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkWriter)

DECLARE_REFCOUNTED_CLASS(TPartitionChunkReader)
DECLARE_REFCOUNTED_CLASS(TPartitionMultiChunkReader)

DECLARE_REFCOUNTED_STRUCT(IVersionedChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IVersionedMultiChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IPartitioner)

DECLARE_REFCOUNTED_CLASS(TColumnarChunkMeta)
DECLARE_REFCOUNTED_CLASS(TCachedVersionedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TColumnarStatisticsFetcher)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderPerformanceCounters)

struct IValueConsumer;

DECLARE_REFCOUNTED_STRUCT(IChunkLookupHashTable)

DECLARE_REFCOUNTED_STRUCT(TChunkState)

struct TOwningBoundaryKeys;

struct TBlobTableSchema;
class TBlobTableWriter;

struct TChunkTimestamps;

DECLARE_REFCOUNTED_CLASS(TSkynetColumnEvaluator)

DECLARE_REFCOUNTED_CLASS(TCachedBlockMeta)
DECLARE_REFCOUNTED_CLASS(TBlockMetaCache)

class TSchemafulRowMerger;
class TUnversionedRowMerger;
class TVersionedRowMerger;
class TSamplingRowMerger;

DECLARE_REFCOUNTED_CLASS(TChunkWriterOptions)
DECLARE_REFCOUNTED_CLASS(TChunkReaderOptions)

DECLARE_REFCOUNTED_CLASS(TTableWriterOptions)
DECLARE_REFCOUNTED_CLASS(TTableReaderOptions)

DECLARE_REFCOUNTED_CLASS(TBlobTableWriterConfig)

DECLARE_REFCOUNTED_CLASS(TBufferedTableWriterConfig)

DECLARE_REFCOUNTED_CLASS(TTypeConversionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
