#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TVirtualValueDirectory;

} // namespace NProto

constexpr int DefaultPartitionTag = -1;

// TODO(ifsmirnov): calculate actual estimates.
constexpr i64 DefaultRemoteDynamicStoreReaderMemoryEstimate = 64_MB;

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

class TSchemaDictionary;
class TColumnFilterDictionary;

struct IBlockWriter;
class TBlockWriter;

class THorizontalBlockReader;

struct TTableReadSpec;
struct TFetchSingleTableReadSpecOptions;

DECLARE_REFCOUNTED_STRUCT(ILookupReader)

DECLARE_REFCOUNTED_CLASS(TSamplesFetcher)

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

DECLARE_REFCOUNTED_STRUCT(IHunkChunkPayloadWriter)

DECLARE_REFCOUNTED_STRUCT(ITimingReader)

DECLARE_REFCOUNTED_STRUCT(IPartitioner)

DECLARE_REFCOUNTED_CLASS(TRowReaderAdapter)

DECLARE_REFCOUNTED_CLASS(TColumnarChunkMeta)
DECLARE_REFCOUNTED_CLASS(TCachedVersionedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TColumnarStatisticsFetcher)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderPerformanceCounters)

DECLARE_REFCOUNTED_STRUCT(IChunkLookupHashTable)

DECLARE_REFCOUNTED_STRUCT(TChunkState)

DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)

DECLARE_REFCOUNTED_STRUCT(TVirtualValueDirectory)

struct TOwningBoundaryKeys;

struct TBlobTableSchema;
class TBlobTableWriter;

struct TChunkTimestamps;

DECLARE_REFCOUNTED_CLASS(TSkynetColumnEvaluator)

DECLARE_REFCOUNTED_CLASS(TCachedBlockMeta)
DECLARE_REFCOUNTED_CLASS(TBlockMetaCache)

DECLARE_REFCOUNTED_CLASS(TTableColumnarStatisticsCache)

class TSchemafulRowMerger;
class TUnversionedRowMerger;
class TVersionedRowMerger;
class TSamplingRowMerger;

DECLARE_REFCOUNTED_CLASS(TTableWriterOptions)
DECLARE_REFCOUNTED_CLASS(TTableReaderOptions)

DECLARE_REFCOUNTED_CLASS(TBlobTableWriterConfig)
DECLARE_REFCOUNTED_CLASS(TBufferedTableWriterConfig)
DECLARE_REFCOUNTED_CLASS(TPartitionConfig)
DECLARE_REFCOUNTED_CLASS(TTableColumnarStatisticsCacheConfig)
DECLARE_REFCOUNTED_CLASS(THunkChunkPayloadWriterConfig)
DECLARE_REFCOUNTED_CLASS(TBatchHunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
