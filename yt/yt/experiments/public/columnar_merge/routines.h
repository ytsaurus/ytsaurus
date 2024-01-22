#pragma once

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/columnar_chunk_format/versioned_chunk_reader.h>

namespace NYT {

using NTableClient::TTableSchema;
using NTableClient::TTableSchemaPtr;
using NIO::IIOEnginePtr;

using TOwningKey = NTableClient::TUnversionedOwningRow;

IIOEnginePtr CreateIOEngine();
NTableClient::TTableSchemaPtr GetTableSchema();

struct TReaderOptions
{
    bool AllCommitted = false;
    bool ProduceAllVersions = false;
    int ValueColumnCount = -1;
    bool NewReader = false;
    bool NoBlockFetcher = false;
};

struct TReaderData
{
    std::vector<NChunkClient::TBlock> Blocks;
    NTableClient::TCachedVersionedChunkMetaPtr ChunkMeta;
    NChunkClient::IBlockCachePtr BlockCache;
    NChunkClient::IChunkReaderPtr ChunkReader;
    NTableClient::TChunkLookupHashTablePtr LookupHashTable;
    NTableClient::TChunkColumnMappingPtr ColumnMapping;

    TReaderData(const IIOEnginePtr& ioEngine, TTableSchemaPtr schema, TString chunkFileName);

    TReaderData(TTableSchemaPtr schema, NChunkClient::TRefCountedChunkMetaPtr meta, std::vector<NChunkClient::TBlock> blocks);

    void PrepareMeta();

    void PrepareLookupHashTable();

    void PrepareColumnMapping(const TTableSchemaPtr& schema);
};

template <class TItem>
NTableClient::IVersionedReaderPtr CreateChunkReader(
    const TReaderData& readerData,
    const TTableSchemaPtr schema,
    TSharedRange<TItem> readItems,
    TReaderOptions options,
    NColumnarChunkFormat::TReaderStatisticsPtr timeStatistics = nullptr);

template <class TItem>
NTableClient::ISchemafulUnversionedReaderPtr CreateMergingReader(
    const IIOEnginePtr& ioEngine,
    const TTableSchemaPtr schema,
    TSharedRange<TItem> readItems,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options);

NTableClient::IVersionedReaderPtr CreateCompactionReader(
    const IIOEnginePtr& ioEngine,
    TTableSchemaPtr schema,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options);

void Shutdown();

} // namespace NYT
