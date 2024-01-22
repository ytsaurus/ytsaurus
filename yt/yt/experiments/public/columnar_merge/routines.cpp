#include "routines.h"

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/chunk_column_mapping.h>
#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>

#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schema.h>
#include <yt/yt/ytlib/table_client/overlapping_reader.h>
#include <yt/yt/ytlib/table_client/row_merger.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {

using namespace NChunkClient;
using namespace NIO;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

auto RowComparerCache = CreateRowComparerProvider(TSlruCacheConfig::CreateWithCapacity(100));

IIOEnginePtr CreateIOEngine()
{
    return CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
}

struct TLocalChunkStore
{
    THashMap<TChunkId, TChunkFileReaderPtr> Readers_;
};

TTableSchemaPtr GetTableSchema()
{
    TFileInput file("schema.yson");
    auto yson = TYsonString(file.ReadAll());
    return ConvertTo<TTableSchemaPtr>(yson);
}

IChunkReaderAllowingRepairPtr GetChunkReader(const IIOEnginePtr& ioEngine, const TString& chunkFileName)
{
    auto chunkId = TChunkId::FromString(NFS::GetFileName(chunkFileName));
    return CreateChunkFileReaderAdapter(New<TChunkFileReader>(
        ioEngine,
        chunkId,
        TString(chunkFileName),
        true /*validateBlocksChecksums*/));
}

////////////////////////////////////////////////////////////////////////////////

class TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TChunkId chunkId,
        const std::vector<NChunkClient::TBlock>& blocks)
        : ChunkId_(chunkId)
        , Blocks_(blocks)
    { }

    void PutBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/) override
    { }

    TCachedBlock FindBlock(
        const TBlockId& id,
        EBlockType /*type*/) override
    {
        YT_ASSERT(id.ChunkId == ChunkId_);
        return TCachedBlock{Blocks_[id.BlockIndex]};
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::UncompressedData;
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return nullptr;
    }

    bool IsBlockTypeActive(EBlockType blockType) const override
    {
        return blockType == EBlockType::UncompressedData;
    }

private:
    const TChunkId ChunkId_;

    std::vector<NChunkClient::TBlock> Blocks_;
};

DEFINE_REFCOUNTED_TYPE(TPreloadedBlockCache)

////////////////////////////////////////////////////////////////////////////////

struct TBlockProvider
    : public NColumnarChunkFormat::IBlockDataProvider
{
    TChunkId ChunkId;
    IBlockCachePtr BlockCache;

    TBlockProvider(TChunkId chunkId, IBlockCachePtr blockCache)
        : ChunkId(chunkId)
        , BlockCache(blockCache)
    { }

    const char* GetBlock(ui32 blockIndex) override
    {
        NChunkClient::TBlockId blockId(ChunkId, blockIndex);
        auto cachedBlock = BlockCache->FindBlock(blockId, EBlockType::UncompressedData);

        if (!cachedBlock) {
            THROW_ERROR_EXCEPTION("Using lookup hash table with compressed in memory mode is not supported");
        }


        return cachedBlock.Data.Begin();
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderData::TReaderData(const IIOEnginePtr& ioEngine, TTableSchemaPtr /*schema*/, TString chunkFileName)
{
    Cout << Format("Loading chunk: %v", chunkFileName) << Endl;

    ChunkReader = GetChunkReader(ioEngine, chunkFileName);

    auto meta = WaitFor(ChunkReader->GetMeta(/*chunkReadOptions*/ {}))
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
    auto blockMetaExt = GetProtoExtension<NTableClient::NProto::TDataBlockMetaExt>(meta->extensions());

    auto compressedBlocks = WaitFor(ChunkReader->ReadBlocks(
        /*options*/ {},
        0,
        blockMetaExt.data_blocks_size()))
        .ValueOrThrow();

    Cout << Format("Decompressing blocks") << Endl;

    auto codecId = CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec());
    auto* codec = NCompression::GetCodec(codecId);

    size_t uncompressedChunkSize = 0;

    TDuration decompressTime;

    std::vector<TBlock> cachedBlocks;
    for (const auto& compressedBlock : compressedBlocks) {
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&decompressTime);

        cachedBlocks.emplace_back(codec->Decompress(compressedBlock.Data));
        uncompressedChunkSize += cachedBlocks.back().Size();
    }

    Blocks = cachedBlocks;

    Cout << Format("ChunkSize: %v, DecompressTime: %v", uncompressedChunkSize, decompressTime) << Endl;

    ChunkMeta = TCachedVersionedChunkMeta::Create(
        /*prepareColumnarMeta*/ false,
        /*memoryTracker*/ nullptr,
        meta);
    BlockCache = New<TPreloadedBlockCache>(ChunkReader->GetChunkId(), cachedBlocks);

    if (ChunkMeta->GetChunkFormat() == NChunkClient::EChunkFormat::TableVersionedColumnar) {
        TBlockProvider blockProvider{ChunkReader->GetChunkId(), BlockCache};
        ChunkMeta->GetPreparedChunkMeta(&blockProvider);
    }
}

TReaderData::TReaderData(TTableSchemaPtr /*schema*/, TRefCountedChunkMetaPtr meta, std::vector<TBlock> blocks)
    : Blocks(blocks)
{
    ChunkReader = NChunkClient::CreateMemoryReader(meta, blocks);

    size_t uncompressedChunkSize = 0;
    for (const auto& block : blocks) {
        uncompressedChunkSize += block.Size();
    }

    Cout << Format("ChunkSize: %v", uncompressedChunkSize) << Endl;

    ChunkMeta = TCachedVersionedChunkMeta::Create(
        /*prepareColumnarMeta*/ false,
        /*memoryTracker*/ nullptr,
        meta);
    BlockCache = New<TPreloadedBlockCache>(ChunkReader->GetChunkId(), blocks);

    if (ChunkMeta->GetChunkFormat() == NChunkClient::EChunkFormat::TableVersionedColumnar) {
        TBlockProvider blockProvider{ChunkReader->GetChunkId(), BlockCache};
        ChunkMeta->GetPreparedChunkMeta(&blockProvider);
    }
}

void TReaderData::PrepareMeta()
{
    struct TBlockProvider
        : public NColumnarChunkFormat::IBlockDataProvider
    {
        TChunkId ChunkId;
        NChunkClient::IBlockCachePtr BlockCache;

        TBlockProvider(TChunkId chunkId, NChunkClient::IBlockCachePtr blockCache)
            : ChunkId(chunkId)
            , BlockCache(blockCache)
        { }

        const char* GetBlock(ui32 blockIndex) override
        {
            NChunkClient::TBlockId blockId(ChunkId, blockIndex);
            auto cachedBlock = BlockCache->FindBlock(blockId, EBlockType::UncompressedData);

            if (!cachedBlock) {
                THROW_ERROR_EXCEPTION("Using lookup hash table with compressed in memory mode is not supported");
            }

            return cachedBlock.Data.Begin();
        }
    };
    TBlockProvider blockProvider{ChunkReader->GetChunkId(), BlockCache};

    ChunkMeta->GetPreparedChunkMeta(&blockProvider);
}

void TReaderData::PrepareLookupHashTable()
{
    auto schema = ChunkMeta->ChunkSchema();
    LookupHashTable = CreateChunkLookupHashTable(
        ChunkReader->GetChunkId(),
        0,
        Blocks,
        ChunkMeta,
        schema,
        TKeyComparer(RowComparerCache->Get(schema->GetKeyColumnTypes()).UUComparer));
}
void TReaderData::PrepareColumnMapping(const TTableSchemaPtr& schema)
{
    ColumnMapping = New<TChunkColumnMapping>(schema, ChunkMeta->ChunkSchema());
}

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
IVersionedReaderPtr CreateChunkReader(
    const TReaderData& readerData,
    const TTableSchemaPtr schema,
    TSharedRange<TItem> readItems,
    TReaderOptions options,
    NColumnarChunkFormat::TReaderStatisticsPtr timeStatistics)
{
    NTableClient::TColumnFilter columnFilter;

    if (options.ValueColumnCount >= 0) {
        columnFilter = NTableClient::TColumnFilter(
            schema->GetKeyColumnCount() + std::min(options.ValueColumnCount, schema->GetValueColumnCount()));
    }

    auto timestamp = options.AllCommitted ? AllCommittedTimestamp : AsyncLastCommittedTimestamp;
    if (options.NewReader) {
        auto blockManagerFactory = options.NoBlockFetcher
            ? NColumnarChunkFormat::CreateSyncBlockWindowManagerFactory(
                readerData.BlockCache,
                readerData.ChunkMeta,
                readerData.ChunkReader->GetChunkId())
            : NColumnarChunkFormat::CreateAsyncBlockWindowManagerFactory(
                TChunkReaderConfig::GetDefault(),
                readerData.ChunkReader,
                readerData.BlockCache,
                /*chunkReadOptions*/ {},
                readerData.ChunkMeta);

        if constexpr (std::is_same_v<TItem, TLegacyKey>) {
            if (readerData.LookupHashTable) {

                auto keysWithHints = NColumnarChunkFormat::BuildKeyHintsUsingLookupTable(
                    *readerData.LookupHashTable,
                    std::move(readItems));

                return NColumnarChunkFormat::CreateVersionedChunkReader(
                    std::move(keysWithHints),
                    timestamp,
                    readerData.ChunkMeta,
                    schema,
                    columnFilter,
                    readerData.ColumnMapping,
                    blockManagerFactory,
                    options.ProduceAllVersions,
                    timeStatistics);

            }
        }

        return NColumnarChunkFormat::CreateVersionedChunkReader(
            readItems,
            timestamp,
            readerData.ChunkMeta,
            schema,
            columnFilter,
            readerData.ColumnMapping,
            blockManagerFactory,
            options.ProduceAllVersions,
            timeStatistics);
    } else {
        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = readerData.BlockCache,
            .ChunkMeta = readerData.ChunkMeta,
            .LookupHashTable = readerData.LookupHashTable,
            .KeyComparer = TKeyComparer(RowComparerCache->Get(schema->GetKeyColumnTypes()).UUComparer),
            .TableSchema = schema,
            .ChunkColumnMapping = readerData.ColumnMapping,
        });

        if (options.NoBlockFetcher) {
            return NTableClient::CreateCacheBasedVersionedChunkReader(
                readerData.ChunkReader->GetChunkId(),
                chunkState,
                std::move(readerData.ChunkMeta),
                /*chunkReadOptions*/ {},
                readItems,
                columnFilter,
                timestamp,
                options.ProduceAllVersions);
        } else {
            return CreateVersionedChunkReader(
                TChunkReaderConfig::GetDefault(),
                std::move(readerData.ChunkReader),
                chunkState,
                std::move(readerData.ChunkMeta), // Already in chunkState?
                /*chunkReadOptions*/ {},
                readItems,
                columnFilter,
                timestamp,
                options.ProduceAllVersions);
        }
    }
}

template
IVersionedReaderPtr CreateChunkReader<TRowRange>(
    const TReaderData& readerData,
    const TTableSchemaPtr schema,
    TSharedRange<TRowRange> readItems,
    TReaderOptions options,
    NColumnarChunkFormat::TReaderStatisticsPtr timeStatistics);

template
IVersionedReaderPtr CreateChunkReader<TLegacyKey>(
    const TReaderData& readerData,
    const TTableSchemaPtr schema,
    TSharedRange<TLegacyKey> readItems,
    TReaderOptions options,
    NColumnarChunkFormat::TReaderStatisticsPtr timeStatistics);

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetSchemaFromChunkMeta(const NChunkClient::NProto::TChunkMeta& meta)
{
    auto tableSchemaExt = GetProtoExtension<TTableSchemaExt>(meta.extensions());
    auto maybeKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(meta.extensions());
    TTableSchemaPtr tableSchema;
    if (maybeKeyColumnsExt) {
        FromProto(&tableSchema, tableSchemaExt, *maybeKeyColumnsExt);
    } else {
        FromProto(&tableSchema, tableSchemaExt);
    }
    return tableSchema;
}

TTableSchemaPtr GetMergedSchema(const IIOEnginePtr& ioEngine, const std::vector<TString>& chunkFileNames)
{
    std::vector<TTableSchemaPtr> schemas;

    for (const auto& chunkFileName : chunkFileNames) {
        auto chunkReader = GetChunkReader(ioEngine, chunkFileName);
        auto meta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {}))
            .ValueOrThrow();
        schemas.push_back(GetSchemaFromChunkMeta(*meta));
    }

    return InferInputSchema(schemas, false /*discardKeyColumns*/);
}

template <class TItem>
ISchemafulUnversionedReaderPtr CreateMergingReader(
    const IIOEnginePtr& ioEngine,
    TTableSchemaPtr schema,
    TSharedRange<TItem> readItems,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options)
{
    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }

    std::vector<TOwningKey> boundaries;
    std::vector<IVersionedReaderPtr> readers;

    for (const auto& chunkFileName : chunkFileNames) {
        TReaderData readerData(ioEngine, schema, chunkFileName);

        readers.push_back(CreateChunkReader(
            readerData,
            schema,
            readItems,
            options));

        boundaries.push_back(MinKey());
    }


    NTableClient::TColumnFilter columnFilter;

    if (options.ValueColumnCount >= 0) {
        columnFilter = NTableClient::TColumnFilter(
            schema->GetKeyColumnCount() + std::min(options.ValueColumnCount, schema->GetValueColumnCount()));
    }

    auto columnEvaluatorCache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto columnEvaluator = columnEvaluatorCache->Find(schema);

    // Column filter does not correspond schema.
    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(),
        options.ValueColumnCount >= 0 ? schema->GetKeyColumnCount() + options.ValueColumnCount : schema->GetColumnCount(),
        schema->GetKeyColumnCount(),
        columnFilter,
        columnEvaluator);

    auto versionedReader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [readers = std::move(readers)] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    return versionedReader;
}

template
ISchemafulUnversionedReaderPtr CreateMergingReader(
    const IIOEnginePtr& ioEngine,
    TTableSchemaPtr schema,
    TSharedRange<TRowRange> readItems,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options);

template
ISchemafulUnversionedReaderPtr CreateMergingReader(
    const IIOEnginePtr& ioEngine,
    TTableSchemaPtr schema,
    TSharedRange<TLegacyKey> readItems,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options);

IVersionedReaderPtr CreateCompactionReader(
    const IIOEnginePtr& ioEngine,
    TTableSchemaPtr schema,
    const std::vector<TString>& chunkFileNames,
    TReaderOptions options)
{
    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }

    std::vector<TOwningKey> boundaries;
    std::vector<IVersionedReaderPtr> readers;

    for (const auto& chunkFileName : chunkFileNames) {
        TReaderData readerData(ioEngine, schema, chunkFileName);

        readers.push_back(CreateChunkReader(
            readerData,
            schema,
            MakeSingletonRowRange(MinKey(), MaxKey()),
            options));

        boundaries.push_back(MinKey());
    }

    NTableClient::TColumnFilter columnFilter;

    auto columnEvaluatorCache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto columnEvaluator = columnEvaluatorCache->Find(schema);

    auto rowMerger = std::make_unique<TVersionedRowMerger>(
        New<TRowBuffer>(),
        schema->GetColumnCount(),
        schema->GetKeyColumnCount(),
        NTableClient::TColumnFilter(),
        New<TRetentionConfig>(),
        AllCommittedTimestamp,
        0,
        columnEvaluator,
        false,
        false);

    auto versionedReader = CreateVersionedOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [readers = std::move(readers)] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    WaitFor(versionedReader->Open())
        .ThrowOnError();

    return versionedReader;
}

void Shutdown()
{
    RowComparerCache.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
