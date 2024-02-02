#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>
#include <yt/yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/key_filter.h>
#include <yt/yt/ytlib/table_client/overlapping_reader.h>
#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/schema.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_row_digest.h>
#include <yt/yt/ytlib/table_client/versioned_row_merger.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/string/cast.h>

#include <cstdlib>
#include <cstdio>
#include <random>
#include <tuple>

using namespace NYT;
using namespace NYTree;
using namespace NYT::NYson;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NLastGetopt;
using namespace NObjectClient;
using namespace NIO;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NQueryClient;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TDataStatistics;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESliceBy,
    (None)
    (Keys)
    (Rows)
);

namespace {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr GetChunkReader(const IIOEnginePtr& ioEngine, const TString& chunkFileName)
{
    auto chunkId = TChunkId::FromString(NFS::GetFileName(chunkFileName));
    return CreateChunkFileReaderAdapter(New<TChunkFileReader>(
        ioEngine,
        chunkId,
        chunkFileName,
        true /*validateBlocksChecksums*/));
}

std::tuple<std::function<IVersionedReaderPtr(int)>, std::vector<TLegacyOwningKey>> CreateVersionedChunkReadersFactory(
    const IIOEnginePtr& ioEngine,
    const TTableSchemaPtr& schema,
    const TLegacyOwningKey& lowerKey,
    const TLegacyOwningKey& upperKey,
    IBlockCachePtr blockCache,
    const std::vector<TString>& chunkFileNames)
{
    std::vector<TLegacyOwningKey> boundaries;
    std::vector<IChunkReaderPtr> chunkReaders;

    for (const auto& chunkFileName : chunkFileNames) {
        auto chunkReader = GetChunkReader(ioEngine, chunkFileName);
        auto meta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {}))
            .ValueOrThrow();
        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(meta->extensions());
        auto minKey = WidenKey(NYT::FromProto<TLegacyOwningKey>(boundaryKeysExt.min()), schema->GetKeyColumnCount());
        boundaries.push_back(minKey);
        chunkReaders.push_back(std::move(chunkReader));
    }

    auto factory = [=, chunkReaders = std::move(chunkReaders)] (int index) -> IVersionedReaderPtr {
        const auto& chunkReader = chunkReaders[index];
        auto cachedVersionedChunkMeta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr)))
            .ValueOrThrow();

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = blockCache,
            .TableSchema = schema,
        });
        return CreateVersionedChunkReader(
            TChunkReaderConfig::GetDefault(),
            std::move(chunkReader),
            chunkState,
            std::move(cachedVersionedChunkMeta),
            /*chunkReadOptions*/ {},
            lowerKey,
            upperKey,
            NTableClient::TColumnFilter(),
            AllCommittedTimestamp,
            true);
    };

    return std::tie(factory, boundaries);
}

TTableSchemaPtr GetSchemaFromChunkMeta(const TChunkMeta& meta)
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

////////////////////////////////////////////////////////////////////////////////

bool TryPrintXorFilterBlock(const TSystemBlockMeta& meta)
{
    if (!meta.HasExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext)) {
        return false;
    }

    auto xorFilterBlockMeta = meta.GetExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext);

    Cout << "      Type: xor filter" << Endl;

    return true;
}

void PrintSystemBlockMeta(const TSystemBlockMetaExt& blocks, int dataBlockCount)
{
    Cout << "  System blocks:" << Endl;

    int i = 0;
    for (const auto& block : blocks.system_blocks()) {
        Cout << Format("    Index: %v (absolute %v)", i, i + dataBlockCount) << Endl;
        ++i;

        if (TryPrintXorFilterBlock(block)) {
            continue;
        }

        Cout << "      Type: unknown" << Endl;
    }
}

void PrintMeta(const IIOEnginePtr& ioEngine, const TString& chunkFileName)
{
    auto chunkId = TChunkId::FromString(NFS::GetFileName(TString(chunkFileName)));
    auto chunkReader = GetChunkReader(ioEngine, chunkFileName);
    auto meta = chunkReader->GetMeta(/*chunkReadOptions*/{})
        .Get()
        .ValueOrThrow();
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
    auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());
    auto blocksMeta =  GetProtoExtension<TDataBlockMetaExt>(meta->extensions());
    auto maybeDataBlocksMeta = FindProtoExtension<TDataBlockMetaExt>(meta->extensions());
    auto maybeSystemBlocksMeta = FindProtoExtension<TSystemBlockMetaExt>(meta->extensions());
    auto dataBlockCount = maybeDataBlocksMeta ? maybeDataBlocksMeta->data_blocks().size() : 0;
    auto systemBlockCount = maybeSystemBlocksMeta ? maybeSystemBlocksMeta->system_blocks().size() : 0;
    auto maybeKeyColumnsExt = FindProtoExtension<NTableClient::NProto::TKeyColumnsExt>(meta->extensions());
    auto tableSchemaExt = GetProtoExtension<NTableClient::NProto::TTableSchemaExt>(meta->extensions());
    auto maybeVersionedRowDigestExt = FindProtoExtension<NTableClient::NProto::TVersionedRowDigestExt>(meta->extensions());

    TTableSchema schema;
    if (maybeKeyColumnsExt) {
        FromProto(&schema, tableSchemaExt, *maybeKeyColumnsExt);
    } else {
        FromProto(&schema, tableSchemaExt);
    }

    Cout << "Chunk: " << ToString(chunkFileName) << Endl;
    Cout << "  Id: " << ToString(chunkId) << Endl;
    Cout << "  Chunk format: " << ToString(FromProto<EChunkFormat>(meta->format())) << Endl;
    Cout << "  Codec: " << ToString(NCompression::ECodec(miscExt.compression_codec())) << Endl;
    Cout << "  Compressed size: " << FromProto<i64>(miscExt.compressed_data_size()) << Endl;
    Cout << "  Uncompressed size: " << FromProto<i64>(miscExt.uncompressed_data_size()) << Endl;
    Cout << "  Data weight: " << FromProto<i64>(miscExt.data_weight()) << Endl;
    Cout << "  Row count: " << miscExt.row_count() << Endl;
    Cout << "  Block count: " << blocksExt.blocks_size() << Endl;
    Cout << "  Data block count: " << dataBlockCount << Endl;
    Cout << "  System block count: " << systemBlockCount << Endl;
    Cout << "  Sorted: " << miscExt.sorted() << Endl;
    Cout << "  Unique keys: " << miscExt.unique_keys() << Endl;
    Cout << "  Eden: " << miscExt.eden() << Endl;
    Cout << "  System block count (from misc ext): " << miscExt.system_block_count() << Endl;
    Cout << "  Schema: " << ConvertToYsonString(schema, EYsonFormat::Text).AsStringBuf() << Endl;
    if (maybeVersionedRowDigestExt) {
        TVersionedRowDigest digest;
        FromProto(&digest, *maybeVersionedRowDigestExt);
        Cout << "  Earliest nth timestamps:";
        for (auto x : digest.EarliestNthTimestamp) {
            Cout << " " << x;
        }
        Cout << Endl;

        Cout << "  Last timestamp percentiles (in seconds from now):" << Endl;
        auto now = TInstant::Now();
        for (int p : {1, 10, 20, 50, 80, 90, 99}) {
            auto ts = TInstant::Seconds(digest.LastTimestampDigest->GetQuantile(p * 0.01));
            auto diff = now - ts;
            Cout << Format("  %2d%%: %v", p, diff) << Endl;
        }
    }

    auto boundaryKeysExt = FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(meta->extensions());
    if (boundaryKeysExt) {
        Cout << "  MinKey: " << ToString(NYT::FromProto<TLegacyOwningKey>(boundaryKeysExt->min())) << Endl;
        Cout << "  MaxKey: " << ToString(NYT::FromProto<TLegacyOwningKey>(boundaryKeysExt->max())) << Endl;
    }

    int i = 0;
    for (const auto& block : blocksExt.blocks()) {
        Cout << "  Index: " << i << Endl;
        Cout << "    Size: " << block.size() << Endl;
        Cout << "    Offset: " << block.offset() << Endl;
        Cout << "    Checksum: " << block.checksum() << Endl;
        ++i;
    }

    if (maybeSystemBlocksMeta) {
        PrintSystemBlockMeta(*maybeSystemBlocksMeta, dataBlockCount);
    }

#if 0
    Cout << "  Block count: " << blocksMeta.data_blocks_size() << Endl;
    for (const auto& block : blocksMeta.data_blocks()) {
        Cout << "    Block index: " << block.block_index() << Endl;
        Cout << "      Partition index: " << block.partition_index() << Endl;
        Cout << "      Row count: " << block.row_count() << Endl;
        Cout << "      Uncompressed size: " << block.uncompressed_size() << Endl;
        Cout << "      Last key: " << ToString(FromProto<TLegacyOwningKey>(block.last_key())) << Endl;
    }
#endif
}

void PrintMeta(const IIOEnginePtr& ioEngine, const std::vector<TString>& chunkFileNames)
{
    for (const auto& chunkFileName : chunkFileNames) {
        PrintMeta(ioEngine, chunkFileName);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct IUniversalReader
{
    virtual ~IUniversalReader() = default;
    virtual void DumpRows() = 0;
    virtual void SweepRows() = 0;
    virtual TDataStatistics GetDataStatistics() const = 0;
};

template <class IUnversionedReader>
class TUnversionedUniversalReader
    : public IUniversalReader
{
public:
    explicit TUnversionedUniversalReader(NYT::TIntrusivePtr<IUnversionedReader> reader)
        : Reader_(std::move(reader))
    { }

    void DumpRows() override
    {
        while (auto batch = Reader_->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }
            for (auto row : batch->MaterializeRows()) {
                Cout << ToString(row) << Endl;
            }
        }
    }

    void SweepRows() override
    {
        while (auto batch = Reader_->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            }
        }
    }

    TDataStatistics GetDataStatistics() const override
    {
        return Reader_->GetDataStatistics();
    }

private:
    NYT::TIntrusivePtr<IUnversionedReader> Reader_;
};

class TVersionedUniversalReader
    : public IUniversalReader
{
public:
    explicit TVersionedUniversalReader(IVersionedReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    void DumpRows() override
    {
        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1024
        };
        while (auto batch = Reader_->Read(options)) {
            auto failedChunkIds = Reader_->GetFailedChunkIds();
            for (auto chunkId : failedChunkIds) {
                // TODO(aozeritsky): how to get the original exception here?
                THROW_ERROR_EXCEPTION("Something bad happened while reading %v", chunkId);
            }
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            } else {
                for (const auto& row : batch->MaterializeRows()) {
                    Cout << ToString(row) << Endl;
                }
            }
        }
    }

    void SweepRows() override
    {
        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1024
        };
        while (auto batch = Reader_->Read(options)) {
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            }
        }
    }

    TDataStatistics GetDataStatistics() const override
    {
        return Reader_->GetDataStatistics();
    }

private:
    IVersionedReaderPtr Reader_;
};


////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUniversalReader> CreateUnversionedUniversalReader(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    if (chunkFileNames.size() != 1) {
        THROW_ERROR_EXCEPTION("Only one chunk is supported in this mode");
    }

    auto chunkReader = GetChunkReader(ioEngine, chunkFileNames[0]);
    auto chunkMeta = chunkReader->GetMeta(/*chunkReadOptions*/ {})
        .Get()
        .ValueOrThrow();

    TChunkSpec chunkSpec;
    chunkSpec.set_table_index(0);
    chunkSpec.set_range_index(0);
    chunkSpec.mutable_chunk_meta()->MergeFrom(*chunkMeta);

    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }

    auto chunkState = New<TChunkState>(TChunkState{
        .BlockCache = std::move(blockCache),
        .ChunkSpec = chunkSpec,
        .TableSchema = schema,
    });

    int keyColumnCount = schema->GetKeyColumnCount();

    TReadLimit lowerLimit;
    TReadLimit upperLimit;

    if (keyColumnCount > 0) {
        lowerLimit = TReadLimit{KeyBoundFromLegacyRow(lowerKey, /*isUpper*/ false, keyColumnCount)};
        upperLimit = TReadLimit{KeyBoundFromLegacyRow(upperKey, /*isUpper*/ true, keyColumnCount)};
    }

    auto schemalessReader = CreateSchemalessRangeChunkReader(
        std::move(chunkState),
        New<TColumnarChunkMeta>(*chunkMeta),
        TChunkReaderConfig::GetDefault(),
        TChunkReaderOptions::GetDefault(),
        std::move(chunkReader),
        New<TNameTable>(),
        /*chunkReadOptions*/ {},
        schema->GetSortColumns(),
        /*omittedInaccessibleColumns*/ {},
        NTableClient::TColumnFilter(),
        TReadRange{lowerLimit, upperLimit});

    return std::make_unique<TUnversionedUniversalReader<ISchemalessUnversionedReader>>(std::move(schemalessReader));
}

std::unique_ptr<IUniversalReader> CreateVersionedUniversalReader(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    if (chunkFileNames.size() != 1) {
        THROW_ERROR_EXCEPTION("Only one chunk is supported in this mode");
    }

    auto chunkReader = GetChunkReader(ioEngine, chunkFileNames[0]);
    auto chunkMeta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {}))
        .ValueOrThrow();

    if (!lowerKey) {
        lowerKey = MinKey();
    }
    if (!upperKey) {
        upperKey = MaxKey();
    }

    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }
    if (!schema->GetStrict()) {
        THROW_ERROR_EXCEPTION("Versioned chunk schema should be strict");
    }

    auto cachedVersionedChunkMeta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {})
        .Apply(BIND(
            &TCachedVersionedChunkMeta::Create,
            /*prepareColumnarMeta*/ false,
            /*memoryTracker*/ nullptr)))
        .ValueOrThrow();

    auto chunkState = New<TChunkState>(TChunkState{
        .BlockCache = std::move(blockCache),
        .TableSchema = schema,
    });
    auto versionedReader = CreateVersionedChunkReader(
        TChunkReaderConfig::GetDefault(),
        std::move(chunkReader),
        chunkState,
        std::move(cachedVersionedChunkMeta),
        /*chunkReadOptions*/ {},
        lowerKey,
        upperKey,
        NTableClient::TColumnFilter(),
        AllCommittedTimestamp,
        true);

    WaitFor(versionedReader->Open())
        .ThrowOnError();

    return std::make_unique<TVersionedUniversalReader>(std::move(versionedReader));
}

std::unique_ptr<IUniversalReader> CreateNativeUniversalReader(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    if (chunkFileNames.size() != 1) {
        THROW_ERROR_EXCEPTION("Only one chunk is supported in this mode");
    }

    auto chunkReader = GetChunkReader(ioEngine, chunkFileNames[0]);
    auto chunkMeta = WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {}))
        .ValueOrThrow();
    auto format = FromProto<EChunkFormat>(chunkMeta->format());

    switch (format) {
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
        case EChunkFormat::TableUnversionedColumnar:
            return CreateUnversionedUniversalReader(
                ioEngine,
                chunkFileNames,
                std::move(blockCache),
                schema,
                lowerKey,
                upperKey);

        case EChunkFormat::TableVersionedSimple:
        case EChunkFormat::TableVersionedColumnar:
        case EChunkFormat::TableVersionedSlim:
            return CreateVersionedUniversalReader(
                ioEngine,
                chunkFileNames,
                std::move(blockCache),
                schema,
                lowerKey,
                upperKey);

        default:
            THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv", format);
    }
}

std::unique_ptr<IUniversalReader> CreateMergedUnversionedUniversalReader(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    std::function<IVersionedReaderPtr(int)> readerFactory;
    std::vector<TLegacyOwningKey> boundaries;

    if (!lowerKey) {
        lowerKey = MinKey();
    }
    if (!upperKey) {
        upperKey = MaxKey();
    }

    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }

    std::tie(readerFactory, boundaries) = CreateVersionedChunkReadersFactory(
        ioEngine,
        schema,
        lowerKey,
        upperKey,
        blockCache,
        chunkFileNames);

    auto columnEvaluatorCache = CreateColumnEvaluatorCache(
        New<TColumnEvaluatorCacheConfig>());
    auto columnEvaluator = columnEvaluatorCache->Find(schema);
    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(),
        schema->GetColumnCount(),
        schema->GetKeyColumnCount(),
        NTableClient::TColumnFilter(),
        columnEvaluator);

    auto schemafulReader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        std::move(readerFactory),
        CompareValueRanges,
        1);

    return std::make_unique<TUnversionedUniversalReader<ISchemafulUnversionedReader>>(std::move(schemafulReader));
}

std::unique_ptr<IUniversalReader> CreateMergedVersionedUniversalReader(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    std::function<IVersionedReaderPtr(int)> readerFactory;
    std::vector<TLegacyOwningKey> boundaries;

    if (!lowerKey) {
        lowerKey = MinKey();
    }
    if (!upperKey) {
        upperKey = MaxKey();
    }

    if (!schema) {
        schema = GetMergedSchema(ioEngine, chunkFileNames);
    }

    std::tie(readerFactory, boundaries) = CreateVersionedChunkReadersFactory(
        ioEngine,
        schema,
        lowerKey,
        upperKey,
        blockCache,
        chunkFileNames);

    auto columnEvaluatorCache = CreateColumnEvaluatorCache(
        New<TColumnEvaluatorCacheConfig>());
    auto columnEvaluator = columnEvaluatorCache->Find(schema);
    auto rowMerger = CreateLegacyVersionedRowMerger(
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
        std::move(readerFactory),
        CompareValueRanges,
        1);

    WaitFor(versionedReader->Open())
        .ThrowOnError();

    return std::make_unique<TVersionedUniversalReader>(std::move(versionedReader));
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUniversalReader> CreateUniversalReader(
    const IIOEnginePtr& ioEngine,
    const TString& mode,
    const std::vector<TString>& chunkFileNames,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    if (mode == "native") {
        return CreateNativeUniversalReader(
            ioEngine,
            chunkFileNames,
            blockCache,
            schema,
            lowerKey,
            upperKey);
    } else if (mode == "unversioned") {
        return CreateUnversionedUniversalReader(
            ioEngine,
            chunkFileNames,
            blockCache,
            schema,
            lowerKey,
            upperKey);
    } else if (mode == "versioned") {
        return CreateVersionedUniversalReader(
            ioEngine,
            chunkFileNames,
            blockCache,
            schema,
            lowerKey,
            upperKey);
    } else if (mode == "merged_unversioned") {
        return CreateMergedUnversionedUniversalReader(
            ioEngine,
            chunkFileNames,
            blockCache,
            schema,
            lowerKey,
            upperKey);
    } else if (mode == "merged_versioned") {
        return CreateMergedVersionedUniversalReader(
            ioEngine,
            chunkFileNames,
            blockCache,
            schema,
            lowerKey,
            upperKey);
    } else {
        THROW_ERROR_EXCEPTION("Unknown mode %Qv", mode);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ExtractErasureBlocks(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames,
    bool actionValidateErasureCodec)
{
    YT_VERIFY(!chunkFileNames.empty());

    NYT::NErasure::ICodec* codec;
    {
        auto chunkReader = GetChunkReader(ioEngine, chunkFileNames[0]);
        auto meta = chunkReader->GetMeta(/*chunkReadOptions*/ {}).Get()
            .ValueOrThrow();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
        auto codecId = CheckedEnumCast<NYT::NErasure::ECodec>(miscExt.erasure_codec());
        codec = NYT::NErasure::GetCodec(codecId);
    }
    YT_VERIFY(std::ssize(chunkFileNames) == codec->GetTotalPartCount());

    int totalBlockCount = 0;
    std::vector<int> blockIndices;
    std::vector<IChunkReaderAllowingRepairPtr> readers;
    for (int partIndex = 0; partIndex < codec->GetDataPartCount(); ++partIndex) {
        auto chunkReader = GetChunkReader(ioEngine, chunkFileNames[partIndex]);
        auto meta = chunkReader->GetMeta(/*chunkReadOptions*/ {}).Get()
            .ValueOrThrow();
        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());
        auto blockMetaExt = GetProtoExtension<NTableClient::NProto::TDataBlockMetaExt>(meta->extensions());
        auto dataBlockCount = blockMetaExt.data_blocks_size();
        auto maybeSystemBlocksMeta = FindProtoExtension<TSystemBlockMetaExt>(meta->extensions());
        auto systemBlockCount = maybeSystemBlocksMeta ? maybeSystemBlocksMeta->system_blocks_size() : 0;
        auto blockCount = dataBlockCount + systemBlockCount;
        YT_VERIFY(blockCount == blocksExt.blocks_size());
        for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
            blockIndices.push_back(totalBlockCount + blockIndex);
        }
        totalBlockCount += blockCount;
        readers.push_back(std::move(chunkReader));
    }

    auto config = New<TErasureReaderConfig>();
    config->EnableAutoRepair = false;
    auto repairingReader = CreateAdaptiveRepairingErasureReader(
        NullChunkId,
        codec,
        config,
        readers,
        /*testingOptions*/ std::nullopt);

    auto blocks = WaitFor(repairingReader->ReadBlocks(
        /*options*/ {},
        blockIndices))
        .ValueOrThrow();

    Cout << "Erasure chunk contains " << totalBlockCount << " blocks" << Endl;

    if (!actionValidateErasureCodec) {
        return;
    }

    std::vector<TSharedRef> groundTruth;
    for (const auto& block : blocks) {
        groundTruth.push_back(block.Data);
    }
    YT_VERIFY(std::ssize(groundTruth) == totalBlockCount);

    std::mt19937 gen;
    std::uniform_int_distribution<int> dist(0, codec->GetTotalPartCount() - 1);
    constexpr int iterCount = 100;
    for (int iter = 0; iter < iterCount; ++iter) {
        bool dataPartErased = false;
        std::deque<int> erasedIndicesQ;
        while (erasedIndicesQ.size() < 3 || !dataPartErased) {
            dataPartErased = false;
            erasedIndicesQ.push_back(dist(gen));
            if (erasedIndicesQ.size() > 3) {
                erasedIndicesQ.pop_front();
            }
            for (int id : erasedIndicesQ) {
                if (id < codec->GetDataPartCount()) {
                    dataPartErased = true;
                }
            }
        }
        ::NErasure::TPartIndexList erasedIndices(erasedIndicesQ.begin(), erasedIndicesQ.end());
        std::sort(erasedIndices.begin(), erasedIndices.end());
        erasedIndices.erase(std::unique(erasedIndices.begin(), erasedIndices.end()), erasedIndices.end());
        std::set<int> erasedIndicesSet(erasedIndices.begin(), erasedIndices.end());
        auto erasedIndicesStr = ToString(erasedIndices[0]);
        for (int i = 1; i < std::ssize(erasedIndices); ++i) {
            erasedIndicesStr += " " + ToString(erasedIndices[i]);
        }
        Cout << "Iteration " << iter << " of repair with erased indices " << erasedIndicesStr << Endl;

        readers.clear();
        auto repairIndices = *codec->GetRepairIndices(erasedIndices);
        std::set<int> repairIndicesSet(repairIndices.begin(), repairIndices.end());
        for (int partIndex = 0; partIndex < codec->GetTotalPartCount(); ++partIndex) {
            if (erasedIndicesSet.find(partIndex) == erasedIndicesSet.end() &&
               (partIndex < codec->GetDataPartCount() || repairIndicesSet.find(partIndex) != repairIndicesSet.end()))
            {
                readers.push_back(GetChunkReader(ioEngine, chunkFileNames[partIndex]));
            }
        }

        auto config = New<TErasureReaderConfig>();
        config->EnableAutoRepair = false;
        auto repairingReader = CreateAdaptiveRepairingErasureReader(
            NullChunkId,
            codec,
            config,
            readers,
            TRepairingErasureReaderTestingOptions{
                .ErasedIndices = erasedIndices,
            });

        auto repairedBlocks = WaitFor(repairingReader->ReadBlocks(
            /*options*/ {},
            blockIndices))
            .ValueOrThrow();
        YT_VERIFY(std::ssize(repairedBlocks) == totalBlockCount);

        for (int blockIndex = 0; blockIndex < totalBlockCount; ++blockIndex) {
            if (ToString(groundTruth[blockIndex]) != ToString(repairedBlocks[blockIndex].Data)) {
                THROW_ERROR_EXCEPTION("Block repair failed")
                    << TErrorAttribute("repair_iteration", iter)
                    << TErrorAttribute("block_index", blockIndex)
                    << TErrorAttribute("erased_parts_indices", erasedIndicesStr);
            }
        }
    }
}

void ExtractBlocks(
    const IIOEnginePtr& ioEngine,
    const TString& chunkFileName)
{
    auto chunkReader = GetChunkReader(ioEngine, chunkFileName);
    auto meta = chunkReader->GetMeta(/*chunkReadOptions*/ {})
        .Get()
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
    auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());
    auto blockMetaExt = GetProtoExtension<NTableClient::NProto::TDataBlockMetaExt>(meta->extensions());
    auto dataBlockCount = blockMetaExt.data_blocks_size();
    auto totalBlockCount = dataBlockCount;

    for (int index = 0; index < dataBlockCount; ++index) {
        auto block = chunkReader->ReadBlocks(
            /*options*/ {},
            index,
            1);
        WaitFor(block)
            .ValueOrThrow();
    }

    if (auto maybeSystemBlocksMeta = FindProtoExtension<TSystemBlockMetaExt>(meta->extensions())) {
        auto systemBlockCount = maybeSystemBlocksMeta->system_blocks_size();
        for (int index = 0; index < systemBlockCount; ++index) {
            auto block = chunkReader->ReadBlocks(
                /*options*/ {},
                dataBlockCount + index,
                1);
            WaitFor(block)
                .ValueOrThrow();
        }
        totalBlockCount += systemBlockCount;
    }

    YT_VERIFY(totalBlockCount == blocksExt.blocks_size());
}

void ExtractBlocks(
    const IIOEnginePtr& ioEngine,
    const std::vector<TString>& chunkFileNames)
{
    for (const auto& chunkFileName : chunkFileNames) {
        ExtractBlocks(ioEngine, chunkFileName);
    }
}

////////////////////////////////////////////////////////////////////////////////

void SliceChunk(
    const IIOEnginePtr& ioEngine,
    TString chunkFileName,
    ESliceBy sliceBy,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey,
    i64 sliceDataWeight,
    int keyColumnCount)
{
    auto chunkReader = GetChunkReader(ioEngine, chunkFileName);
    auto meta = chunkReader->GetMeta(/*chunkReadOptions*/ {})
        .Get()
        .ValueOrThrow();

    NChunkClient::NProto::TSliceRequest req;
    ToProto(req.mutable_chunk_id(), chunkReader->GetChunkId());
    if (lowerKey) {
        ToProto(req.mutable_lower_limit(), TLegacyReadLimit(lowerKey));
    }
    if (upperKey) {
        ToProto(req.mutable_upper_limit(), TLegacyReadLimit(upperKey));
    }
    req.set_slice_data_weight(sliceDataWeight);
    req.set_key_column_count(keyColumnCount);
    req.set_slice_by_keys(sliceBy == ESliceBy::Keys);
    auto slices = NTableClient::SliceChunk(req, *meta);
    for (const auto& slice : slices) {
        Cout << "Slice: " << Endl;
        if (slice.LowerLimit.KeyBound()) {
            Cout << "  LowerKey: " << ToString(slice.LowerLimit.KeyBound()) << Endl;
        }
        if (slice.LowerLimit.GetRowIndex()) {
            Cout << "  LowerIndex: " << *slice.LowerLimit.GetRowIndex() << Endl;
        }
        if (slice.UpperLimit.KeyBound()) {
            Cout << "  UpperKey: " << ToString(slice.UpperLimit.KeyBound()) << Endl;
        }
        if (slice.UpperLimit.GetRowIndex()) {
            Cout << "  UpperIndex: " << *slice.UpperLimit.GetRowIndex() << Endl;
        }
        Cout << "  DataWeight: " << slice.DataWeight << Endl;
        Cout << "  RowCount: " << slice.RowCount << Endl;
        Cout << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

void GuardedMain(int argc, char** argv)
{
    auto mode = TString("native");
    EIOEngineType engineType = EIOEngineType::ThreadPool;
    TString schemaString;
    TString lowerString;
    TString upperString;
    TString ioConfigString;
    bool actionMeta = false;
    bool actionDump = false;
    bool actionBenchmark = false;
    bool actionLoop = false;
    bool actionExtract = false;
    bool erasureChunk = false;
    bool actionValidateErasureCodec = false;
    ESliceBy sliceBy = ESliceBy::None;
    i64 sliceDataWeight = -1;

    TOpts opts;
    opts.AddLongOption("schema", "Table schema")
        .StoreResult(&schemaString);
    opts.AddLongOption("lower", "Lower key")
        .StoreResult(&lowerString);
    opts.AddLongOption("upper", "Upper key")
        .StoreResult(&upperString);
    opts.AddLongOption("mode", "Read mode (native|unversioned|versioned|merged_unversioned|merged_versioned)")
        .StoreResult(&mode);
    opts.AddLongOption("meta", "Print chunk meta")
        .NoArgument()
        .SetFlag(&actionMeta);
    opts.AddLongOption("dump", "Print rows")
        .NoArgument()
        .SetFlag(&actionDump);
    opts.AddLongOption("extract", "Extract blocks")
        .NoArgument()
        .SetFlag(&actionExtract);
    opts.AddLongOption("benchmark", "Benchmark reader")
        .NoArgument()
        .SetFlag(&actionBenchmark);
    opts.AddLongOption("loop", "Run reader in a loop")
        .NoArgument()
        .SetFlag(&actionLoop);
    opts.AddLongOption("engine", "I/O Engine (ThreadPool)")
        .StoreMappedResultT<TString>(&engineType, &TEnumTraits<EIOEngineType>::FromString);
    opts.AddLongOption("io-config", "I/O Engine config")
        .StoreResult(&ioConfigString);
    opts.AddLongOption("slice-by", "Slice chunk (keys|rows)")
        .StoreMappedResultT<TString>(&sliceBy, &TEnumTraits<ESliceBy>::FromString);
    opts.AddLongOption("slice-data-weight", "Slice data weight")
        .StoreResult(&sliceDataWeight);
    opts.AddLongOption("erasure-chunk", "In case of erasure chunks. Only one chunk processing is supported")
        .NoArgument()
        .SetFlag(&erasureChunk);
    opts.AddLongOption("validate-erasure-codec", "Tests codec decode correctness")
        .NoArgument()
        .SetFlag(&actionValidateErasureCodec);
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgTitle(0, "chunks", "Chunk files");

    TOptsParseResult results(&opts, argc, argv);

    argc -= results.GetFreeArgsPos();
    argv += results.GetFreeArgsPos();

#if 0
    if (argc == 0) {
        opts.PrintUsage(self, Cout);
        return;
    }
#endif

    TTableSchemaPtr schema;
    TLegacyOwningKey lowerKey;
    TLegacyOwningKey upperKey;

    IIOEnginePtr ioEngine;

    if (!schemaString.empty()) {
        schema = ConvertTo<TTableSchemaPtr>(TYsonString(schemaString));
    } else {
        if (!lowerString.empty() || !upperString.empty()) {
            THROW_ERROR_EXCEPTION("Lower and upperKey bounds require schema");
        }
    }
    if (!lowerString.empty()) {
        lowerKey = YsonToSchemafulRow(lowerString, *schema, false);
    }
    if (!upperString.empty()) {
        upperKey = YsonToSchemafulRow(upperString, *schema, false);
    }

    auto blockCache = GetNullBlockCache();

    std::vector<TString> chunkFileNames;
    for (int chunkIndex = 0; chunkIndex < argc; ++chunkIndex) {
        chunkFileNames.push_back(argv[chunkIndex]);
    }

    if (actionValidateErasureCodec && !erasureChunk) {
        THROW_ERROR_EXCEPTION("Erasure codec validation requires erasure-chunk option to be set");
    }
    if (erasureChunk) {
        if (actionDump || actionBenchmark || actionLoop) {
            THROW_ERROR_EXCEPTION("Dump, benchmark and loop actions are not supported for erasure chunks");
        }
        if (chunkFileNames.size() != 1) {
            THROW_ERROR_EXCEPTION("Only one chunk should be specified in case of erasure");
        }

        auto chunkPath = std::move(chunkFileNames[0]);
        chunkFileNames.clear();
        auto replicaPaths = NFS::EnumerateDirectories(chunkPath);
        if (replicaPaths.size() != 16) {
            THROW_ERROR_EXCEPTION("Erasure chunk directory %v must contain 16 subpaths", chunkPath);
        }
        std::sort(replicaPaths.begin(), replicaPaths.end(), [] (const auto& lhs, const auto& rhs)
            { return IntFromString<int, 10>(lhs) < IntFromString<int, 10>(rhs); });

        for (const auto& replicaDir : replicaPaths) {
            auto replicaPath = NFS::CombinePaths(chunkPath, replicaDir);
            auto files = NFS::EnumerateFiles(replicaPath);
            if (files.size() != 2) {
                THROW_ERROR_EXCEPTION("Erasure chunk replica %v subdirectory must contain 2 files: chunk and its meta", replicaPath);
            }
            replicaPath = files[0].size() < files[1].size()
                ? NFS::CombinePaths(replicaPath, files[0])
                : NFS::CombinePaths(replicaPath, files[1]);
            chunkFileNames.push_back(std::move(replicaPath));
        }
    }

    auto ioConfig = NYTree::INodePtr();
    if (!ioConfigString.empty()) {
        ioConfig = ConvertTo<NYTree::INodePtr>(TYsonString(ioConfigString));
    }

    ioEngine = CreateIOEngine(engineType, ioConfig);

    if (sliceBy != ESliceBy::None) {
        YT_VERIFY(chunkFileNames.size() == 1);
        YT_VERIFY(sliceDataWeight != -1);
        YT_VERIFY(schema);
        SliceChunk(ioEngine, chunkFileNames[0], sliceBy, lowerKey, upperKey, sliceDataWeight, schema->GetKeyColumnCount());
        return;
    }

    if (actionExtract) {
        if (erasureChunk) {
            ExtractErasureBlocks(ioEngine, chunkFileNames, actionValidateErasureCodec);
        } else {
            ExtractBlocks(ioEngine, chunkFileNames);
        }
    }
    if (actionMeta) {
        PrintMeta(ioEngine, chunkFileNames);
    }

    if (erasureChunk) {
        return;
    }

    auto reader = CreateUniversalReader(
        ioEngine,
        mode,
        chunkFileNames,
        blockCache,
        schema,
        lowerKey,
        upperKey);

    if (actionDump) {
        reader->DumpRows();
    }
    if (actionBenchmark) {
        auto start = Now();
        reader->SweepRows();
        auto duration = Now() - start;
        auto stat = reader->GetDataStatistics();
        auto seconds = duration.Seconds();
        Cout << "Time: " << seconds << " seconds.";
        Cout << " Speed: " << stat.data_weight() / 1024 / 1024 / (seconds ? seconds : 1) << " Mb/s." << Endl;
    }
    if (actionLoop) {
        while (true) {
            auto reader = CreateUniversalReader(
                ioEngine,
                mode,
                chunkFileNames,
                blockCache,
                schema,
                lowerKey,
                upperKey);
            reader->SweepRows();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////
