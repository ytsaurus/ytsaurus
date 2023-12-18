#include <yt/yt/library/process/pipe.h>
#include <yt/yt/library/process/io_dispatcher.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/client/formats/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>
#include <yt/yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/assert/assert.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/string/cast.h>

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

using NFormats::TControlAttributesConfig;
using NFormats::ISchemalessFormatWriterPtr;
using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TDataStatistics;
using NChunkClient::TChunkReaderStatistics;

class TChunkCache
    : public IBlocksExtCache
{

public:
    NIO::TBlocksExtPtr Find() override
    {
        return BlocksExt_;
    }

    void Put(const TRefCountedChunkMetaPtr& /*chunkMeta*/, const NIO::TBlocksExtPtr& blocksExt) override
    {
        BlocksExt_ = blocksExt;
    }

private:
    NIO::TBlocksExtPtr BlocksExt_;
};

class TChunkFileReaderWithCache
    : public TChunkCache, public TChunkFileReader
{
public:
    TChunkFileReaderWithCache(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool validateBlocksChecksums = true)
        : TChunkFileReader(ioEngine, chunkId, fileName, validateBlocksChecksums, this)
    { }
};

IChunkReaderAllowingRepairPtr CreateChunkReader(const IIOEnginePtr& ioEngine, const TString& chunkFileName)
{
    auto chunkId = TChunkId::FromString(NFS::GetFileName(chunkFileName));
    return CreateChunkFileReaderAdapter(New<TChunkFileReaderWithCache>(
        ioEngine,
        chunkId,
        chunkFileName,
        true /*validateBlocksChecksums*/));
}

class TUnversionedUniversalReader
{
public:
    explicit TUnversionedUniversalReader(ISchemalessChunkReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    void DumpRows(ISchemalessFormatWriterPtr writer)
    {
        while (auto batch = Reader_->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }
            writer->Write(batch->MaterializeRows());
        }
        writer->Close().Get().ThrowOnError();
    }

    void SweepRows()
    {
        while (auto batch = Reader_->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            }
        }
    }

    ISchemalessUnversionedReader* GetReader() const {
        return Reader_.Get();
    }

    TDataStatistics GetDataStatistics() const
    {
        return Reader_->GetDataStatistics();
    }

private:
    ISchemalessChunkReaderPtr Reader_;
};


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

TUnversionedUniversalReader CreateUnversionedUniversalReader(
    const IIOEnginePtr& ioEngine,
    TString chunkFileName,
    IBlockCachePtr blockCache,
    TTableSchemaPtr schema,
    TLegacyOwningKey lowerKey,
    TLegacyOwningKey upperKey)
{
    YT_VERIFY(schema);

    TClientChunkReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

    auto chunkReader = CreateChunkReader(ioEngine, chunkFileName);
    auto chunkMeta = chunkReader->GetMeta(blockReadOptions)
        .Get()
        .ValueOrThrow();

    TChunkSpec chunkSpec;
    chunkSpec.set_table_index(0);
    chunkSpec.set_range_index(0);
    chunkSpec.mutable_chunk_meta()->MergeFrom(*chunkMeta);

    auto chunkState = New<TChunkState>(TChunkState{
        .BlockCache = std::move(blockCache),
        .ChunkSpec = chunkSpec,
        .TableSchema = schema,
    });

    int keyColumnCount = schema->GetKeyColumnCount();

    TReadLimit lowerLimit;
    TReadLimit upperLimit;

    if (keyColumnCount > 0) {
        lowerLimit = TReadLimit{KeyBoundFromLegacyRow(lowerKey, /* isUpper */ false, keyColumnCount)};
        upperLimit = TReadLimit{KeyBoundFromLegacyRow(upperKey, /* isUpper */ true, keyColumnCount)};
    }

    auto schemalessReader = CreateSchemalessRangeChunkReader(
        std::move(chunkState),
        New<TColumnarChunkMeta>(*chunkMeta),
        New<TChunkReaderConfig>(),
        New<TChunkReaderOptions>(),
        std::move(chunkReader),
        New<TNameTable>(),
        blockReadOptions,
        schema->GetSortColumns(),
        {} /*omittedInaccessibleColumns*/,
        NTableClient::TColumnFilter(),
        TReadRange{lowerLimit, upperLimit});

    return TUnversionedUniversalReader(std::move(schemalessReader));
}

void GuardedMain(int argc, char** argv)
{
    TOpts opts;
    TString formatStr = "yson";
    TString formatFileStr;
    opts.AddLongOption("format", "Yson output format").StoreResult(&formatStr);
    opts.AddLongOption("format-file", "File with yson output format").StoreResult(&formatFileStr);
    opts.SetFreeArgsNum(1);
    TOptsParseResult results(&opts, argc, argv);

    NFormats::TFormat format;
    if (!formatFileStr.Empty()) {
        auto formatYsonStr = TFileInput(formatFileStr).ReadAll();
        format =  NYTree::ConvertTo<NFormats::TFormat>(TYsonString(formatYsonStr));
    } else {
        format = NYTree::ConvertTo<NFormats::TFormat>(TYsonString(formatStr));
    }
    argc -= results.GetFreeArgsPos();
    argv += results.GetFreeArgsPos();
    TString chunkFileName = argv[0];

    TTableSchemaPtr schema;

    IIOEnginePtr ioEngine = NYT::NIO::CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
    auto blockCache = GetNullBlockCache();

    auto chunkReader = CreateChunkReader(ioEngine, chunkFileName);
    TClientChunkReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    auto meta = WaitFor(chunkReader->GetMeta(blockReadOptions))
        .ValueOrThrow();
    schema = GetSchemaFromChunkMeta(*meta);

    auto reader = CreateUnversionedUniversalReader(
        ioEngine,
        chunkFileName,
        blockCache,
        schema,
        TLegacyOwningKey(),
        TLegacyOwningKey());

    NPipes::TPipeFactory factory;
    NPipes::TPipe pipe = factory.Create();
    auto writerStream = NNet::CreateConnectionFromFD(1, {}, {}, NPipes::TIODispatcher::Get()->GetPoller());
    auto writer = NFormats::CreateStaticTableWriterForFormat(
        format,
        reader.GetReader()->GetNameTable(),
        { schema },
        writerStream,
        false, New<TControlAttributesConfig>(),
        0);

    auto start = Now();
    reader.DumpRows(writer);
    auto duration = Now() - start;
    auto stat = reader.GetDataStatistics();
    auto seconds = duration.Seconds();
    Cerr << "Time: " << seconds << " seconds.";
    Cerr << " Speed: " << stat.data_weight() / 1024 / 1024 / (seconds ? seconds : 1) << " Mb/s." << Endl;
}

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cerr << ToString(TError(ex)) << Endl;
    }
}
