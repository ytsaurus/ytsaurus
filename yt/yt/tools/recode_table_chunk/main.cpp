#include <yt/yt/library/program/program.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTools::NRecodeTableChunk {

using namespace NIO;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NFS;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddLongOption("input", "path to input chunk")
            .StoreResult(&InputFile_)
            .Required();
        Opts_
            .AddLongOption("output", "path to output chunk")
            .StoreResult(&OutputFile_)
            .Required();
        Opts_
            .AddLongOption("writer-options", "writer options")
            .StoreResult(&WriterOptions_)
            .Optional();
        Opts_
            .AddLongOption("writer-config", "writer config")
            .StoreResult(&WriterConfig_)
            .Optional();
    }

private:
    TString InputFile_;
    TString OutputFile_;

    TString CompressionCodec_;
    TString WriterOptions_;
    TString WriterConfig_;

    TRefCountedChunkMetaPtr InputChunkMeta_;
    TChunkStatePtr InputChunkState_;
    IChunkReaderPtr ChunkReader_;
    TChunkWriterOptionsPtr TableWriterOptions_;
    TChunkWriterConfigPtr TableWriterConfig_;
    TChunkFileWriterPtr ChunkWriter_;


    void DumpStatistics(const TRefCountedChunkMetaPtr& chunkMeta)
    {
        auto chunkType = CheckedEnumCast<EChunkType>(chunkMeta->type());
        if (chunkType != EChunkType::Table) {
            THROW_ERROR_EXCEPTION("Unsupported chunk type %Qlv", chunkType);
        }

        auto chunkFormat = CheckedEnumCast<EChunkFormat>(chunkMeta->format());
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta->extensions());

        Cout << "  Chunk format: " << ToString(chunkFormat) << Endl;
        Cout << "  Row count: " << miscExt.row_count() << Endl;
        Cout << "  Data weight: " << miscExt.data_weight() << Endl;
        Cout << "  Compressed data size: " << miscExt.compressed_data_size() << Endl;
        Cout << "  Uncompressed data size: " << miscExt.uncompressed_data_size() << Endl;
        Cout << "  Compression codec: " << ToString(CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec())) << Endl;
    }

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());

        ChunkReader_ = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
            ioEngine,
            TChunkId(),
            InputFile_,
            true /*validateBlocksChecksums*/));

        Cout << "Chunk id: " << ToString(ChunkReader_->GetChunkId()) << Endl;

        InputChunkMeta_ = WaitFor(ChunkReader_->GetMeta(/*chunkReadOptions*/ {}))
            .ValueOrThrow();

        InputChunkState_ = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .TableSchema = FromProto<TTableSchemaPtr>(GetProtoExtension<NTableClient::NProto::TTableSchemaExt>(InputChunkMeta_->extensions())),
        });

        Cout << "Input statistics" << Endl;
        DumpStatistics(InputChunkMeta_);
        Cout << Endl;

        ChunkWriter_ = New<TChunkFileWriter>(
            ioEngine,
            ChunkReader_->GetChunkId(),
            OutputFile_);

        TableWriterOptions_ = WriterOptions_
            ? ConvertTo<TChunkWriterOptionsPtr>(TYsonString(WriterOptions_))
            : New<TChunkWriterOptions>();

        TableWriterConfig_ = WriterConfig_
            ? ConvertTo<TChunkWriterConfigPtr>(TYsonString(WriterConfig_))
            : New<TChunkWriterConfig>();

        auto format = CheckedEnumCast<EChunkFormat>(InputChunkMeta_->format());
        if (IsTableChunkFormatVersioned(format)) {
            DoRunVersioned();
        } else {
            DoRunUnversioned();
        }

        Cout << "Output statistics" << Endl;
        auto outputChunkMeta = ChunkWriter_->GetChunkMeta();
        DumpStatistics(outputChunkMeta);
    }

    void DoRunVersioned()
    {
        auto cachedVersionedChunkMeta = WaitFor(ChunkReader_->GetMeta(/*chunkReadOptions*/ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr)))
            .ValueOrThrow();

        auto tableReader = CreateVersionedChunkReader(
            TChunkReaderConfig::GetDefault(),
            ChunkReader_,
            InputChunkState_,
            std::move(cachedVersionedChunkMeta),
            /*chunkReadOptions*/ {},
            MinKey(),
            MaxKey(),
            /*columnFilter*/ {},
            AllCommittedTimestamp,
            /*produceAllVersions*/ true);

        WaitFor(tableReader->Open())
            .ThrowOnError();

        auto tableWriter = CreateVersionedChunkWriter(
            TableWriterConfig_,
            TableWriterOptions_,
            InputChunkState_->TableSchema,
            ChunkWriter_,
            /*dataSink*/ {});

        while (auto batch = ReadRowBatch(tableReader)) {
            auto rows = batch->MaterializeRows();
            WriteRowBatch(tableWriter, rows);
        }

        WaitFor(tableWriter->Close())
            .ThrowOnError();
    }

    void DoRunUnversioned()
    {
        const auto& schema = InputChunkState_->TableSchema;

        TReadLimit lowerLimit;
        TReadLimit upperLimit;
        if (int keyColumnCount = schema->GetKeyColumnCount(); keyColumnCount > 0) {
            lowerLimit = TReadLimit{KeyBoundFromLegacyRow(MinKey(), /* isUpper */ false, keyColumnCount)};
            upperLimit = TReadLimit{KeyBoundFromLegacyRow(MaxKey(), /* isUpper */ true, keyColumnCount)};
        }

        auto tableReader = CreateSchemalessRangeChunkReader(
            InputChunkState_,
            New<TColumnarChunkMeta>(*InputChunkMeta_),
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            ChunkReader_,
            New<TNameTable>(),
            /*chunkReadOptions*/ {},
            schema->GetSortColumns(),
            /*omittedInaccessibleColumns*/ {},
            NTableClient::TColumnFilter(),
            TReadRange{lowerLimit, upperLimit});

        auto tableWriter = CreateSchemalessChunkWriter(
            TableWriterConfig_,
            TableWriterOptions_,
            schema,
            /*nameTable*/ nullptr,
            ChunkWriter_);

        while (auto batch = ReadRowBatch(tableReader)) {
            auto rows = batch->MaterializeRows();
            WriteRowBatch(tableWriter, rows);
        }

        WaitFor(tableWriter->Close())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra::NTools::NRecodeTableChunk

int main(int argc, const char** argv)
{
    return NYT::NTools::NRecodeTableChunk::TProgram().Run(argc, argv);
}
