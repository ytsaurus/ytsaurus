#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_reader_adapter.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/table_chunk_format/integer_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/integer_column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/helpers.h>
#include <yt/yt/ytlib/table_chunk_format/private.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/client/chunk_client/public.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/library/formats/skiff_parser.h>
#include <yt/yt/library/formats/skiff_writer.h>
#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/random/random.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>
#include <contrib/libs/zstd/include/zdict.h>

namespace NYT {
namespace {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;
using namespace NIO;
using namespace NFormats;
using namespace NConcurrency;
using namespace NTableChunkFormat;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("DictionaryCompression");

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TCompressorOptions
{
    int InlineValueSize = 64;

    int SamplesSize = 400_KB;
    int DictionarySize = 4_KB;

    TString ChunkPath;
    TString ColumnName = "Main";
};

////////////////////////////////////////////////////////////////////////////////

class TValueDictionaryCompressor
{
public:
    void PrepareChunk(const TCompressorOptions& compressorOptions)
    {
        if (std::exchange(Initialized_, true)) {
            return;
        }

        IOEngine_ = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
        ChunkId_ = TChunkId::FromString(NFS::GetFileName(compressorOptions.ChunkPath));
        BackendReader_ = CreateChunkFileReaderAdapter(New<TChunkFileReader>(
            IOEngine_,
            ChunkId_,
            compressorOptions.ChunkPath,
            /*validateBlocksChecksums*/ true));
        ChunkMeta_ = BackendReader_->GetMeta(/*chunkReadOptions*/ {})
            .Get()
            .ValueOrThrow();
        CachedVersionedChunkMeta_ = TCachedVersionedChunkMeta::Create(
            /*preparedColumnarMeta*/ false,
            /*memoryTracker*/ nullptr,
            ChunkMeta_);
        Schema_ = GetSchemaFromChunkMeta(*ChunkMeta_);
        NameTable_ = TNameTable::FromSchema(*Schema_);

        YT_VERIFY(EChunkFormat::TableVersionedSimple == FromProto<EChunkFormat>(ChunkMeta_->format()));

        TChunkSpec chunkSpec;
        chunkSpec.set_table_index(0);
        chunkSpec.set_range_index(0);
        chunkSpec.mutable_chunk_meta()->MergeFrom(*ChunkMeta_);

        ChunkState_ = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = chunkSpec,
            .TableSchema = Schema_,
        });

        Cerr << "Compressor initialized" << Endl;
    }

    IVersionedReaderPtr CreateChunkReader(const TCompressorOptions& compressorOptions) const
    {
        std::vector<int> columnIndexes;
        for (const auto& schemaColumn : {compressorOptions.ColumnName}) {
            columnIndexes.push_back(Schema_->GetColumnIndex(schemaColumn));
        }

        TClientChunkReadOptions options;
        return CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            BackendReader_,
            ChunkState_,
            CachedVersionedChunkMeta_,
            options,
            MinKey(),
            MaxKey(),
            NTableClient::TColumnFilter(columnIndexes),
            AsyncLastCommittedTimestamp,
            /*produceAllVersions*/ false,
            /*memoryManager*/ nullptr,
            /*sessionInvoker*/ nullptr);
    }

    const TRowBufferPtr& GetRowBuffer() const
    {
        return RowBuffer_;
    }

private:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    bool Initialized_ = false;

    IIOEnginePtr IOEngine_;
    TChunkId ChunkId_;
    IChunkReaderAllowingRepairPtr BackendReader_;
    TRefCountedChunkMetaPtr ChunkMeta_;
    TCachedVersionedChunkMetaPtr CachedVersionedChunkMeta_;
    TTableSchemaPtr Schema_;
    TNameTablePtr NameTable_;
    TChunkStatePtr ChunkState_;

    std::unique_ptr<IValueColumnWriter> ColumnWriter_;
    std::unique_ptr<TDataBlockWriter> BlockWriter_;
};

////////////////////////////////////////////////////////////////////////////////

void DoRunChunkDictionaryCompression(TCompressorOptions compressorOptions)
{
    TValueDictionaryCompressor compressor;
    compressor.PrepareChunk(compressorOptions);
    auto chunkReader = compressor.CreateChunkReader(compressorOptions);

    TRowBatchReadOptions readOptions{ .MaxRowsPerRead = 10000 };

    std::vector<TVersionedRow> rows;
    while (auto batch = chunkReader->Read(readOptions)) {
        if (batch->IsEmpty()) {
            WaitFor(chunkReader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }
        auto rowsBatch = batch->MaterializeRows();
        for (auto row : rowsBatch) {
            rows.push_back(compressor.GetRowBuffer()->CaptureRow(row));
        }
        if (std::ssize(rows) > 1000000) {
            break;
        }
    }

    int inlineValuesSize = 0;
    int refValuesSize = 0;
    std::vector<TUnversionedValue> inlineValues;
    std::vector<TUnversionedValue> refValues;

    int filledSamplesSize = 0;
    std::vector<size_t> sampleSizes;
    TBlob samples(
        GetRefCountedTypeCookie<TDefaultBlobTag>(),
        /*size*/ compressorOptions.SamplesSize,
        /*initiailizeStorage*/ true,
        /*pageAligned*/ true);

    for (auto row : rows) {
        if (row.GetValueCount() == 0) {
            continue;
        }
        const auto& value = row.Values()[0];
        YT_VERIFY(IsStringLikeType(value.Type));
        int valueLength = value.Length;
        if (valueLength < compressorOptions.InlineValueSize) {
            inlineValues.push_back(value);
            inlineValuesSize += value.Length;
        } else {
            if (filledSamplesSize + valueLength < compressorOptions.SamplesSize) {
                memcpy(samples.Begin() + filledSamplesSize, value.Data.String, valueLength);
                filledSamplesSize += valueLength;
                sampleSizes.push_back(valueLength);
            }
            refValues.push_back(value);
            refValuesSize += valueLength;
        }
    }

    Cerr << "Total: " << std::ssize(rows) <<
        " Inline: " << std::ssize(inlineValues) << " Size: " << inlineValuesSize <<
        " Ref: " << std::ssize(refValues) << " Size: " << refValuesSize <<
        " DictFromSamplesSize: " << filledSamplesSize << Endl;

    // Compress blob with all values as a one.
    {
        TBlob valuesBlob(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            /*size*/ refValuesSize,
            /*initiailizeStorage*/ true,
            /*pageAligned*/ true);
        int offset = 0;
        for (const auto& value : refValues) {
            memcpy(valuesBlob.Begin() + offset, value.Data.String, value.Length);
            offset += value.Length;
        }

        TBlob compressedValuesBlob(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            /*size*/ refValuesSize + 1_KB,
            /*initiailizeStorage*/ true,
            /*pageAligned*/ true);

        TWallTimer timer;

        auto compressedSize = ZSTD_compress(
            compressedValuesBlob.Begin(), compressedValuesBlob.Size(),
            valuesBlob.Begin(), valuesBlob.Size(),
            /*compressionLevel*/ ZSTD_CLEVEL_DEFAULT);
        YT_VERIFY(!ZSTD_isError(compressedSize));
        compressedValuesBlob.Resize(compressedSize);

        Cerr << "Size: " << refValuesSize << " CompressedSize: " << compressedSize
            << " CompressionTime: " << timer.GetElapsedTime() << Endl;
    }

    // Dictionary directly from samples.
    {
        TWallTimer timer;

        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        ZSTD_CDict* cdict = ZSTD_createCDict(
            samples.Begin(),
            compressorOptions.SamplesSize,
            /*compressionLevel*/ ZSTD_CLEVEL_DEFAULT);

        auto dictCreationTime = timer.GetElapsedTime();
        timer.Restart();
        timer.Stop();

        TBlob buffer;
        int compressedRefValueSize = 0;
        for (auto row : rows) {
            if (row.GetValueCount() == 0) {
                continue;
            }
            const auto& value = row.Values()[0];
            int valueLength = value.Length;
            buffer.Resize(valueLength + 1_KB);
            if (valueLength < compressorOptions.InlineValueSize) {
                continue;
            }
            timer.Start();
            auto compressedSize = ZSTD_compress_usingCDict(
                cctx,
                buffer.Begin(),
                buffer.Size(),
                value.Data.String,
                valueLength,
                cdict);
            YT_VERIFY(!ZSTD_isError(compressedSize));
            compressedRefValueSize += compressedSize;
            timer.Stop();
        }

        auto dictCompressionTime = timer.GetElapsedTime();

        Cerr << "Size: " << refValuesSize << " CompressedSize: " << compressedRefValueSize <<
            " DictCreationTime: " << dictCreationTime << " CompressionTime: " << dictCompressionTime << Endl;
    }

    // Train dictionary from samples.
    {
        TBlob dict(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            /*size*/ compressorOptions.DictionarySize,
            /*initiailizeStorage*/ true,
            /*pageAligned*/ true);

        TWallTimer timer;

        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        int dictSize = ZDICT_trainFromBuffer(
            dict.Begin(), compressorOptions.DictionarySize,
            samples.Begin(),
            sampleSizes.begin(), sampleSizes.size());
        YT_VERIFY(!ZSTD_isError(dictSize));
        YT_VERIFY(dictSize <= compressorOptions.DictionarySize);
        dict.Resize(dictSize);
        ZSTD_CDict* cdict = ZSTD_createCDict(
            dict.Begin(),
            dict.Size(),
            /*compressionLevel*/ ZSTD_CLEVEL_DEFAULT);

        auto dictCreationTime = timer.GetElapsedTime();
        timer.Restart();
        timer.Stop();

        TBlob buffer;
        int compressedRefValueSize = 0;
        for (auto row : rows) {
            if (row.GetValueCount() == 0) {
                continue;
            }
            const auto& value = row.Values()[0];
            int valueLength = value.Length;
            buffer.Resize(valueLength + 1_KB);
            if (valueLength < compressorOptions.InlineValueSize) {
                continue;
            }
            timer.Start();
            auto compressedSize = ZSTD_compress_usingCDict(
                cctx,
                buffer.Begin(),
                buffer.Size(),
                value.Data.String,
                valueLength,
                cdict);
            YT_VERIFY(!ZSTD_isError(compressedSize));
            compressedRefValueSize += compressedSize;
            timer.Stop();
        }

        auto dictCompressionTime = timer.GetElapsedTime();

        Cerr << "Size: " << refValuesSize << " CompressedSize: " << compressedRefValueSize <<
            " DictCreationTime: " << dictCreationTime << " CompressionTime: " << dictCompressionTime << Endl;
    }

    // Experiment with small random strings on frame header, frame parameters.
    {
        TString sample1 = "vrjlj";
        TString sample2 = "gfqbxfmysd";
        TString sample3 = "fmslsaiqnacjjfw";
        TString sample4 = "zpsqwuohhqugwruqpfln";

        TBlob dict(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            /*size*/ compressorOptions.DictionarySize,
            /*initiailizeStorage*/ true,
            /*pageAligned*/ true);

        int dictSize = ZDICT_trainFromBuffer(
            dict.Begin(),
            compressorOptions.DictionarySize,
            samples.Begin(),
            sampleSizes.begin(),
            sampleSizes.size());
        YT_VERIFY(!ZSTD_isError(dictSize));
        YT_VERIFY(dictSize <= compressorOptions.DictionarySize);
        dict.Resize(dictSize);
        ZSTD_CDict* cdict = ZSTD_createCDict(
            dict.Begin(),
            dict.Size(),
            /*compressionLevel*/ ZSTD_CLEVEL_DEFAULT);

        std::vector<size_t> compressedSizes;
        std::vector<size_t> frameCompressedSizes;
        std::vector<size_t> frameHeaderSizes;

        auto performCompression = [&] (ZSTD_CCtx* ctx, ZSTD_format_e frameFormat) {
            compressedSizes.clear();
            frameCompressedSizes.clear();
            frameHeaderSizes.clear();

            TBlob buffer;
            buffer.Resize(1_KB);
            for (auto sample : {sample1, sample2, sample3, sample4}) {
                size_t compressedSize;
                if (ctx) {
                    YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_refCDict(ctx, cdict)));
                    compressedSize = ZSTD_compress2(
                        ctx,
                        buffer.Begin(), buffer.Size(),
                        sample.Data(), sample.Size());
                    YT_VERIFY(!ZSTD_isError(compressedSize));
                    compressedSizes.push_back(compressedSize);
                } else {
                    compressedSize = ZSTD_compress_usingCDict(
                        ZSTD_createCCtx(),
                        buffer.Begin(), buffer.Size(),
                        sample.Data(), sample.Size(),
                        cdict);
                    YT_VERIFY(!ZSTD_isError(compressedSize));
                    compressedSizes.push_back(compressedSize);
                }

                ZSTD_frameHeader frameHeader;
                ZSTD_getFrameHeader_advanced(&frameHeader, buffer.Begin(), compressedSize, frameFormat);
                frameCompressedSizes.push_back(frameHeader.frameContentSize);
                frameHeaderSizes.push_back(frameHeader.headerSize);
            }
        };

        auto dumpStatistics = [&] (TString header) {
            Cerr << header << Endl
                << compressedSizes[0] << "/" << frameCompressedSizes[0] << "/" << sample1.Size() << "(" << frameHeaderSizes[0] << ")" << ", "
                << compressedSizes[1] << "/" << frameCompressedSizes[1] << "/" << sample2.Size() << "(" << frameHeaderSizes[1] << ")" << ", "
                << compressedSizes[2] << "/" << frameCompressedSizes[2] << "/" << sample3.Size() << "(" << frameHeaderSizes[2] << ")" << ", "
                << compressedSizes[3] << "/" << frameCompressedSizes[3] << "/" << sample4.Size() << "(" << frameHeaderSizes[3] << ")" << Endl;
        };

        performCompression(nullptr, ZSTD_f_zstd1);
        dumpStatistics("Small random string compression statistics:");

        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        ZSTD_frameParameters frameParameters{
            .contentSizeFlag = 1,
            .checksumFlag = 0,
            .noDictIDFlag = 1,
        };
        YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_setFParams(cctx, frameParameters)));
        YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_format, ZSTD_f_zstd1_magicless)));

        performCompression(cctx, ZSTD_f_zstd1_magicless);
        dumpStatistics("DictionaryId-less magicless compression statistics:");
    }

    // Compress/decompress random string with advanced API from experiment above.
    {
        TString sample = "vrjlj";

        TBlob dict(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            /*size*/ compressorOptions.DictionarySize,
            /*initiailizeStorage*/ true,
            /*pageAligned*/ true);

        int dictSize = ZDICT_trainFromBuffer(
            dict.Begin(),
            compressorOptions.DictionarySize,
            samples.Begin(),
            sampleSizes.begin(),
            sampleSizes.size());
        YT_VERIFY(!ZSTD_isError(dictSize));
        YT_VERIFY(dictSize <= compressorOptions.DictionarySize);
        dict.Resize(dictSize);

        ZSTD_CDict* cdict = ZSTD_createCDict(
            dict.Begin(),
            dict.Size(),
            /*compressionLevel*/ ZSTD_CLEVEL_DEFAULT);

        ZSTD_DDict* ddict = ZSTD_createDDict(
            dict.Begin(),
            dict.Size());

        TBlob input;
        input.Resize(ZSTD_compressBound(sample.Size()));

        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        ZSTD_frameParameters frameParameters{
            .contentSizeFlag = 1,
            .checksumFlag = 0,
            .noDictIDFlag = 1,
        };
        YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_setFParams(cctx, frameParameters)));
        YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_format, ZSTD_f_zstd1_magicless)));
        YT_VERIFY(!ZSTD_isError(ZSTD_CCtx_refCDict(cctx, cdict)));
        auto compressedSize = ZSTD_compress2(
            cctx,
            input.Begin(), input.Size(),
            sample.Data(), sample.Size());
        YT_VERIFY(!ZSTD_isError(compressedSize));
        input.Resize(compressedSize);

        ZSTD_DCtx* dctx = ZSTD_createDCtx();
        YT_VERIFY(!ZSTD_isError(ZSTD_DCtx_setParameter(dctx, ZSTD_d_format, ZSTD_f_zstd1_magicless)));
        YT_VERIFY(!ZSTD_isError(ZSTD_DCtx_refDDict(dctx, ddict)));

        TBlob output;
        ZSTD_frameHeader frameHeader;
        ZSTD_getFrameHeader_advanced(&frameHeader, input.Begin(), input.Size(), ZSTD_f_zstd1_magicless);
        YT_VERIFY(frameHeader.frameContentSize == sample.Size());
        output.Resize(frameHeader.frameContentSize);
        auto decompressedSize = ZSTD_decompressDCtx(
            dctx,
            output.Begin(), output.Size(),
            input.Begin(), input.Size());
        output.Resize(decompressedSize);
        YT_VERIFY(!ZSTD_isError(decompressedSize));
        YT_VERIFY(decompressedSize == sample.Size());
        YT_VERIFY(TString(output.Begin(), output.Size()) == sample);
        Cerr << sample << " == " << TString(output.Begin(), output.Size()) << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace
}  // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[]) {
    NYT::TCompressorOptions compressorOptions;

    NLastGetopt::TOpts opts;

    opts.AddLongOption("inline-value-size", "Inline size")
        .StoreResult(&compressorOptions.InlineValueSize);

    opts.AddLongOption("samples-size", "Samples size")
        .StoreResult(&compressorOptions.SamplesSize);
    opts.AddLongOption("dictionary-size", "Dictionary size")
        .StoreResult(&compressorOptions.DictionarySize);

    opts.AddLongOption("chunk-path", "Chunk path")
        .StoreResult(&compressorOptions.ChunkPath);
    opts.AddLongOption("column-name", "Column name")
        .StoreResult(&compressorOptions.ColumnName);

    NLastGetopt::TOptsParseResult results(&opts, argc, argv);

    NYT::DoRunChunkDictionaryCompression(compressorOptions);
}
