#include <yt/cpp/mapreduce/interface/client_method_options.h>

#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <yt/cpp/mapreduce/library/blob_table/blob_table.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <yt/yt/client/api/cypress_client.h>

#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

#include <yt/yt/library/huggingface_client/client.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yson/node/node.h>

#include <library/cpp/getopt/modchooser.h>

#include <util/system/env.h>

namespace NYT::NTools::NImporter {

static const NLogging::TLogger Logger("Importer");

////////////////////////////////////////////////////////////////////////////////

constexpr int BufferSize = 4_MB;
constexpr int DefaultFooterReadSize = 64_KB;
constexpr int SizeOfMetadataSize = 4;
constexpr int SizeOfMagicBytes = 4;

const TString MetadataColumnName = "metadata";
const TString StartMetadataOffsetColumnName = "start_metadata_offset";
const TString PartIndexColumnName = "part_index";
const TString FileUrlColumnName = "file_url";
const TString FileIndexColumnName = "file_index";
const TString DataColumnName = "data";

////////////////////////////////////////////////////////////////////////////////

struct TOpts
{
    TOpts()
        : Opts(NLastGetopt::TOpts::Default())
    {
        Opts.AddLongOption("proxy", "Cluster name")
            .StoreResult(&Cluster)
            .Required();
        Opts.AddLongOption("dataset", "Name of dataset")
            .StoreResult(&Dataset)
            .Required();
        Opts.AddLongOption("config", "Name of config")
            .DefaultValue("default")
            .StoreResult(&Config);
        Opts.AddLongOption("split", "Name of split")
            .StoreResult(&Split)
            .Required();
        Opts.AddLongOption("output", "Path to output table")
            .StoreResult(&ResultTable)
            .Required();
    }

    NLastGetopt::TOpts Opts;

    TString Cluster;
    TString Dataset;
    TString Config;
    TString Split;
    TString ResultTable;
};

////////////////////////////////////////////////////////////////////////////////

class TDownloadFromHuggingFaceMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TDownloadFromHuggingFaceMapper()
        : Client_(GetEnv("YT_SECURE_VAULT_HUGGINGFACE_TOKEN"))
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        NYtBlobTable::TBlobTableSchema blobTableSchema;
        blobTableSchema.BlobIdColumns({ NYT::TColumnSchema().Name(FileIndexColumnName).Type(VT_INT64) });

        for (auto& cursor : *reader) {
            const auto& curRow = cursor.GetRow();
            auto url = curRow[FileUrlColumnName].AsString();
            auto fileIndex = curRow[FileIndexColumnName].AsInt64();

            BufferPosition_ = 0;

            TNode keyNode = TNode::CreateMap();
            keyNode[FileIndexColumnName] = fileIndex;

            BlobTableWriter_ = NYtBlobTable::CreateBlobTableWriter(
                writer,
                keyNode,
                blobTableSchema,
                /*firstPartIndex*/ 1,
                /*autoFinishOfWriter*/ false);

            FileSize_ = 0;

            auto stream = Client_.DownloadFile(url);
            while (auto data = NConcurrency::WaitFor(stream->Read()).ValueOrThrow()) {
                DownloadFilePart(data);
            }

            BlobTableWriter_->Finish();

            writer->AddRow(MakeOutputMetadataRow(fileIndex), /*tableIndex*/ 1);
        }
    }

private:
    int FileSize_;
    IFileWriterPtr BlobTableWriter_;
    NHuggingface::THuggingfaceClient Client_;

    // A ring buffer in which we save the current end of the file.
    char RingBuffer_[BufferSize];
    int BufferPosition_;

    void DownloadFilePart(TSharedRef data)
    {
        auto size = std::ssize(data);
        BlobTableWriter_->Write(data.Begin(), size);
        FileSize_ += size;

        if (size > BufferSize) {
            data = data.Slice(size - BufferSize, size);
            size = BufferSize;
        }

        auto restSize = BufferSize - BufferPosition_;
        if (size <= restSize) {
            // One copy is enough.
            // In the case when there is more space between the current write position and the end of the buffer
            // than the size of the data we want to write.
            // For example:
            // ..............
            //     ^ - current write position
            //     .... - data we want to write
            memcpy(RingBuffer_ + BufferPosition_, data.Begin(), size);
            BufferPosition_ += size;
        } else {
            // Two copies are needed.
            // In the case when there is less space between the current write position and the end of the buffer
            // than the size of the data we want to write.
            // So, the data that did not fit at the end will be written at the beginning.
            // For example:
            // ..............
            //             ^ - current write position
            //             ..... - data we want to write
            memcpy(RingBuffer_ + BufferPosition_, data.Begin(), restSize);
            memcpy(RingBuffer_, data.Begin() + restSize, size - restSize);
            BufferPosition_ += size;
        }
        BufferPosition_ %= BufferSize;
    }

    TNode MakeOutputMetadataRow(int fileIndex)
    {
        char metadataSizeData[SizeOfMetadataSize];
        auto metadataSizeStart = (BufferPosition_ + BufferSize - (SizeOfMagicBytes + SizeOfMetadataSize)) % BufferSize;
        for (int i = 0; i < SizeOfMetadataSize; i++) {
            metadataSizeData[i] = RingBuffer_[metadataSizeStart];
            metadataSizeStart++;
            metadataSizeStart = metadataSizeStart % BufferSize;
        }
        int metadataSize = *(reinterpret_cast<int*>(metadataSizeData)) + (SizeOfMagicBytes + SizeOfMetadataSize);
        metadataSize = std::max(DefaultFooterReadSize + SizeOfMagicBytes + SizeOfMetadataSize, metadataSize);
        if (metadataSize > BufferSize) {
            THROW_ERROR_EXCEPTION("Meta data size of parquet file is too big");
        }

        auto metadataStartOffset = (BufferPosition_ + BufferSize - metadataSize) % BufferSize;

        TString metadata;
        metadata.resize(metadataSize);

        auto restSize = BufferSize - metadataStartOffset;
        if (metadataSize <= restSize) {
            // One copy is enough.
            memcpy(metadata.begin(), &(RingBuffer_[metadataStartOffset]), metadataSize);
        } else {
            // Two copies are needed.
            memcpy(metadata.begin(), &(RingBuffer_[metadataStartOffset]), restSize);
            memcpy(metadata.begin() + restSize, &(RingBuffer_[0]), metadataSize - restSize);
        }

        TNode outMetadataRow;
        outMetadataRow[FileIndexColumnName] = fileIndex;
        outMetadataRow[MetadataColumnName] = metadata;
        outMetadataRow[StartMetadataOffsetColumnName] = FileSize_ - metadataSize;
        outMetadataRow[PartIndexColumnName] = 0;

        return outMetadataRow;
    }
};

REGISTER_MAPPER(TDownloadFromHuggingFaceMapper);


class TParseParquetFilesReducer
    : public IRawJob
{
public:
    void Do(const TRawJobContext& context) override
    {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[0]);

        TBufferedInput input(&unbufferedInput);
        TBufferedOutput output(&unbufferedOutput);

        auto reader = CreateTableReader<TNode>(&input);

        const auto& curRow = reader->GetRow();
        auto tableIndex = reader->GetTableIndex();

        YT_VERIFY(tableIndex == 0);

        auto metadata = curRow[MetadataColumnName].AsString();
        auto startIndex = curRow[StartMetadataOffsetColumnName].AsInt64();

        auto stream = std::make_shared<TFileReader>(reader);

        auto parquetAdapter = NFormats::NArrow::CreateParquetAdapter(&metadata, startIndex, stream);

        auto* pool = arrow::default_memory_pool();

        std::unique_ptr<parquet::arrow::FileReader> arrowFileReader;

        NFormats::NArrow::ThrowOnError(parquet::arrow::FileReader::Make(
            pool,
            parquet::ParquetFileReader::Open(parquetAdapter),
            parquet::ArrowReaderProperties{},
            &arrowFileReader));

        auto numRowGroups = arrowFileReader->num_row_groups();

        TArrowOutputStream outputStream(&output);

        std::shared_ptr<arrow::Schema> arrowSchema;
        NFormats::NArrow::ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));

        auto recordBatchWriterOrError = arrow::ipc::MakeStreamWriter(&outputStream, arrowSchema);
        NFormats::NArrow::ThrowOnError(recordBatchWriterOrError.status());
        auto recordBatchWriter = recordBatchWriterOrError.ValueOrDie();
        for (int rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++) {
            std::vector<int> rowGroup = {rowGroupIndex};

            std::shared_ptr<arrow::Table> table;
            NFormats::NArrow::ThrowOnError(arrowFileReader->ReadRowGroups(rowGroup, &table));
            arrow::TableBatchReader tableBatchReader(*table);
            std::shared_ptr<arrow::RecordBatch> batch;
            NFormats::NArrow::ThrowOnError(tableBatchReader.ReadNext(&batch));

            while (batch) {
                NFormats::NArrow::ThrowOnError(recordBatchWriter->WriteRecordBatch(*batch));
                NFormats::NArrow::ThrowOnError(tableBatchReader.ReadNext(&batch));
            }
        }
    }

private:
    class TFileReader
        : public IInputStream
    {
    public:
        explicit TFileReader(TTableReaderPtr<TNode> reader)
            : Reader_(std::move(reader))
        { }

    protected:
        size_t DoRead(void* buf, size_t len) override
        {
            if (Buffer_.size() == Position_) {
                Reader_->Next();
                if (!Reader_->IsValid()) {
                    return 0;
                }

                YT_VERIFY(Reader_->GetTableIndex() == 1);
                const auto& curRow = Reader_->GetRow();
                Buffer_ = curRow[DataColumnName].AsString();
                Position_ = 0;
            }
            auto size = std::min(len, Buffer_.Size() - Position_);
            memcpy(buf, Buffer_.begin() + Position_, size);
            Position_ += size;
            return size;
        }

    private:
        TTableReaderPtr<TNode> Reader_;
        TString Buffer_;
        size_t Position_ = 0;
    };

    class TArrowOutputStream
        : public arrow::io::OutputStream
    {
    public:
        TArrowOutputStream(IOutputStream* outputStream)
            : OutputStream_(outputStream)
        { }

        arrow::Status Write(const void* data, int64_t nbytes) override
        {
            Position_ += nbytes;
            OutputStream_->Write(data, nbytes);
            return arrow::Status::OK();
        }

        arrow::Status Flush() override
        {
            OutputStream_->Flush();
            Position_ = 0;
            return arrow::Status::OK();
        }

        arrow::Status Close() override
        {
            IsClosed_ = true;
            return arrow::Status::OK();
        }

        arrow::Result<int64_t> Tell() const override
        {
            return Position_;
        }

        bool closed() const override
        {
            return IsClosed_;
        }

    private:
        i64 Position_ = 0;
        bool IsClosed_ = false;
        IOutputStream* OutputStream_;
    };
};

REGISTER_RAW_JOB(TParseParquetFilesReducer)

TTableSchema CreateResultTableSchema(IClientPtr ytClient, const TString& metadataOfParquetTable)
{
    // Extract metadata to find out the schema.
    auto reader = ytClient->CreateTableReader<TNode>(metadataOfParquetTable);
    if (!reader->IsValid()) {
        THROW_ERROR_EXCEPTION("Can't read metadata of parquet file");
    }

    auto& row = reader->GetRow();
    auto metadata = row[MetadataColumnName].AsString();
    auto metadataStartOffset = row[StartMetadataOffsetColumnName].AsInt64();

    auto arrowSchema = NFormats::NArrow::CreateArrowSchemaFromParquetMetadata(&metadata, metadataStartOffset);
    return NFormats::NArrow::CreateYtTableSchemaFromArrowSchema(arrowSchema);
}

int UploadParquetsFileToTableFromHuggingface(int argc, const char** argv)
{
    TOpts opts;
    NLastGetopt::TOptsParseResult parseResult(&opts.Opts, argc, argv);

    TString huggingfaceToken = GetEnv("HUGGINGFACE_TOKEN");

    YT_LOG_INFO("Start getting list of parquet files");

    NYT::NHuggingface::THuggingfaceClient huggingfaceClient(huggingfaceToken);

    auto result = huggingfaceClient.GetParquetFileUrls(opts.Dataset, opts.Config, opts.Split);

    YT_LOG_INFO("Successfully downloaded %v parquet files", result.size());

    YT_LOG_INFO("Create table with meta information");

    auto ytClient = NYT::CreateClient(opts.Cluster);

    NYT::TTempTable metaInformationTable(
        ytClient,
        /*prefix*/ {},
        /*path*/ {},
        TCreateOptions().Attributes(TNode()("schema", TTableSchema()
            .AddColumn(TColumnSchema()
                .Name(FileUrlColumnName)
                .Type(VT_STRING, true))
            .AddColumn(TColumnSchema()
                .Name(FileIndexColumnName)
                .Type(VT_INT64, true)).ToNode())));

    auto writer = ytClient->CreateTableWriter<TNode>(metaInformationTable.Name());
    int fileIndex = 0;
    for (const auto& fileName : result) {
        writer->AddRow(TNode()(FileUrlColumnName, fileName)(FileIndexColumnName, fileIndex));
        ++fileIndex;
    }
    writer->Finish();

    YT_LOG_INFO("Create tables with data and meta parquet information from parquet files");

    NYtBlobTable::TBlobTableSchema blobTableSchema;
    blobTableSchema.BlobIdColumns({NYT::TColumnSchema().Name(FileIndexColumnName).Type(VT_INT64)});

    auto createOptions = NYT::TCreateOptions().Attributes(
        NYT::TNode()("schema", blobTableSchema.CreateYtSchema().ToNode()));

    NYT::TTempTable dataTable(ytClient, /*prefix*/ {}, /*path*/ {}, createOptions);
    NYT::TTempTable metadataTable(
        ytClient,
        /*prefix*/ {},
        /*path*/ {},
        TCreateOptions().Attributes(TNode()("schema", TTableSchema()
            .AddColumn(TColumnSchema()
                .Name(FileIndexColumnName)
                .Type(VT_INT64, true))
            .AddColumn(TColumnSchema()
                .Name(PartIndexColumnName)
                .Type(VT_INT64, true))
            .AddColumn(TColumnSchema()
                .Name(MetadataColumnName)
                .Type(VT_STRING, true))
            .AddColumn(TColumnSchema()
                .Name(StartMetadataOffsetColumnName)
                .Type(VT_INT64, true)).ToNode())));

    const TString dataTablePath = dataTable.Name();
    const TString metadataTablePath = metadataTable.Name();

    TNode secureVault;
    secureVault["HUGGINGFACE_TOKEN"] = huggingfaceToken;

    ytClient->Map(
        TMapOperationSpec()
            .AddInput<TNode>(metaInformationTable.Name())
            .AddOutput<TNode>(dataTablePath)
            .AddOutput<TNode>(metadataTablePath),
        new TDownloadFromHuggingFaceMapper(),
        NYT::TOperationOptions()
            .SecureVault(secureVault));

    YT_LOG_INFO("Start sort operation of dataParquetTable and metadataOfParquetTable");

    ytClient->Sort(TSortOperationSpec()
        .SortBy({FileIndexColumnName, PartIndexColumnName})
        .AddInput(dataTablePath)
        .Output(TRichYPath(dataTablePath)));

    ytClient->Sort(TSortOperationSpec()
        .SortBy({FileIndexColumnName, PartIndexColumnName})
        .AddInput(metadataTablePath)
        .Output(metadataTablePath));

    YT_LOG_INFO("Start reduce operation: filling rows in the result table");

    ytClient->RawReduce(
        TRawReduceOperationSpec()
            .ReduceBy({FileIndexColumnName})
            .SortBy({FileIndexColumnName, PartIndexColumnName})
            .AddInput(metadataTablePath)
            .AddInput(dataTablePath)
            .AddOutput(TRichYPath(opts.ResultTable)
                .Schema(CreateResultTableSchema(ytClient, metadataTablePath)))
            .InputFormat(TFormat(TNode("yson")))
            .OutputFormat(TFormat(TNode("arrow"))),
        new TParseParquetFilesReducer);

    YT_LOG_INFO("Parquet files were successfully uploaded to the table with path %v", opts.ResultTable);
    return 0;
}

int UploadParquetsFileToTableFromS3(int /*argc*/, const char** /*argv*/) {
    THROW_ERROR_EXCEPTION("Not implemented");
    return 1;
}

void UploadParquetsFileToTable(int argc, const char** argv)
{
    TModChooser modChooser;

    modChooser.AddMode(
        "huggingface",
        UploadParquetsFileToTableFromHuggingface,
        "-- import parquet files from huggingface"
    );
    modChooser.AddMode(
        "s3",
        UploadParquetsFileToTableFromS3,
        "-- import parquet files from s3"
    );

    modChooser.Run(argc, argv);
}

} // namespace NYT::NTools::NImporter

// Huggingface token must be placed in an environment variable $HUGGINGFACE_TOKEN
// Usage example: ./importer huggingface \
//     --proxy <cluster-name> \
//     --dataset Deysi/spanish-chinese \
//     --split train \
//     --output //tmp/result_parquet_table
// or
//     ./importer huggingface \
//     --proxy <cluster-name> \
//     --dataset Deysi/spanish-chinese \
//     --config not_default \
//     --split train \
//     --output //tmp/result_parquet_table

int main(int argc, const char** argv)
{
    NYT::Initialize();
    try {
        NYT::NTools::NImporter::UploadParquetsFileToTable(argc, argv);
    } catch (const std::exception& e) {
        Cerr << ToString(NYT::TError(e));
        return 1;
    }

    return 0;
}
