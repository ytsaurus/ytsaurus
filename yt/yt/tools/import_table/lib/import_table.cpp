#include "import_table.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

#include <yt/yt/library/s3/client.h>

#include <yt/yt/library/huggingface_client/client.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>

#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <yt/cpp/mapreduce/library/blob_table/blob_table.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <yt/cpp/mapreduce/library/table_schema/arrow.h>

#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <library/cpp/yson/node/node.h>

#include <library/cpp/json/json_writer.h>

#include <util/system/env.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NTools::NImporter {

using namespace NConcurrency;
using namespace NYtBlobTable;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Importer");

////////////////////////////////////////////////////////////////////////////////

constexpr int BufferSize = 4_MB;
constexpr int DefaultFooterReadSize = 64_KB;
constexpr int SizeOfMetadataSize = 4;
constexpr int SizeOfMagicBytes = 4;

const TString MetadataColumnName = "metadata";
const TString StartMetadataOffsetColumnName = "start_metadata_offset";
const TString PartIndexColumnName = "part_index";
const TString FileIdColumnName = "file_id";
const TString FileIndexColumnName = "file_index";
const TString DataColumnName = "data";

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> GetEnvOrNull(const TString& key)
{
    auto res = GetEnv(key, "");
    if (res == "") {
        return std::nullopt;
    }
    return res;
}

////////////////////////////////////////////////////////////////////////////////

struct THuggingfaceConfig
{
    std::optional<TString> UrlOverride;

    Y_SAVELOAD_DEFINE(UrlOverride);
};

struct TS3Config
{
    TString Url;
    TString Region;
    TString Bucket;

    Y_SAVELOAD_DEFINE(
        Url,
        Region,
        Bucket);
};

struct TSourceConfig
{
    std::optional<TS3Config> S3Config;
    std::optional<THuggingfaceConfig> HuggingfaceConfig;

    Y_SAVELOAD_DEFINE(
        S3Config,
        HuggingfaceConfig);
};

////////////////////////////////////////////////////////////////////////////////

void ExtractKeys(std::vector<TString>& keys, const std::vector<NS3::TObject>& objects)
{
    for (const auto& value : objects) {
        keys.push_back(value.Key);
    }
}

NS3::IClientPtr CreateS3Client(
    const TS3Config& s3Config,
    const TString& accessKeyId,
    const TString& secretAccessKey)
{
    auto clientConfig = New<NS3::TS3ClientConfig>();

    clientConfig->Url = s3Config.Url;
    clientConfig->Region = s3Config.Region;
    clientConfig->Bucket = s3Config.Bucket;
    clientConfig->AccessKeyId = accessKeyId;
    clientConfig->SecretAccessKey = secretAccessKey;

    auto poller = CreateThreadPoolPoller(1, "S3Poller");
    auto client = NS3::CreateClient(
        std::move(clientConfig),
        poller,
        poller->GetInvoker());

    WaitFor(client->Start())
        .ThrowOnError();
    return client;
}

std::vector<TString> GetListFilesKeysFromS3(
    const TS3Config& s3Config,
    const TString& accessKeyId,
    const TString& secretAccessKey,
    const TString& prefix)
{
    auto s3Client =  CreateS3Client(
        s3Config,
        accessKeyId,
        secretAccessKey);

    std::vector<TString> keys;
    NS3::TListObjectsResponse response({ .NextContinuationToken = std::nullopt });
    do {
        response = WaitFor(s3Client->ListObjects({
            .Prefix = prefix,
            .Bucket = s3Config.Bucket,
            .ContinuationToken = response.NextContinuationToken,
        })).ValueOrThrow();
        ExtractKeys(keys, response.Objects);
    } while (response.NextContinuationToken);

    return keys;
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDownloader)

struct IDownloader
    : public TRefCounted
{
   virtual IAsyncZeroCopyInputStreamPtr GetFile(const TString& fileId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDownloader)

class TS3Downloader
    : public IDownloader
{
public:
    TS3Downloader(
        const TS3Config& s3Config,
        const TString& accessKeyId,
        const TString& secretAccessKey)
        : Client_(CreateS3Client(
            s3Config,
            accessKeyId,
            secretAccessKey))
        , Bucket_(s3Config.Bucket)
    { }

    IAsyncZeroCopyInputStreamPtr GetFile(const TString& fileId) override
    {
        return WaitFor(Client_->GetObjectStream({
            .Bucket = Bucket_,
            .Key = fileId,
        })).ValueOrThrow().Stream;
    }

private:
    NS3::IClientPtr Client_;
    TString Bucket_;
};

class THuggingfaceDownloader
    : public IDownloader
{
public:
    THuggingfaceDownloader(const TString& token, const std::optional<TString>& url)
        : Client_(
            token,
            CreateThreadPoolPoller(1, "HuggingfacePoller"),
            url)
    { }

    IAsyncZeroCopyInputStreamPtr GetFile(const TString& fileId) override
    {
        return Client_.DownloadFile(fileId);
    }

private:
    NHuggingface::THuggingfaceClient Client_;
};

IDownloaderPtr CreateDownloader(const TSourceConfig& sourceConfig)
{
    if (sourceConfig.S3Config) {
        TString accessKeyId = GetEnv("YT_SECURE_VAULT_ACCESS_KEY_ID");
        TString secretAccessKey = GetEnv("YT_SECURE_VAULT_SECRET_ACCESS_KEY");
        return New<TS3Downloader>(
            *sourceConfig.S3Config,
            accessKeyId,
            secretAccessKey);
    } else if (sourceConfig.HuggingfaceConfig) {
        TString huggingfaceToken = GetEnv("YT_SECURE_VAULT_HUGGINGFACE_TOKEN");
        return New<THuggingfaceDownloader>(huggingfaceToken, sourceConfig.HuggingfaceConfig->UrlOverride);
    } else {
        THROW_ERROR_EXCEPTION("The importer source is not defined");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDownloadMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TDownloadMapper() = default;

    explicit TDownloadMapper(TSourceConfig sourceConfig)
        : SourceConfig_(std::move(sourceConfig))
    { }

    void Start(TWriter* /*writer*/) override
    {
        Downloader_ = CreateDownloader(SourceConfig_);
    }

    void Do(TReader* reader, TWriter* writer) override
    {
        TBlobTableSchema blobTableSchema;
        blobTableSchema.BlobIdColumns({ TColumnSchema().Name(FileIndexColumnName).Type(VT_INT64) });

        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            auto fileId = row[FileIdColumnName].AsString();
            auto fileIndex = row[FileIndexColumnName].AsInt64();

            BufferPosition_ = 0;

            TNode keyNode = TNode::CreateMap();
            keyNode[FileIndexColumnName] = fileIndex;

            BlobTableWriter_ = CreateBlobTableWriter(
                writer,
                keyNode,
                blobTableSchema,
                /*firstPartIndex*/ 1,
                /*autoFinishOfWriter*/ false);

            FileSize_ = 0;

            auto stream = Downloader_->GetFile(fileId);
            while (auto data = WaitFor(stream->Read()).ValueOrThrow()) {
                DownloadFilePart(data);
            }

            BlobTableWriter_->Finish();

            writer->AddRow(MakeOutputMetadataRow(fileIndex), /*tableIndex*/ 1);
        }
    }

    Y_SAVELOAD_JOB(SourceConfig_);

private:
    int FileSize_;
    IFileWriterPtr BlobTableWriter_;
    TSourceConfig SourceConfig_;
    IDownloaderPtr Downloader_;

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
            THROW_ERROR_EXCEPTION("Metadata size of Parquet file is too big");
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

REGISTER_MAPPER(TDownloadMapper);


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

        auto reader = TNodeTableReader(MakeIntrusive<NYT::NDetail::TInputStreamProxy>(&input));

        while (reader.IsValid()) {
            const auto& row = reader.GetRow();
            auto tableIndex = reader.GetTableIndex();

            YT_VERIFY(tableIndex == 0);

            auto metadata = row[MetadataColumnName].AsString();
            auto startIndex = row[StartMetadataOffsetColumnName].AsInt64();

            auto stream = std::make_shared<TFileReader>(&reader);

            auto parquetAdapter = NArrow::CreateParquetAdapter(&metadata, startIndex, stream);

            auto* pool = arrow::default_memory_pool();

            std::unique_ptr<parquet::arrow::FileReader> arrowFileReader;

            NArrow::ThrowOnError(parquet::arrow::FileReader::Make(
                pool,
                parquet::ParquetFileReader::Open(parquetAdapter),
                parquet::ArrowReaderProperties{},
                &arrowFileReader));

            auto numRowGroups = arrowFileReader->num_row_groups();

            TArrowOutputStream outputStream(&output);

            std::shared_ptr<arrow::Schema> arrowSchema;
            NArrow::ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));

            auto recordBatchWriterOrError = arrow::ipc::MakeStreamWriter(&outputStream, arrowSchema);
            NArrow::ThrowOnError(recordBatchWriterOrError.status());
            auto recordBatchWriter = recordBatchWriterOrError.ValueOrDie();
            for (int rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++) {
                std::vector<int> rowGroup = {rowGroupIndex};

                std::shared_ptr<arrow::Table> table;
                NArrow::ThrowOnError(arrowFileReader->ReadRowGroups(rowGroup, &table));
                arrow::TableBatchReader tableBatchReader(*table);
                std::shared_ptr<arrow::RecordBatch> batch;
                NArrow::ThrowOnError(tableBatchReader.ReadNext(&batch));

                while (batch) {
                    NArrow::ThrowOnError(recordBatchWriter->WriteRecordBatch(*batch));
                    NArrow::ThrowOnError(tableBatchReader.ReadNext(&batch));
                }
            }
            // EOS marker.
            ui32 zero = 0;
            output.Write(&zero, sizeof(ui32));
            reader.NextKey();
            if (reader.IsValid()) {
                reader.Next();
            }
        }
    }

private:
    class TFileReader
        : public IInputStream
    {
    public:
        explicit TFileReader(TNodeTableReader* reader)
            : Reader_(reader)
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
                const auto& row = Reader_->GetRow();
                Buffer_ = row[DataColumnName].AsString();
                Position_ = 0;
            }
            auto size = std::min(len, Buffer_.Size() - Position_);
            memcpy(buf, Buffer_.begin() + Position_, size);
            Position_ += size;
            return size;
        }

    private:
        TNodeTableReader* Reader_;
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

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateResultTableSchema(IClientPtr ytClient, const TString& metadataOfParquetTable)
{
    // Extract metadata to find out the schema.
    auto reader = ytClient->CreateTableReader<TNode>(metadataOfParquetTable);
    if (!reader->IsValid()) {
        THROW_ERROR_EXCEPTION("Can't read metadata of Parquet file");
    }

    auto& row = reader->GetRow();
    auto metadata = row[MetadataColumnName].AsString();
    auto metadataStartOffset = row[StartMetadataOffsetColumnName].AsInt64();

    auto arrowSchema = NArrow::CreateArrowSchemaFromParquetMetadata(&metadata, metadataStartOffset);
    return CreateYTTableSchemaFromArrowSchema(arrowSchema);
}

void ImportParquetFilesFromSource(
    const TString& proxy,
    const std::vector<TString>& fileIds,
    const TString& resultTable,
    const TSourceConfig& sourceConfig)
{
    auto ytClient = NYT::CreateClient(proxy);

    YT_LOG_INFO("Create table with meta information");
    TTempTable metaInformationTable(
        ytClient,
        /*prefix*/ TString(),
        /*path*/ TString(),
        TCreateOptions().Attributes(TNode()("schema", TTableSchema()
            .AddColumn(TColumnSchema()
                .Name(FileIdColumnName)
                .Type(VT_STRING, true))
            .AddColumn(TColumnSchema()
                .Name(FileIndexColumnName)
                .Type(VT_INT64, true)).ToNode())));

    auto writer = ytClient->CreateTableWriter<TNode>(metaInformationTable.Name());
    int fileIndex = 0;
    for (const auto& fileName : fileIds) {
        writer->AddRow(TNode()(FileIdColumnName, fileName)(FileIndexColumnName, fileIndex));
        ++fileIndex;
    }
    writer->Finish();

    YT_LOG_INFO("Create tables with data and meta Parquet information from Parquet files");

    TBlobTableSchema blobTableSchema;
    blobTableSchema.BlobIdColumns({TColumnSchema().Name(FileIndexColumnName).Type(VT_INT64)});

    auto createOptions = TCreateOptions().Attributes(
        TNode()("schema", blobTableSchema.CreateYtSchema().ToNode()));

    TTempTable dataTable(
        ytClient,
        /*prefix*/ TString(),
        /*path*/ TString(),
        createOptions);

    TTempTable metadataTable(
        ytClient,
        /*prefix*/ TString(),
        /*path*/ TString(),
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

    TOperationOptions operationOptions;
    TNode secureVault;

    if (sourceConfig.S3Config) {
        secureVault["ACCESS_KEY_ID"] = GetEnv("ACCESS_KEY_ID");
        secureVault["SECRET_ACCESS_KEY"] = GetEnv("SECRET_ACCESS_KEY");
    } else if (sourceConfig.HuggingfaceConfig) {
        secureVault["HUGGINGFACE_TOKEN"] = GetEnv("HUGGINGFACE_TOKEN");
    } else {
        THROW_ERROR_EXCEPTION("The importer source is not defined");
    }

    operationOptions.SecureVault(secureVault);
    ytClient->Map(
        TMapOperationSpec()
            .AddInput<TNode>(metaInformationTable.Name())
            .AddOutput<TNode>(dataTablePath)
            .AddOutput<TNode>(metadataTablePath),
        new TDownloadMapper(sourceConfig),
        operationOptions);

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
            .AddOutput(TRichYPath(resultTable)
                .Schema(CreateResultTableSchema(ytClient, metadataTablePath)))
            .InputFormat(TFormat(TNode("yson")))
            .OutputFormat(TFormat(TNode("arrow"))),
        new TParseParquetFilesReducer);

    YT_LOG_INFO("Parquet files were successfully uploaded to the table with path %v", resultTable);
}

////////////////////////////////////////////////////////////////////////////////

void ImportParquetFilesFromS3(
    const TString& proxy,
    const TString& url,
    const TString& region,
    const TString& bucket,
    const TString& prefix,
    const TString& resultTable)
{
    TString accessKeyId = GetEnv("ACCESS_KEY_ID");
    TString secretAccessKey = GetEnv("SECRET_ACCESS_KEY");

    TS3Config s3Config({
        .Url = url,
        .Region = region,
        .Bucket = bucket,
    });

    auto fileKeys = GetListFilesKeysFromS3(s3Config, accessKeyId, secretAccessKey, prefix);

    YT_LOG_INFO("Successfully received %v file names from S3", fileKeys.size());

    ImportParquetFilesFromSource(
        proxy,
        fileKeys,
        resultTable,
        TSourceConfig({ .S3Config = s3Config }));
}

void ImportParquetFilesFromHuggingface(
    const TString& proxy,
    const TString& dataset,
    const TString& config,
    const TString& split,
    const TString& resultTable,
    const std::optional<TString>& urlOverride
)
{
    YT_LOG_INFO("Start getting list of files");
    auto huggingfaceToken = GetEnvOrNull("HUGGINGFACE_TOKEN");

    auto poller = CreateThreadPoolPoller(1, "HuggingfacePoller");
    NHuggingface::THuggingfaceClient huggingfaceClient(huggingfaceToken, poller, urlOverride);

    auto fileIds = huggingfaceClient.GetParquetFileUrls(dataset, config, split);

    YT_LOG_INFO("Successfully received %v file names from huggingface", fileIds.size());

    ImportParquetFilesFromSource(
        proxy,
        fileIds,
        resultTable,
        TSourceConfig{
            .HuggingfaceConfig = THuggingfaceConfig{
                .UrlOverride = urlOverride
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTools::NImporter
