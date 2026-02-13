#include "import_table.h"

#include "config.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/arrow_adapter/arrow.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/library/s3/client.h>

#include <yt/yt/library/huggingface_client/client.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/re2/re2.h>

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
#include <util/system/execpath.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/writer.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/adapters/orc/adapter.h>

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

namespace NYT::NTools::NImporter {

using namespace NArrow;
using namespace NConcurrency;
using namespace NRe2;
using namespace NYson;
using namespace NYtBlobTable;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Importer");

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultRingBufferMetadataWeight = 4_MB;
constexpr int DefaultFooterReadSize = 64_KB;
constexpr int SizeOfMetadataWeight = 4;
constexpr int SizeOfMagicBytes = 4;

const std::string MetadataColumnName = "metadata";
const std::string StartMetadataOffsetColumnName = "start_metadata_offset";
const std::string PartIndexColumnName = "part_index";
const std::string FileIdColumnName = "file_id";
const std::string FileIndexColumnName = "file_index";
const std::string DataColumnName = "data";
const std::string OutputTableIndexColumnName = "output_table_index";

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
    std::string Url;
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
    EFileFormat Format;

    Y_SAVELOAD_DEFINE(
        S3Config,
        HuggingfaceConfig,
        Format);
};

////////////////////////////////////////////////////////////////////////////////

void ExtractKeys(std::vector<TString>& keys, const std::vector<NS3::TObject>& objects)
{
    for (const auto& value : objects) {
        keys.push_back(value.Key);
    }
}

NS3::IClientPtr CreateS3Client(
    TS3Config s3Config,
    TString accessKeyId,
    TString secretAccessKey)
{
    auto credentialProvider = NS3::CreateStaticCredentialProvider(std::move(accessKeyId), std::move(secretAccessKey));
    auto clientConfig = New<NS3::TS3ClientConfig>();

    clientConfig->Url = std::move(s3Config.Url);
    clientConfig->Region = std::move(s3Config.Region);

    auto sslConfig = NYT::New<NYT::NCrypto::TSslContextConfig>();
    sslConfig->InsecureSkipVerify = true;

    auto poller = CreateThreadPoolPoller(1, "S3Poller");
    auto client = NS3::CreateClient(
        std::move(clientConfig),
        std::move(credentialProvider),
        sslConfig,
        poller,
        poller->GetInvoker());

    WaitFor(client->Start())
        .ThrowOnError();
    return client;
}

std::vector<TString> GetListFilesKeysFromS3(
    const TS3Config& s3Config,
    TString accessKeyId,
    TString secretAccessKey,
    TString prefix)
{
    auto s3Client = CreateS3Client(
        s3Config,
        std::move(accessKeyId),
        std::move(secretAccessKey));

    std::vector<TString> keys;
    NS3::TListObjectsResponse response({ .NextContinuationToken = std::nullopt });
    do {
        response = WaitFor(s3Client->ListObjects({
            .Prefix = std::move(prefix),
            .Bucket = s3Config.Bucket,
            .ContinuationToken = response.NextContinuationToken,
        })).ValueOrThrow();
        ExtractKeys(keys, response.Objects);
    } while (response.NextContinuationToken);

    return keys;
}

////////////////////////////////////////////////////////////////////////////////

class TOrcInputStreamAdapter
    : public orc::InputStream
{
public:
    explicit TOrcInputStreamAdapter(TRingBuffer* buffer)
        : Buffer_(buffer)
    { }

    uint64_t getLength() const override
    {
        return Buffer_->GetEndPosition();
    }

    uint64_t getNaturalReadSize() const override
    {
        return 1;
    }

    void read(void* buf, uint64_t length, uint64_t offset) override
    {
        Buffer_->Read(offset, length, (char*)buf);
    }

    const std::string& getName() const override
    {
        static std::string name = "OrcInputStreamAdapter";
        return name;
    }

private:
    TRingBuffer* const Buffer_;
};

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
        TString accessKeyId,
        TString secretAccessKey)
        : Client_(CreateS3Client(
            s3Config,
            std::move(accessKeyId),
            std::move(secretAccessKey)))
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

    explicit TDownloadMapper(TSourceConfig sourceConfig, i64 maxMetadataRowWeight, TString serializedSingletonsConfig)
        : SourceConfig_(std::move(sourceConfig))
        , MaxMetadataRowWeight_(maxMetadataRowWeight)
        , SerializedSingletonsConfig_(std::move(serializedSingletonsConfig))
    { }

    void Start(TWriter* /*writer*/) override
    {
        auto config = ConvertTo<TSingletonsConfigPtr>(NYson::TYsonString(SerializedSingletonsConfig_));
        ConfigureSingletons(config);
        Downloader_ = CreateDownloader(SourceConfig_);
    }

    void Do(TReader* reader, TWriter* writer) override
    {
        TBlobTableSchema blobTableSchema;
        // TODO(babenko): migrate to std::string
        blobTableSchema.BlobIdColumns({TColumnSchema().Name(TString(FileIndexColumnName)).Type(VT_INT64) });

        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            auto fileId = row[FileIdColumnName].AsString();
            auto fileIndex = row[FileIndexColumnName].AsInt64();

            BufferPosition_ = 0;

            TNode keyNode = TNode::CreateMap();
            keyNode[FileIndexColumnName] = fileIndex;

            FileSize_ = 0;

            auto metadataWeight = GetMetadataWeight(fileId);
            auto firstDataPartIndex = (metadataWeight / MaxMetadataRowWeight_) + 2;
            auto stream = Downloader_->GetFile(fileId);
            // A ring buffer in which we save the current end of the file.
            TRingBuffer ringBuffer(metadataWeight);

            BlobTableWriter_ = CreateBlobTableWriter(
                writer,
                keyNode,
                blobTableSchema,
                /*firstPartIndex*/ firstDataPartIndex,
                /*autoFinishOfWriter*/ false);

            while (auto data = WaitFor(stream->Read()).ValueOrThrow()) {
                DownloadFilePart(data, ringBuffer);
            }

            BlobTableWriter_->Finish();

            MakeOutputMetadataRow(fileIndex, metadataWeight, writer, ringBuffer);
        }
    }

    Y_SAVELOAD_JOB(SourceConfig_, SerializedSingletonsConfig_, MaxMetadataRowWeight_);

private:
    i64 FileSize_;
    IFileWriterPtr BlobTableWriter_;
    TSourceConfig SourceConfig_;
    i64 MaxMetadataRowWeight_;
    TString SerializedSingletonsConfig_;
    IDownloaderPtr Downloader_;

    int BufferPosition_;

    i64 GetMetadataWeight(const TString& fileId)
    {
        auto stream = Downloader_->GetFile(fileId);
        i64 fileSize = 0;
        TRingBuffer metadataRingBuffer(DefaultRingBufferMetadataWeight);
        while (auto data = WaitFor(stream->Read()).ValueOrThrow()) {
            fileSize += std::ssize(data);
            NArrow::ThrowOnError(metadataRingBuffer.Write(data));
        }

        switch (SourceConfig_.Format) {
            case EFileFormat::Parquet: {
                char metadataWeightData[SizeOfMetadataWeight];
                metadataRingBuffer.Read(fileSize - (SizeOfMagicBytes + SizeOfMetadataWeight), SizeOfMetadataWeight, metadataWeightData);
                i64 metadataWeight = *(reinterpret_cast<int*>(metadataWeightData)) + (SizeOfMagicBytes + SizeOfMetadataWeight);
                metadataWeight = std::max(static_cast<i64>(DefaultFooterReadSize + SizeOfMagicBytes + SizeOfMetadataWeight), metadataWeight);
                return metadataWeight;
            }
            case EFileFormat::Orc: {
                std::unique_ptr<orc::InputStream> stream = std::make_unique<TOrcInputStreamAdapter>(&metadataRingBuffer);
                orc::ReaderOptions option;
                auto reader = orc::createReader(std::move(stream), option);
                return std::max(static_cast<ui64>(DefaultFooterReadSize), fileSize - reader->getContentLength());
            }
            default: {
                YT_ABORT();
            }
        }
    }

    void DownloadFilePart(TSharedRef data, TRingBuffer& ringBuffer)
    {
        auto size = std::ssize(data);
        BlobTableWriter_->Write(data.Begin(), size);
        FileSize_ += size;

        NArrow::ThrowOnError(ringBuffer.Write(data));
    }

    void MakeOutputMetadataRow(int fileIndex, i64 metadataWeight, TWriter* writer, TRingBuffer& ringBuffer)
    {
        metadataWeight = std::min(FileSize_, metadataWeight);

        TString metadata;
        metadata.resize(metadataWeight);

        ringBuffer.Read(std::max(static_cast<i64>(0), FileSize_ - metadataWeight), std::min(metadataWeight, FileSize_), metadata.begin());

        i64 partIndex = 0;
        i64 currentSize = metadataWeight;
        i64 metadataOffset = 0;
        while (currentSize > 0) {
            i64 partSize = std::min(static_cast<i64>(MaxMetadataRowWeight_), currentSize);
            TNode outMetadataRow;

            outMetadataRow[FileIndexColumnName] = fileIndex;
            outMetadataRow[MetadataColumnName] = metadata.substr(metadataOffset, partSize);
            outMetadataRow[StartMetadataOffsetColumnName] = FileSize_ - metadataWeight;
            outMetadataRow[PartIndexColumnName] = partIndex;

            writer->AddRow(outMetadataRow, /*tableIndex*/ 1);

            currentSize -= partSize;
            metadataOffset += partSize;
            ++partIndex;
        }
    }
};

REGISTER_MAPPER(TDownloadMapper);

////////////////////////////////////////////////////////////////////////////////

class TRecordBatchReaderOrcAdapter
    : public arrow20::RecordBatchReader
{
public:
    TRecordBatchReaderOrcAdapter(const TArrowRandomAccessFilePtr& stream, arrow20::MemoryPool* pool)
    {
        auto readerOrError = arrow20::adapters::orc::ORCFileReader::Open(
            stream,
            pool);
        ThrowOnError(readerOrError.status());
        Reader_ = std::move(readerOrError.ValueOrDie());
        StripeCount_ = Reader_->NumberOfStripes();
    }

    TArrowSchemaPtr schema() const override
    {
        auto resultSchemaOrError = Reader_->ReadSchema();
        ThrowOnError(resultSchemaOrError.status());
        return resultSchemaOrError.ValueOrDie();
    }

    arrow20::Status ReadNext(std::shared_ptr<arrow20::RecordBatch>* batch) override
    {
        if (CurrentStripeIndex_ < StripeCount_) {
            auto batchOrError = Reader_->ReadStripe(CurrentStripeIndex_);
            ThrowOnError(batchOrError.status());
            *batch = batchOrError.ValueOrDie();
            ++CurrentStripeIndex_;
        } else {
            *batch = nullptr;
        }
        return arrow20::Status::OK();
    }

private:
    std::unique_ptr<arrow20::adapters::orc::ORCFileReader> Reader_;
    int StripeCount_ = 0;
    int CurrentStripeIndex_ = 0;
};

class TRecordBatchReaderParquetAdapter
    : public arrow20::RecordBatchReader
{
public:
    TRecordBatchReaderParquetAdapter(const TArrowRandomAccessFilePtr& stream, arrow20::MemoryPool* pool)
    {
        NArrow::ThrowOnError(parquet20::arrow20::FileReader::Make(
            pool,
            parquet20::ParquetFileReader::Open(stream),
            parquet20::ArrowReaderProperties{},
            &ArrowFileReader_));

        NArrow::ThrowOnError(ArrowFileReader_->GetSchema(&ArrowSchema_));
        RowGroupCount_ = ArrowFileReader_->num_row_groups();
        if (RowGroupCount_ > 0) {
            std::vector<int> rowGroup = {CurrentRowGroupIndex_};
            NArrow::ThrowOnError(ArrowFileReader_->ReadRowGroups(rowGroup, &Table_));
            TableBatchReader_ = std::make_shared<arrow20::TableBatchReader>(*Table_);
            ++CurrentRowGroupIndex_;
        }
    }

    TArrowSchemaPtr schema() const override
    {
        return ArrowSchema_;
    }

    arrow20::Status ReadNext(std::shared_ptr<arrow20::RecordBatch>* batch) override
    {
        if (RowGroupCount_ > 0) {
            NArrow::ThrowOnError(TableBatchReader_->ReadNext(batch));
            if (!(*batch) && CurrentRowGroupIndex_ < RowGroupCount_) {
                std::vector<int> rowGroup = {CurrentRowGroupIndex_};
                NArrow::ThrowOnError(ArrowFileReader_->ReadRowGroups(rowGroup, &Table_));
                TableBatchReader_ = std::make_shared<arrow20::TableBatchReader>(*Table_);
                NArrow::ThrowOnError(TableBatchReader_->ReadNext(batch));
                ++CurrentRowGroupIndex_;
            }
        }
        return arrow20::Status::OK();
    }

private:
    std::unique_ptr<parquet20::arrow20::FileReader> ArrowFileReader_;
    TArrowSchemaPtr ArrowSchema_;
    std::shared_ptr<arrow20::TableBatchReader> TableBatchReader_;
    std::shared_ptr<arrow20::Table> Table_;
    int RowGroupCount_;
    int CurrentRowGroupIndex_ = 0;
};

std::shared_ptr<arrow20::RecordBatchReader> MakeRecordBatchReaderAdapter(
    const TArrowRandomAccessFilePtr& stream,
    arrow20::MemoryPool* pool,
    EFileFormat fileFormat)
{
    switch (fileFormat) {
        case EFileFormat::Orc:
            return std::make_shared<TRecordBatchReaderOrcAdapter>(stream, pool);
        case EFileFormat::Parquet:
            return std::make_shared<TRecordBatchReaderParquetAdapter>(stream, pool);
    }
}

TArrowRandomAccessFilePtr MakeFormatStreamAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    const std::shared_ptr<IInputStream>& reader,
    EFileFormat fileFormat)
{
    switch (fileFormat) {
        case EFileFormat::Orc:
        {
            auto maxStripeSize = NArrow::GetMaxStripeSize(metadata, startMetadataOffset);
            return NArrow::CreateOrcAdapter(metadata, startMetadataOffset, maxStripeSize, std::move(reader));
        }
        case EFileFormat::Parquet:
        {
            return NArrow::CreateParquetAdapter(metadata, startMetadataOffset, std::move(reader));
        }
    }
}

class TParseParquetFilesReducer
    : public IRawJob
{
public:
    TParseParquetFilesReducer() = default;

    explicit TParseParquetFilesReducer(TSourceConfig sourceConfig)
        : SourceConfig_(std::move(sourceConfig))
    { }

    void Do(const TRawJobContext& context) override
    {
        TUnbufferedFileInput unbufferedInput(context.GetInputFile());
        TBufferedInput input(&unbufferedInput);

        auto reader = TNodeTableReader(MakeIntrusive<NYT::NDetail::TInputStreamProxy>(&input));

        while (reader.IsValid()) {
            const auto& row = reader.GetRow();
            YT_VERIFY(reader.GetTableIndex() == 0);
            TString metadata;
            auto startIndex = row[StartMetadataOffsetColumnName].AsInt64();

            while (reader.GetTableIndex() == 0) {
                const auto& currentRow = reader.GetRow();
                metadata += currentRow[MetadataColumnName].AsString();

                reader.Next();
            }

            const auto& outputTableInformationRow = reader.GetRow();
            YT_VERIFY(reader.GetTableIndex() == 1);
            auto outputTableIndex = outputTableInformationRow[OutputTableIndexColumnName].AsInt64();

            TUnbufferedFileOutput unbufferedOutput(context.GetOutputFileList()[outputTableIndex]);
            TBufferedOutput output(&unbufferedOutput);

            auto stream = std::make_shared<TFileReader>(&reader);
            auto formatAdapter = MakeFormatStreamAdapter(&metadata, startIndex, stream, SourceConfig_.Format);
            auto* pool = arrow20::default_memory_pool();

            auto batchReader = MakeRecordBatchReaderAdapter(formatAdapter, pool, SourceConfig_.Format);

            TArrowOutputStream outputStream(&output);

            auto recordBatchWriterOrError = arrow20::ipc::MakeStreamWriter(&outputStream, batchReader->schema());
            NArrow::ThrowOnError(recordBatchWriterOrError.status());
            auto recordBatchWriter = recordBatchWriterOrError.ValueOrDie();

            std::shared_ptr<arrow20::RecordBatch> batch;
            NArrow::ThrowOnError(batchReader->ReadNext(&batch));

            while (batch) {
                NArrow::ThrowOnError(recordBatchWriter->WriteRecordBatch(*batch));
                NArrow::ThrowOnError(batchReader->ReadNext(&batch));
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

    Y_SAVELOAD_JOB(SourceConfig_);

private:
    TSourceConfig SourceConfig_;

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

                YT_VERIFY(Reader_->GetTableIndex() == 2);
                const auto& row = Reader_->GetRow();
                Buffer_ = row[DataColumnName].AsString();
                Position_ = 0;
            }
            auto size = std::min(len, Buffer_.size() - Position_);
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
        : public arrow20::io::OutputStream
    {
    public:
        TArrowOutputStream(IOutputStream* outputStream)
            : OutputStream_(outputStream)
        { }

        arrow20::Status Write(const void* data, int64_t nbytes) override
        {
            Position_ += nbytes;
            OutputStream_->Write(data, nbytes);
            return arrow20::Status::OK();
        }

        arrow20::Status Flush() override
        {
            OutputStream_->Flush();
            Position_ = 0;
            return arrow20::Status::OK();
        }

        arrow20::Status Close() override
        {
            IsClosed_ = true;
            return arrow20::Status::OK();
        }

        arrow20::Result<int64_t> Tell() const override
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

std::vector<TTempTable> CreateOutputParserTables(
    IClientPtr ytClient,
    const TString& metadataTablePath,
    const TString& outputInformationTablePath,
    EFileFormat fileFormat)
{
    YT_LOG_INFO("Prepare information about output tables for reduce operation");

    std::vector<TTempTable> outputParserTables;

    auto reader = ytClient->CreateTableReader<TNode>(metadataTablePath);

    TTableSchema prevSchema;
    auto outputInformationTableWriter = ytClient->CreateTableWriter<TNode>(outputInformationTablePath);
    int currentOutputTableIndexColumnName;

    while (reader->IsValid()) {
        const auto& row = reader->GetRow();
        TString metadata;
        auto currentFileIndex = row[FileIndexColumnName].AsInt64();
        auto metadataStartOffset = row[StartMetadataOffsetColumnName].AsInt64();
        i64 lastDataPartIndex = 0;
        while (reader->IsValid() && currentFileIndex == (reader->GetRow())[FileIndexColumnName].AsInt64()) {
            const auto& currentRow = reader->GetRow();
            metadata += currentRow[MetadataColumnName].AsString();
            lastDataPartIndex = currentRow[PartIndexColumnName].AsInt64();
            reader->Next();
        }

        TArrowSchemaPtr arrowSchema;
        switch (fileFormat) {
            case EFileFormat::Orc:
            {
                arrowSchema = NArrow::CreateArrowSchemaFromOrcMetadata(&metadata, metadataStartOffset);
                break;
            }
            case EFileFormat::Parquet:
            {
                arrowSchema = NArrow::CreateArrowSchemaFromParquetMetadata(&metadata, metadataStartOffset);
                break;
            }
        }
        auto currentSchema = CreateYTTableSchemaFromArrowSchema(arrowSchema);

        if (prevSchema != currentSchema) {
            currentOutputTableIndexColumnName = std::ssize(outputParserTables);
            TTempTable outputTable(
                ytClient,
                /*prefix*/ TString(),
                /*path*/ TString(),
                TCreateOptions().Attributes(TNode()("schema", currentSchema.ToNode())));
            outputParserTables.push_back(std::move(outputTable));
        }
        TNode outputRow;
        outputRow[FileIndexColumnName] = currentFileIndex;
        outputRow[PartIndexColumnName] = lastDataPartIndex + 1;
        outputRow[OutputTableIndexColumnName] = currentOutputTableIndexColumnName;

        outputInformationTableWriter->AddRow(std::move(outputRow));
        prevSchema = std::move(currentSchema);
    }

    outputInformationTableWriter->Finish();

    YT_LOG_INFO("Starting sorting the output information table");

    ytClient->Sort(TSortOperationSpec()
        .SortBy({FileIndexColumnName, PartIndexColumnName})
        .AddInput(outputInformationTablePath)
        .Output(outputInformationTablePath));

    return outputParserTables;
}

void ImportFilesFromSource(
    const TString& proxy,
    const std::vector<TString>& fileIds,
    const TString& resultTable,
    const std::optional<TString>& networkProject,
    const TSourceConfig& sourceConfig,
    TImportConfigPtr config)
{
    auto ytClient = NYT::CreateClient(proxy);

    YT_LOG_INFO("Create table with meta information");
    TTempTable metaInformationTable(
        ytClient,
        /*prefix*/ TString(),
        /*path*/ TString(),
        TCreateOptions().Attributes(TNode()("schema", TTableSchema()
            .AddColumn(TColumnSchema()
                // TODO(babenko): migrate to std::string
                .Name(TString(FileIdColumnName))
                .Type(VT_STRING, true))
            .AddColumn(TColumnSchema()
                // TODO(babenko): migrate to std::string
                .Name(TString(FileIndexColumnName))
                .Type(VT_INT64, true)).ToNode())));

    auto writer = ytClient->CreateTableWriter<TNode>(metaInformationTable.Name());
    int fileIndex = 0;

    for (const auto& fileName : fileIds) {
        NRe2::TRe2Ptr regex;
        switch (sourceConfig.Format) {
            case EFileFormat::Orc:
                regex = config->OrcFileRegex;
                break;

            case EFileFormat::Parquet:
                regex = config->ParquetFileRegex;
                break;
        }
        if (re2::RE2::PartialMatch(fileName, *regex)) {
            // TODO(babenko): migrate to std::string
            writer->AddRow(TNode()(TString(FileIdColumnName), fileName)(TString(FileIndexColumnName), fileIndex));
            ++fileIndex;
        }
    }
    writer->Finish();

    YT_LOG_INFO(
        "Create tables with data and meta Parquet information from Parquet files (ParquetFileRegex: %v, FileCount: %v)",
        config->ParquetFileRegex->pattern(),
        fileIndex);

    TBlobTableSchema blobTableSchema;
    // TODO(babenko): migrate to std::string
    blobTableSchema.BlobIdColumns({
        TColumnSchema().Name(TString(FileIndexColumnName)).Type(VT_INT64),
    });

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
                // TODO(babenko): migrate to std::string
                .Name(TString(FileIndexColumnName))
                .Type(VT_INT64, true))
            .AddColumn(TColumnSchema()
                // TODO(babenko): migrate to std::string
                .Name(TString(PartIndexColumnName))
                .Type(VT_INT64, true))
            .AddColumn(TColumnSchema()
                // TODO(babenko): migrate to std::string
                .Name(TString(MetadataColumnName))
                .Type(VT_STRING, true))
            .AddColumn(TColumnSchema()
                // TODO(babenko): migrate to std::string
                .Name(TString(StartMetadataOffsetColumnName))
                .Type(VT_INT64, true)).ToNode())));

    const TString dataTablePath = dataTable.Name();
    const TString metadataTablePath = metadataTable.Name();

    // If libiconv.so exists next to us, assume that we are in OSS world
    // and it has to be attached to the user jobs.
    auto selfDir = NFS::GetDirectoryName(GetExecPath());
    auto supposedLibIconvPath = NFS::JoinPaths(selfDir, "libiconv.so");
    bool attachLibIconv = NFS::Exists(supposedLibIconvPath);
    if (attachLibIconv) {
        YT_LOG_INFO("libiconv.so exists, it will be attached to the user jobs (Path: %v)", supposedLibIconvPath);
    } else {
        YT_LOG_INFO("libiconv.so does not exist, assuming static linkage (SupposedPath: %v)", supposedLibIconvPath);
    }

    YT_LOG_INFO("Starting download operation of Parquet files");

    {
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

        auto spec = TMapOperationSpec()
            .AddInput<TNode>(metaInformationTable.Name())
            .AddOutput<TNode>(dataTablePath)
            .AddOutput<TNode>(metadataTablePath)
            .DataSizePerJob(1);

        auto jobSpec = TUserJobSpec()
            .MemoryLimit(config->MemoryLimit);
        if (networkProject) {
            jobSpec.NetworkProject(*networkProject);
        }
        if (attachLibIconv) {
           jobSpec.AddLocalFile("./libiconv.so");
        }

        ytClient->Map(
            spec.MapperSpec(jobSpec),
            new TDownloadMapper(sourceConfig, config->MaxMetadataRowWeight, ConvertToYsonString(config->JobSingletons).ToString()),
            operationOptions);
    }

    YT_LOG_INFO("Starting sort operation of data and metadata blob tables");

    ytClient->Sort(TSortOperationSpec()
        .SortBy({FileIndexColumnName, PartIndexColumnName})
        .AddInput(dataTablePath)
        .Output(TRichYPath(dataTablePath)));

    ytClient->Sort(TSortOperationSpec()
        .SortBy({FileIndexColumnName, PartIndexColumnName})
        .AddInput(metadataTablePath)
        .Output(metadataTablePath));

    {
        TOperationOptions operationOptions;
        auto specNode = TNode()(
                "job_io", NYT::TNode()("table_writer", NYT::TNode()("max_row_weight", config->MaxRowWeight))
        );
        if (config->Pool) {
            specNode = specNode("pool", *config->Pool);
        }
        operationOptions.Spec(specNode);

        TTempTable outputInformationTable(
            ytClient,
            /*prefix*/ TString(),
            /*path*/ TString(),
            TCreateOptions().Attributes(TNode()("schema", TTableSchema()
                .AddColumn(TColumnSchema()
                    // TODO(babenko): migrate to std::string
                    .Name(TString(FileIndexColumnName))
                    .Type(VT_INT64, true))
                .AddColumn(TColumnSchema()
                    // TODO(babenko): migrate to std::string
                    .Name(TString(PartIndexColumnName))
                    .Type(VT_INT64, true))
                .AddColumn(TColumnSchema()
                    // TODO(babenko): migrate to std::string
                    .Name(TString(OutputTableIndexColumnName))
                    .Type(VT_INT64, true)).ToNode())));

        auto outputParserTables = CreateOutputParserTables(
            ytClient,
            metadataTablePath,
            outputInformationTable.Name(),
            sourceConfig.Format);

        auto reduceOperationSpec = TRawReduceOperationSpec()
            .ReduceBy({FileIndexColumnName})
            .SortBy({FileIndexColumnName, PartIndexColumnName})
            .AddInput(metadataTablePath)
            .AddInput(outputInformationTable.Name())
            .AddInput(dataTablePath)
            .InputFormat(TFormat(TNode("yson")))
            .OutputFormat(TFormat(TNode("arrow")));

        for (const auto& outputReduceTable : outputParserTables) {
            reduceOperationSpec = reduceOperationSpec
                .AddOutput(TRichYPath(outputReduceTable.Name()));
        }

        auto jobSpec = TUserJobSpec()
            .MemoryLimit(config->MemoryLimit);
        if (attachLibIconv) {
            jobSpec.AddLocalFile("./libiconv.so");
        }
        reduceOperationSpec = reduceOperationSpec.ReducerSpec(jobSpec);

        YT_LOG_INFO("Starting reduce operation for parsing arrow and producing rows in the temporary tables (MaxRowWeight: %v)", config->MaxRowWeight);

        ytClient->RawReduce(
            reduceOperationSpec,
            new TParseParquetFilesReducer(sourceConfig),
            operationOptions);

        auto mergeOperationSpec = TMergeOperationSpec();
        for (auto& table : outputParserTables) {
            mergeOperationSpec = mergeOperationSpec.AddInput(table.Name());
        }

        mergeOperationSpec = mergeOperationSpec
            .Output(resultTable)
            .SchemaInferenceMode(ESchemaInferenceMode::FromInput);

        YT_LOG_INFO("Start merge operation: filling rows in the result table (MaxRowWeight: %v)", config->MaxRowWeight);
        ytClient->Merge(mergeOperationSpec, operationOptions);
    }

    YT_LOG_INFO("Parquet files were successfully uploaded (ResultTable: %v)", resultTable);
}

////////////////////////////////////////////////////////////////////////////////

void ImportFilesFromS3(
    const TString& proxy,
    const std::string& url,
    const TString& region,
    const TString& bucket,
    const TString& prefix,
    const TString& resultTable,
    EFileFormat format,
    const std::optional<TString>& networkProject,
    TImportConfigPtr config)
{
    TString accessKeyId = GetEnv("ACCESS_KEY_ID");
    TString secretAccessKey = GetEnv("SECRET_ACCESS_KEY");

    TS3Config s3Config({
        .Url = url,
        .Region = region,
        .Bucket = bucket,
    });

    YT_LOG_INFO("Receiving file name from S3 (Url: %v, Region: %v, Bucket: %v, Prefix: %v)",
        url,
        region,
        bucket,
        prefix);

    auto fileKeys = GetListFilesKeysFromS3(s3Config, accessKeyId, secretAccessKey, prefix);

    YT_LOG_INFO("Successfully received file names from S3 (Count: %v)", fileKeys.size());

    ImportFilesFromSource(
        proxy,
        fileKeys,
        resultTable,
        networkProject,
        TSourceConfig{
            .S3Config = s3Config,
            .Format = format,
        },
        std::move(config));
}

void ImportFilesFromHuggingface(
    const TString& proxy,
    const TString& dataset,
    const TString& subset,
    const TString& split,
    const TString& resultTable,
    EFileFormat format,
    const std::optional<TString>& networkProject,
    const std::optional<TString>& urlOverride,
    TImportConfigPtr config)
{
    YT_LOG_INFO("Receiving file name from Huggingface (Dataset: %v, Subset: %v, Split: %v)",
        dataset,
        subset,
        split);

    auto huggingfaceToken = GetEnvOrNull("HUGGINGFACE_TOKEN");

    auto poller = CreateThreadPoolPoller(1, "HuggingfacePoller");
    NHuggingface::THuggingfaceClient huggingfaceClient(huggingfaceToken, poller, urlOverride);

    auto fileIds = huggingfaceClient.GetParquetFileUrls(dataset, subset, split);

    YT_LOG_INFO("Successfully received file names from Huggingface (Count: %v)", fileIds.size());

    ImportFilesFromSource(
        proxy,
        fileIds,
        resultTable,
        networkProject,
        TSourceConfig{
            .HuggingfaceConfig = THuggingfaceConfig{
                .UrlOverride = urlOverride,
            },
            .Format = format,
        },
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NImporter
