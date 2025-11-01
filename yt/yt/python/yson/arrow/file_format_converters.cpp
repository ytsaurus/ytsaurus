#include "arrow_raw_iterator.h"
#include "private.h"

#include <yt/yt/core/concurrency/async_stream_pipe.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/adapters/orc/adapter.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/adapters/orc/util.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/file.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/writer.h>

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

#include <math.h>

namespace NYT::NPython {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

using TArrowStatusCallback = std::function<void(arrow20::Status)>;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ArrowConverterLogger;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MinFileNameWidth = 4;
static constexpr i64 MaxBufferSize = 128_MB;

////////////////////////////////////////////////////////////////////////////////

int GetFileNameWidth(int fileCount)
{
    return std::max(MinFileNameWidth, static_cast<int>(log10(static_cast<double>(fileCount))) + 1);
}

TString GenerateFileName(int index, int fileNameWidth, EFileFormat format)
{
    auto stringIndex = ToString(index);
    TString zeroPrefix(fileNameWidth - stringIndex.size(), '0');

    switch (format) {
        case EFileFormat::Parquet:
            return Format("%v%v.parquet", zeroPrefix, stringIndex);
        case EFileFormat::Orc:
            return Format("%v%v.orc", zeroPrefix, stringIndex);
    }
}

arrow20::Compression::type GetParquetFileCompression(const std::string& compressionCodec)
{
    if (compressionCodec == "uncompressed") {
        return arrow20::Compression::UNCOMPRESSED;
    } else if (compressionCodec == "snappy") {
        return arrow20::Compression::SNAPPY;
    } else if (compressionCodec == "gzip") {
        return arrow20::Compression::GZIP;
    } else if (compressionCodec == "brotli") {
        return arrow20::Compression::BROTLI;
    } else if (compressionCodec == "zstd") {
        return arrow20::Compression::ZSTD;
    } else if (compressionCodec == "lz4") {
        return arrow20::Compression::LZ4;
    } else if (compressionCodec == "lz4_frame") {
        return arrow20::Compression::LZ4_FRAME;
    } else if (compressionCodec == "lzo") {
        return arrow20::Compression::LZO;
    } else if (compressionCodec == "bz2") {
        return arrow20::Compression::BZ2;
    } else if (compressionCodec == "lz4_hadoop") {
        return arrow20::Compression::LZ4_HADOOP;
    } else {
        throw Py::TypeError(Format("Unexpected compression codec %Qv", compressionCodec));
    }
}

orc::CompressionKind GetOrcFileCompression(const std::string& compressionCodec)
{
    if (compressionCodec == "none") {
        return orc::CompressionKind::CompressionKind_NONE;
    } else if (compressionCodec == "snappy") {
       return orc::CompressionKind::CompressionKind_SNAPPY;
    } else if (compressionCodec == "zlib") {
        return orc::CompressionKind::CompressionKind_ZLIB;
    } else if (compressionCodec == "lz4") {
        return orc::CompressionKind::CompressionKind_LZ4;
    } else if (compressionCodec == "lzo") {
        return orc::CompressionKind::CompressionKind_LZO;
    } else if (compressionCodec == "zstd") {
        return orc::CompressionKind::CompressionKind_ZSTD;
    } else {
        throw Py::TypeError(Format("Unexpected compression codec %Qv", compressionCodec));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TAsyncDumpFileInputArguments
{
    TString OutputPath;
    bool IsDirectory;
    int ThreadCount;
    i64 DataSizePerJob;
    i64 MinBatchRowCount;
    std::unique_ptr<IZeroCopyInput> InputStream;
};

struct TWorkerBlock
{
    // The ID of the thread that processes the worker block.
    int WorkerId;
    TSharedMutableRef Block;
};

TString GetFileName(const TAsyncDumpFileInputArguments& inputArquments, int fileIndex, int totalFileCount, EFileFormat format)
{
    if (inputArquments.IsDirectory) {
        return inputArquments.OutputPath + "/" + GenerateFileName(fileIndex, GetFileNameWidth(totalFileCount), format);
    } else {
        return inputArquments.OutputPath;
    }
}

std::optional<TWorkerBlock> GetNextWorkerBlock(
    const std::unique_ptr<IZeroCopyInput>& inputStream)
{
    // Read thread ID.
    i32 workerId;
    if (inputStream->Load(&workerId, sizeof(workerId)) != sizeof(workerId)) {
        return std::nullopt;
    }

    // Read size of block.
    i32 blockSize;
    inputStream->Load(&blockSize, sizeof(blockSize));

    // Read block.
    auto block = TSharedMutableRef::Allocate(blockSize);
    inputStream->Load(block.Begin(), blockSize);

    return TWorkerBlock{
        .WorkerId = workerId,
        .Block = std::move(block),
    };
}

TAsyncDumpFileInputArguments ExtractAsyncDumpFileArguments(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputPath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_path"));
    auto isDirectory = Py::ConvertToBoolean(ExtractArgument(args, kwargs, "is_directory"));
    auto threadCount = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "thread_count"));
    auto dataSizePerJob = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "data_size_per_thread"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    i64 minBatchRowCount = 0;
    if (HasArgument(args, kwargs, "min_batch_row_count")) {
        minBatchRowCount = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "min_batch_row_count"));
    }

    return TAsyncDumpFileInputArguments{
        .OutputPath = std::move(outputPath),
        .IsDirectory = isDirectory,
        .ThreadCount =  static_cast<int>(threadCount),
        .DataSizePerJob = dataSizePerJob,
        .MinBatchRowCount = minBatchRowCount,
        .InputStream = std::move(stream),
    };
}

struct TParquetConfig
{
    arrow20::Compression::type FileCompression;
};

struct TOrcConfig
{
    orc::CompressionKind FileCompression;
};

struct TFormatConfig
{
    std::optional<TOrcConfig> OrcConfig;
    std::optional<TParquetConfig> ParquetConfig;
    EFileFormat Format;
    i64 MinBatchRowCount;
};

////////////////////////////////////////////////////////////////////////////////

struct IFormatWriter
{
    virtual arrow20::Status WriteTable(const arrow20::Table& table, i64 chunkSize) = 0;
    virtual arrow20::Status Close() = 0;
    virtual ~IFormatWriter() = default;
};

class TParquetWriter
    : public IFormatWriter
{
public:
    TParquetWriter(
        const arrow20::Schema& schema,
        const std::string& outputFilePath,
        TArrowStatusCallback arrowStatusCallback,
        const TParquetConfig& config)
        : ArrowStatusCallback_(std::move(arrowStatusCallback))
    {
        auto outputFileOrError = arrow20::io::FileOutputStream::Open(outputFilePath);
        ArrowStatusCallback_(outputFileOrError.status());
        auto outputFile = outputFileOrError.ValueOrDie();

        auto properties =
            parquet20::WriterProperties::Builder().compression(config.FileCompression)->build();

        auto writerOrError = parquet20::arrow20::FileWriter::Open(
            schema,
            arrow20::default_memory_pool(),
            outputFile,
            properties);

        ArrowStatusCallback_(writerOrError.status());
        Writer_ = std::move(writerOrError.ValueOrDie());
    }

    arrow20::Status WriteTable(const arrow20::Table& table, i64 chunkSize) override
    {
        return Writer_->WriteTable(table, chunkSize);
    }

    arrow20::Status Close() override
    {
        return Writer_->Close();
    }

private:
    const TArrowStatusCallback ArrowStatusCallback_;

    std::unique_ptr<parquet20::arrow20::FileWriter> Writer_;
};

class TOrcWriter
    : public IFormatWriter
{
public:
    TOrcWriter(
        const arrow20::Schema& schema,
        const std::string& outputFilePath,
        TArrowStatusCallback arrowStatusCallback,
        const TOrcConfig& config)
        : OutputStream_(liborc::writeLocalFile(outputFilePath))
        , ArrowStatusCallback_(arrowStatusCallback)
    {
        auto orcSchemaOrError = arrow20::adapters::orc::GetOrcType(schema);
        ArrowStatusCallback_(orcSchemaOrError.status());
        OrcSchema_ = std::move(orcSchemaOrError.ValueOrDie());
        liborc::WriterOptions options;
        options.setCompression(config.FileCompression);
        Writer_ = liborc::createWriter(*OrcSchema_, OutputStream_.get(), options);
    }

    arrow20::Status WriteTable(const arrow20::Table& table, i64 chunkSize) override
    {
        const int columnCount = table.num_columns();
        i64 rowCount = table.num_rows();

        std::vector<int> chunkOffsets(columnCount, 0);
        std::vector<int64_t> indexOffsets(columnCount, 0);

        auto batch = Writer_->createRowBatch(chunkSize);

        auto* root = arrow20::internal::checked_cast<liborc::StructVectorBatch*>(batch.get());

        while (rowCount > 0) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                ArrowStatusCallback_(arrow20::adapters::orc::WriteBatch(
                    *(table.column(columnIndex)),
                    chunkSize,
                    &chunkOffsets[columnIndex],
                    &indexOffsets[columnIndex],
                    root->fields[columnIndex]));
            }
            root->numElements = root->fields[0]->numElements;
            Writer_->add(*batch);
            batch->clear();
            rowCount -= chunkSize;
        }
        return arrow20::Status::OK();
    }

    arrow20::Status Close() override
    {
        Writer_->close();
        return arrow20::Status::OK();
    }

    ~TOrcWriter() = default;

private:
    const std::unique_ptr<liborc::OutputStream> OutputStream_;
    const TArrowStatusCallback ArrowStatusCallback_;

    std::unique_ptr<liborc::Type> OrcSchema_;
    std::unique_ptr<liborc::Writer> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

class TArrowInputStreamAdapter
    : public arrow20::io::InputStream
{
public:
    explicit TArrowInputStreamAdapter(IInputStream* stream)
        : Stream_(stream)
    {
        IsClosed_ = !Stream_->ReadChar(PreviousElement_);
    }

    arrow20::Status Close() override
    {
        return arrow20::Status::OK();
    }

    bool closed() const override
    {
        return IsClosed_;
    }

    arrow20::Result<int64_t> Tell() const override
    {
        return Position_;
    }

    arrow20::Result<int64_t> Read(int64_t nBytes, void* out) override
    {
        return DoLoad(out, nBytes);
    }

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nBytes) override
    {
        std::string buffer;
        buffer.resize(nBytes);
        buffer.resize(DoLoad(buffer.data(), buffer.size()));
        return arrow20::Buffer::FromString(buffer);
    }

private:
    i64 Position_ = 0;
    bool IsClosed_ = false;
    char PreviousElement_;
    IInputStream* Stream_;

    size_t DoLoad(void* buf, size_t len)
    {
        if (IsClosed_ || len == 0) {
            return 0;
        }
        char* outChar = reinterpret_cast<char*>(buf);
        *outChar = PreviousElement_;
        outChar++;
        auto nBytes = Stream_->Load(outChar, len - 1) + 1;
        Position_ += nBytes;
        IsClosed_ = !Stream_->ReadChar(PreviousElement_);
        return nBytes;
    }
};

class TArrowStreamPipe
    : public arrow20::io::InputStream
{
public:
    explicit TArrowStreamPipe(i64 dataSizePerThread)
        : Pipe_(New<TBoundedAsyncStreamPipe>(MaxBufferSize / dataSizePerThread))
    { }

    void Start()
    {
        UpdateLastBlock();
    }

    void Write(TSharedRef ref)
    {
        WaitFor(Pipe_->Write(ref))
            .ThrowOnError();
    }

    void AbortPipe(const TError& error)
    {
        Pipe_->Abort(error);
    }

    arrow20::Status Close() override
    {
        auto error = WaitFor(Pipe_->Close());
        if (error.IsOK()) {
            return arrow20::Status::OK();
        }
        return arrow20::Status::Invalid(error.GetMessage());
    }

    bool closed() const override
    {
        return LastBlock_.size() == 0;
    }

    arrow20::Result<int64_t> Tell() const override
    {
        return Position_;
    }

    arrow20::Result<int64_t> Read(int64_t nBytes, void* out) override
    {
        char* outputChar = reinterpret_cast<char*>(out);
        auto bytesCountLeft = nBytes;

        while (bytesCountLeft > 0) {
            if (LastBlock_.size() == 0) {
                break;
            }

            if (bytesCountLeft < std::ssize(LastBlock_)) {
                memcpy(outputChar, LastBlock_.Data(), bytesCountLeft);
                LastBlock_ = LastBlock_.Slice(bytesCountLeft, LastBlock_.Size());
                bytesCountLeft = 0;
            } else {
                memcpy(outputChar, LastBlock_.Data(), LastBlock_.Size());
                bytesCountLeft -= LastBlock_.Size();
                outputChar += LastBlock_.Size();
                UpdateLastBlock();
            }
        }

        return nBytes - bytesCountLeft;
    }

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nBytes) override
    {
        std::string buffer;
        buffer.resize(nBytes);
        auto resultBytesOrError = Read(buffer.size(), buffer.data());
        if (!resultBytesOrError.ok()) {
            return resultBytesOrError.status();
        }
        buffer.resize(*resultBytesOrError);
        return arrow20::Buffer::FromString(buffer);
    }

private:
    const TBoundedAsyncStreamPipePtr Pipe_;

    TSharedRef LastBlock_;
    i64 Position_ = 0;

    void UpdateLastBlock()
    {
        LastBlock_ = WaitFor(Pipe_->Read())
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::Array> ConvertDictionaryToDense(
    const arrow20::Array& array,
    const TArrowStatusCallback& arrowStatusCallback)
{
    const arrow20::DictionaryType& dictType =
        static_cast<const arrow20::DictionaryType&>(*array.type());

    auto castOutputOrError =
        arrow20::compute::Cast(array.data(), dictType.value_type(), arrow20::compute::CastOptions());
    arrowStatusCallback(castOutputOrError.status());

    auto castOutput = castOutputOrError.ValueOrDie();
    return castOutput.make_array();
}

std::shared_ptr<arrow20::Schema> ConvertDictionarySchema(const std::shared_ptr<arrow20::Schema>& schema)
{
    if (schema == nullptr) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow20::Field>> fields;
    for (auto&& field : schema->fields()) {
        if (field->type()->id() == arrow20::Type::DICTIONARY) {
            auto& dictionaryType = static_cast<const arrow20::DictionaryType&>(*field->type());
            fields.emplace_back(field->WithType(dictionaryType.value_type()));
        } else {
            fields.emplace_back(field);
        }
    }
    return std::make_shared<arrow20::Schema>(fields);
}

std::shared_ptr<arrow20::RecordBatch> ConvertDictionaryArrays(
    const std::shared_ptr<arrow20::RecordBatch>& data,
    const TArrowStatusCallback& arrowStatusCallback)
{
    if (!data) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow20::Field>> fields;
    bool hasDict = false;
    for (auto&& field : data->schema()->fields()) {
        if (field->type()->id() == arrow20::Type::DICTIONARY) {
            auto& dictionaryType = static_cast<const arrow20::DictionaryType&>(*field->type());
            fields.emplace_back(field->WithType(dictionaryType.value_type()));
            hasDict = true;
        } else {
            fields.emplace_back(field);
        }
    }
    if (!hasDict) {
        return data;
    }
    std::vector<std::shared_ptr<arrow20::Array>> columns;
    for (auto&& column : data->columns()) {
        if (column->type_id() == arrow20::Type::DICTIONARY) {
            columns.emplace_back(ConvertDictionaryToDense(*column, arrowStatusCallback));
        } else {
            columns.emplace_back(column);
        }
    }
    std::shared_ptr<arrow20::Schema> schema = std::make_shared<arrow20::Schema>(fields);
    return arrow20::RecordBatch::Make(schema, data->num_rows(), columns);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::ipc::RecordBatchStreamReader> GetNextBatchReader(
    const std::shared_ptr<arrow20::io::InputStream>& pipe,
    const TArrowStatusCallback& arrowStatusCallback)
{
    if (pipe->closed()) {
        return nullptr;
    }
    auto batchReaderOrError = arrow20::ipc::RecordBatchStreamReader::Open(pipe);
    arrowStatusCallback(batchReaderOrError.status());

    return batchReaderOrError.ValueOrDie();
}

std::shared_ptr<arrow20::RecordBatch> GetNextBatch(
    const std::shared_ptr<arrow20::ipc::RecordBatchStreamReader>& batchReader,
    const TArrowStatusCallback& arrowStatusCallback)
{
    auto batchOrError = batchReader->Next();
    arrowStatusCallback(batchOrError.status());
    return ConvertDictionaryArrays(batchOrError.ValueOrDie(), arrowStatusCallback);
}

////////////////////////////////////////////////////////////////////////////////

void DoDumpFile(
    const std::shared_ptr<arrow20::io::InputStream>& pipe,
    TString outputFilePath,
    const TFormatConfig& config,
    const TArrowStatusCallback& arrowStatusCallback)
{
    std::shared_ptr<arrow20::ipc::RecordBatchStreamReader> batchReader = GetNextBatchReader(pipe, arrowStatusCallback);
    if (!batchReader) {
        return;
    }

    auto outputFileOrError = arrow20::io::FileOutputStream::Open(outputFilePath);
    arrowStatusCallback(outputFileOrError.status());

    auto outputFile = outputFileOrError.ValueOrDie();

    auto schema = ConvertDictionarySchema(batchReader->schema());

    std::unique_ptr<IFormatWriter> formatWriter;

    if (config.ParquetConfig) {
        formatWriter = std::make_unique<TParquetWriter>(*schema, outputFilePath, arrowStatusCallback, *config.ParquetConfig);
    } else if (config.OrcConfig) {
        formatWriter = std::make_unique<TOrcWriter>(*schema, outputFilePath, arrowStatusCallback, *config.OrcConfig);
    } else {
        throw Py::TypeError("Unexpected format");
    }

    std::vector<std::shared_ptr<arrow20::RecordBatch>> batches;
    int batchesRowCount = 0;

    auto writeBatches = [&] () {
        YT_VERIFY(batches.size() > 0);
        auto tableOrError = arrow20::Table::FromRecordBatches(batches[0]->schema(), batches);

        arrowStatusCallback(tableOrError.status());
        auto table = tableOrError.ValueOrDie();
        arrowStatusCallback(formatWriter->WriteTable(*table.get(), batchesRowCount));
    };

    do {
        auto batch = GetNextBatch(batchReader, arrowStatusCallback);

        while (batch != nullptr) {
            batchesRowCount += batch->num_rows();
            batches.push_back(std::move(batch));

            if (batchesRowCount >= config.MinBatchRowCount) {
                writeBatches();
                batches.clear();
                batchesRowCount = 0;
            }
            batch = GetNextBatch(batchReader, arrowStatusCallback);
        }

    } while (batchReader = GetNextBatchReader(pipe, arrowStatusCallback));

    if (batchesRowCount > 0) {
        writeBatches();
    }

    arrowStatusCallback(formatWriter->Close());
}

Py::Object DoAsyncDumpFile(TAsyncDumpFileInputArguments&& inputArquments, const TFormatConfig& config)
{
    auto threadPool = CreateThreadPool(inputArquments.ThreadCount, "ParquetDumperPool");

    std::vector<TFuture<void>> asyncResults;

    std::vector<std::shared_ptr<TArrowStreamPipe>> pipes;
    pipes.reserve(inputArquments.ThreadCount);
    for (int fileIndex = 0; fileIndex < inputArquments.ThreadCount; ++fileIndex) {
        pipes.push_back(std::make_shared<TArrowStreamPipe>(inputArquments.DataSizePerJob));
    }

    std::atomic<bool> isError = false;
    TString errorMessage;

    auto onArrowStatusCallback = [&] (const arrow20::Status& status) {
        if (!status.ok()) {
            bool expected = false;
            if (isError.compare_exchange_strong(expected, true)) {
                for (int fileIndex = 0; fileIndex < inputArquments.ThreadCount; ++fileIndex) {
                    pipes[fileIndex]->AbortPipe(TError("%v", status.message()));
                }
                errorMessage = status.message();
            }
            THROW_ERROR_EXCEPTION("An error occurred while parsing parquet: %v", TString(status.message()));
        }
    };

    // Сreate handlers that turn blocks into parquet format and write the result to a file.
    for (int fileIndex = 0; fileIndex < inputArquments.ThreadCount; ++fileIndex) {
        auto fileName = GetFileName(inputArquments, fileIndex, inputArquments.ThreadCount, config.Format);

        asyncResults.push_back(BIND([
            pipe = pipes[fileIndex],
            fileName = std::move(fileName),
            onArrowStatusCallback,
            &config
        ] {
            pipe->Start();
            DoDumpFile(
                pipe,
                fileName,
                config,
                onArrowStatusCallback);
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }

    while (auto currentBlock = GetNextWorkerBlock(inputArquments.InputStream)) {
        // Passing the block to the handler.
        YT_VERIFY(currentBlock->WorkerId >= 0 && currentBlock->WorkerId < std::ssize(pipes));
        pipes[currentBlock->WorkerId]->Write(std::move(currentBlock->Block));
        if (isError) {
            break;
        }
    }

    // Sending a completion signal to all handlers.
    for (int workerId = 0; workerId < inputArquments.ThreadCount; ++workerId) {
        auto closeStatus = pipes[workerId]->Close();
        if (!closeStatus.ok()) {
            throw Py::TypeError(closeStatus.message());
        }
    }

    auto error = WaitFor(AllSet(asyncResults));

    threadPool->Shutdown();

    if (isError) {
        throw Py::TypeError(errorMessage);
    }

    if (!error.IsOK()) {
        throw Py::TypeError(error.GetMessage());
    }

    return Py::None();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Py::Object AsyncDumpParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputArguments = ExtractAsyncDumpFileArguments(args, kwargs);
    auto fileCompression = arrow20::Compression::SNAPPY;
    if (HasArgument(args, kwargs, "file_compression_codec")) {
        fileCompression = GetParquetFileCompression(Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "file_compression_codec")));
    }

    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The AsyncDumpParquet function received unrecognized arguments");
    }
    auto config = TFormatConfig{
        .ParquetConfig = TParquetConfig{
            .FileCompression = fileCompression,
        },
        .Format = EFileFormat::Parquet,
        .MinBatchRowCount = inputArguments.MinBatchRowCount,
    };
    return DoAsyncDumpFile(std::move(inputArguments), config);
}

Py::Object DumpParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    i64 minBatchRowCount = 0;

    if (HasArgument(args, kwargs, "min_batch_row_count")) {
        minBatchRowCount = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "min_batch_row_count"));
    }

    auto fileCompression = arrow20::Compression::SNAPPY;
    if (HasArgument(args, kwargs, "file_compression_codec")) {
        fileCompression = GetParquetFileCompression(Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "file_compression_codec")));
    }

    auto config = TFormatConfig{
        .ParquetConfig = TParquetConfig{
            .FileCompression = fileCompression,
        },
        .Format = EFileFormat::Parquet,
        .MinBatchRowCount = minBatchRowCount,
    };

    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The DumpParquet function received unrecognized arguments");
    }

    auto pipe = std::make_shared<TArrowInputStreamAdapter>(stream.get());

    DoDumpFile(pipe, outputFilePath, config, [] (const arrow20::Status& status) {
        if (!status.ok()) {
            throw Py::TypeError(status.message());
        }
    });

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object AsyncDumpOrc(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputArguments = ExtractAsyncDumpFileArguments(args, kwargs);

    auto fileCompression = orc::CompressionKind::CompressionKind_SNAPPY;
    if (HasArgument(args, kwargs, "file_compression_codec")) {
        fileCompression = GetOrcFileCompression(Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "file_compression_codec")));
    }
    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The AsyncDumpOrc function received unrecognized arguments");
    }
    auto config = TFormatConfig{
        .OrcConfig = TOrcConfig{
            .FileCompression = fileCompression,
        },
        .Format = EFileFormat::Orc,
        .MinBatchRowCount = inputArguments.MinBatchRowCount,
    };
    return DoAsyncDumpFile(std::move(inputArguments), config);
}

Py::Object DumpOrc(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

   i64 minBatchRowCount = 0;

    if (HasArgument(args, kwargs, "min_batch_row_count")) {
        minBatchRowCount = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "min_batch_row_count"));
    }

    auto fileCompression = orc::CompressionKind::CompressionKind_SNAPPY;
    if (HasArgument(args, kwargs, "file_compression_codec")) {
        fileCompression = GetOrcFileCompression(Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "file_compression_codec")));
    }

    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The DumpOrc function received unrecognized arguments");
    }

    auto config = TFormatConfig{
        .OrcConfig = TOrcConfig{
            .FileCompression = fileCompression,
        },
        .Format = EFileFormat::Orc,
        .MinBatchRowCount = minBatchRowCount,
    };

    auto pipe = std::make_shared<TArrowInputStreamAdapter>(stream.get());
    DoDumpFile(pipe, outputFilePath, config, [] (const arrow20::Status& status) {
        if (!status.ok()) {
            throw Py::TypeError(status.message());
        }
    });

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object UploadParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "input_file"));

    int arrowBatchSize = parquet20::kArrowDefaultBatchSize;
    if (HasArgument(args, kwargs, "arrow_batch_size")) {
        arrowBatchSize = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "arrow_batch_size"));
    }

    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The UploadParquet function received unrecognized arguments");
    }

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::Parquet, arrowBatchSize);

    return pythonIter;
}

Py::Object UploadOrc(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "input_file"));

    int arrowBatchSize = parquet20::kArrowDefaultBatchSize;
    if (HasArgument(args, kwargs, "arrow_batch_size")) {
        arrowBatchSize = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "arrow_batch_size"));
    }

    if (!AreArgumentsEmpty(args, kwargs)) {
        YT_LOG_WARNING("The UploadOrc function received unrecognized arguments");
    }

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::Orc, arrowBatchSize);

    return pythonIter;
}

////////////////////////////////////////////////////////////////////////////////

void InitArrowIteratorType()
{
    TArrowRawIterator::InitType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
