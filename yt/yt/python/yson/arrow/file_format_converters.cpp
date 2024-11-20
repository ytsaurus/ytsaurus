#include "arrow_raw_iterator.h"

#include <yt/yt/core/concurrency/async_stream_pipe.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter_util.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

#include <math.h>

namespace NYT::NPython {

namespace {

using namespace NConcurrency;

using TArrowStatusCallback = std::function<void(arrow::Status)>;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MinFileNameWidth = 4;
static constexpr i64 MaxBufferSize = 128_MB;

////////////////////////////////////////////////////////////////////////////////

int GetFileNameWidth(int fileCount)
{
    return std::max(MinFileNameWidth, static_cast<int>(log10(static_cast<double>(fileCount))) + 1);
}

TString GenerateFileName(int index, int fileNameWidth)
{
    auto stringIndex = ToString(index);
    TString zeroPrefix(fileNameWidth - stringIndex.size(), '0');
    return Format("%v%v.par", zeroPrefix, stringIndex);
}

////////////////////////////////////////////////////////////////////////////////

struct TAsyncDumpParquetInputArguments
{
    TString OutputPath;
    bool IsDirectory;
    int ThreadCount;
    i64 DataSizePerJob;
    std::unique_ptr<IZeroCopyInput> InputStream;
};

struct TWorkerBlock
{
    // The ID of the thread that processes the worker block.
    int WorkerId;
    TSharedMutableRef Block;
};

TString GetFileName(const TAsyncDumpParquetInputArguments& inputArquments, int fileIndex, int totalFileCount)
{
    if (inputArquments.IsDirectory) {
        return inputArquments.OutputPath + "/" + GenerateFileName(fileIndex, GetFileNameWidth(totalFileCount));
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

TAsyncDumpParquetInputArguments ExtractAsyncDumpParquetArguments(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputPath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_path"));
    auto isDirectory = Py::ConvertToBoolean(ExtractArgument(args, kwargs, "is_directory"));
    auto threadCount = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "thread_count"));
    auto dataSizePerJob = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "data_size_per_thread"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    ValidateArgumentsEmpty(args, kwargs);
    return TAsyncDumpParquetInputArguments{
        .OutputPath = std::move(outputPath),
        .IsDirectory = isDirectory,
        .ThreadCount =  static_cast<int>(threadCount),
        .DataSizePerJob = dataSizePerJob,
        .InputStream = std::move(stream),
    };
}

////////////////////////////////////////////////////////////////////////////////

struct IFormatWriter
{
    virtual arrow::Status WriteTable(const arrow::Table& table, int64_t chunkSize) = 0;
    virtual arrow::Status Close() = 0;
    virtual ~IFormatWriter() = default;
};

class TParquetWriter
    : public IFormatWriter
{
public:
    TParquetWriter(
        const arrow::Schema& schema,
        const std::string& outputFilePath,
        TArrowStatusCallback arrowStatusCallback)
        : ArrowStatusCallback_(std::move(arrowStatusCallback))
    {
        auto outputFileOrError = arrow::io::FileOutputStream::Open(outputFilePath);
        if (!outputFileOrError.ok()) {
            throw Py::TypeError(outputFileOrError.status().message());
        }
        auto outputFile = outputFileOrError.ValueOrDie();

        auto properties =
            parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();

        ArrowStatusCallback_(parquet::arrow::FileWriter::Open(
            schema,
            arrow::default_memory_pool(),
            outputFile,
            properties,
            &Writer_));
    }

    arrow::Status WriteTable(const arrow::Table& table, int64_t chunkSize) override
    {
        return Writer_->WriteTable(table, chunkSize);
    }

    arrow::Status Close() override
    {
        return Writer_->Close();
    }

    ~TParquetWriter() = default;

private:
    const TArrowStatusCallback ArrowStatusCallback_;

    std::unique_ptr<parquet::arrow::FileWriter> Writer_;
};

class TOrcWriter
    : public IFormatWriter
{
public:
    TOrcWriter(
        const arrow::Schema& schema,
        const std::string& outputFilePath,
        TArrowStatusCallback arrowStatusCallback)
        : OutputStream_(liborc::writeLocalFile(outputFilePath))
        , ArrowStatusCallback_(arrowStatusCallback)
    {
        auto orcSchemaOrError = arrow::adapters::orc::GetOrcType(schema);
        ArrowStatusCallback_(orcSchemaOrError.status());
        OrcSchema_ = std::move(orcSchemaOrError.ValueOrDie());
        Writer_ = liborc::createWriter(*OrcSchema_, OutputStream_.get(), liborc::WriterOptions{});
    }

    arrow::Status WriteTable(const arrow::Table& table, int64_t chunkSize) override
    {
        const int columnCount = table.num_columns();
        i64 rowCount = table.num_rows();

        std::vector<int> chunkOffsets(columnCount, 0);
        std::vector<int64_t> indexOffsets(columnCount, 0);

        auto batch = Writer_->createRowBatch(chunkSize);

        auto* root = arrow::internal::checked_cast<liborc::StructVectorBatch*>(batch.get());

        while (rowCount > 0) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                ArrowStatusCallback_(arrow::adapters::orc::WriteBatch(
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
        return arrow::Status::OK();
    }

    arrow::Status Close() override
    {
        Writer_->close();
        return arrow::Status::OK();
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
    : public arrow::io::InputStream
{
public:
    explicit TArrowInputStreamAdapter(IInputStream* stream)
        : Stream_(stream)
    {
        IsClosed_ = !Stream_->ReadChar(PreviousElement_);
    }

    arrow::Status Close() override
    {
        return arrow::Status::OK();
    }

    bool closed() const override
    {
        return IsClosed_;
    }

    arrow::Result<int64_t> Tell() const override
    {
        return Position_;
    }

    arrow::Result<int64_t> Read(int64_t nBytes, void* out) override
    {
        return DoLoad(out, nBytes);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nBytes) override
    {
        std::string buffer;
        buffer.resize(nBytes);
        buffer.resize(DoLoad(buffer.data(), buffer.size()));
        return arrow::Buffer::FromString(buffer);
    }

private:
    int64_t Position_ = 0;
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
    : public arrow::io::InputStream
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

    arrow::Status Close() override
    {
        auto error = WaitFor(Pipe_->Close());
        if (error.IsOK()) {
            return arrow::Status::OK();
        }
        return arrow::Status::Invalid(error.GetMessage());
    }

    bool closed() const override
    {
        return LastBlock_.size() == 0;
    }

    arrow::Result<int64_t> Tell() const override
    {
        return Position_;
    }

    arrow::Result<int64_t> Read(int64_t nBytes, void* out) override
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

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nBytes) override
    {
        std::string buffer;
        buffer.resize(nBytes);
        auto resultBytesOrError = Read(buffer.size(), buffer.data());
        if (!resultBytesOrError.ok()) {
            return resultBytesOrError.status();
        }
        buffer.resize(*resultBytesOrError);
        return arrow::Buffer::FromString(buffer);
    }

private:
    const TBoundedAsyncStreamPipePtr Pipe_;

    TSharedRef LastBlock_;
    int64_t Position_ = 0;

    void UpdateLastBlock()
    {
        LastBlock_ = WaitFor(Pipe_->Read())
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::Array> ConvertDictionaryToDense(
    const arrow::Array& array,
    const TArrowStatusCallback& arrowStatusCallback)
{
    const arrow::DictionaryType& dictType =
        static_cast<const arrow::DictionaryType&>(*array.type());

    auto castOutputOrError =
        arrow::compute::Cast(array.data(), dictType.value_type(), arrow::compute::CastOptions());
    arrowStatusCallback(castOutputOrError.status());

    auto castOutput = castOutputOrError.ValueOrDie();
    return castOutput.make_array();
}

std::shared_ptr<arrow::Schema> ConvertDictionarySchema(const std::shared_ptr<arrow::Schema>& schema)
{
    if (schema == nullptr) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            auto& dictionaryType = static_cast<const arrow::DictionaryType&>(*field->type());
            fields.emplace_back(field->WithType(dictionaryType.value_type()));
        } else {
            fields.emplace_back(field);
        }
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::RecordBatch> ConvertDictionaryArrays(
    const std::shared_ptr<arrow::RecordBatch>& data,
    const TArrowStatusCallback& arrowStatusCallback)
{
    if (!data) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    bool hasDict = false;
    for (auto&& field : data->schema()->fields()) {
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            auto& dictionaryType = static_cast<const arrow::DictionaryType&>(*field->type());
            fields.emplace_back(field->WithType(dictionaryType.value_type()));
            hasDict = true;
        } else {
            fields.emplace_back(field);
        }
    }
    if (!hasDict) {
        return data;
    }
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& column : data->columns()) {
        if (column->type_id() == arrow::Type::DICTIONARY) {
            columns.emplace_back(ConvertDictionaryToDense(*column, arrowStatusCallback));
        } else {
            columns.emplace_back(column);
        }
    }
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, data->num_rows(), columns);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::ipc::RecordBatchStreamReader> GetNextBatchReader(
    const std::shared_ptr<arrow::io::InputStream>& pipe,
    const TArrowStatusCallback& arrowStatusCallback)
{
    if (pipe->closed()) {
        return nullptr;
    }
    auto batchReaderOrError = arrow::ipc::RecordBatchStreamReader::Open(pipe);
    arrowStatusCallback(batchReaderOrError.status());

    return batchReaderOrError.ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> GetNextBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& batchReader,
    const TArrowStatusCallback& arrowStatusCallback)
{
    auto batchOrError = batchReader->Next();
    arrowStatusCallback(batchOrError.status());
    return ConvertDictionaryArrays(batchOrError.ValueOrDie(), arrowStatusCallback);
}

////////////////////////////////////////////////////////////////////////////////

void DoDumpFile(
    const std::shared_ptr<arrow::io::InputStream>& pipe,
    TString outputFilePath,
    EFileFormat format,
    const TArrowStatusCallback& arrowStatusCallback)
{
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batchReader = GetNextBatchReader(pipe, arrowStatusCallback);
    if (!batchReader) {
        return;
    }

    auto outputFileOrError = arrow::io::FileOutputStream::Open(outputFilePath);
    arrowStatusCallback(outputFileOrError.status());

    auto outputFile = outputFileOrError.ValueOrDie();

    auto schema = ConvertDictionarySchema(batchReader->schema());

    std::unique_ptr<IFormatWriter> formatWriter;
    switch (format) {
        case EFileFormat::Parquet:
            formatWriter = std::make_unique<TParquetWriter>(*schema, outputFilePath, arrowStatusCallback);
            break;
        case EFileFormat::ORC:
            formatWriter = std::make_unique<TOrcWriter>(*schema, outputFilePath, arrowStatusCallback);
    }

    do {
        auto batch = GetNextBatch(batchReader, arrowStatusCallback);

        while (batch != nullptr) {
            auto tableOrError = arrow::Table::FromRecordBatches(batch->schema(), {batch});
            arrowStatusCallback(tableOrError.status());

            auto table = tableOrError.ValueOrDie();
            arrowStatusCallback(formatWriter->WriteTable(*table.get(), batch->num_rows()));
            batch = GetNextBatch(batchReader, arrowStatusCallback);
        }

    } while (batchReader = GetNextBatchReader(pipe, arrowStatusCallback));

    arrowStatusCallback(formatWriter->Close());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Py::Object AsyncDumpParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputArquments = ExtractAsyncDumpParquetArguments(args, kwargs);

    auto threadPool = CreateThreadPool(inputArquments.ThreadCount, "ParquetDumperPool");

    std::vector<TFuture<void>> asyncResults;

    std::vector<std::shared_ptr<TArrowStreamPipe>> pipes;
    pipes.reserve(inputArquments.ThreadCount);
    for (int fileIndex = 0; fileIndex < inputArquments.ThreadCount; ++fileIndex) {
        pipes.push_back(std::make_shared<TArrowStreamPipe>(inputArquments.DataSizePerJob));
    }

    std::atomic<bool> isError = false;
    TString errorMessage;

    auto onArrowStatusCallback = [&] (const arrow::Status& status) {
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
        auto fileName = GetFileName(inputArquments, fileIndex, inputArquments.ThreadCount);

        asyncResults.push_back(BIND([pipe = pipes[fileIndex], fileName = std::move(fileName), onArrowStatusCallback] {
            pipe->Start();
            DoDumpFile(
                pipe,
                fileName,
                EFileFormat::Parquet,
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

Py::Object DumpParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    ValidateArgumentsEmpty(args, kwargs);

    auto pipe = std::make_shared<TArrowInputStreamAdapter>(stream.get());
    DoDumpFile(pipe, outputFilePath, EFileFormat::Parquet, [] (const arrow::Status& status) {
        if (!status.ok()) {
            throw Py::TypeError(status.message());
        }
    });

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpORC(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    ValidateArgumentsEmpty(args, kwargs);

    auto pipe = std::make_shared<TArrowInputStreamAdapter>(stream.get());
    DoDumpFile(pipe, outputFilePath, EFileFormat::ORC, [] (const arrow::Status& status) {
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
    auto arrowBatchSize = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "arrow_batch_size"));

    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::Parquet, arrowBatchSize);

    return pythonIter;
}

Py::Object UploadORC(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "input_file"));
    auto arrowBatchSize = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "arrow_batch_size"));

    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::ORC, arrowBatchSize);

    return pythonIter;
}

////////////////////////////////////////////////////////////////////////////////

void InitArrowIteratorType()
{
    TArrowRawIterator::InitType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
