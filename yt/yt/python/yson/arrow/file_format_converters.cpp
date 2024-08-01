#include "arrow_raw_iterator.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter_util.h>

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

namespace NYT::NPython {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status) {
    if (!status.ok()) {
        throw Py::TypeError(status.message());
    }
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
    TParquetWriter(const arrow::Schema& schema, const std::string& outputFilePath)
    {
        auto outputFileOrError = arrow::io::FileOutputStream::Open(outputFilePath);
        if (!outputFileOrError.ok()) {
            throw Py::TypeError(outputFileOrError.status().message());
        }
        auto outputFile = outputFileOrError.ValueOrDie();

        auto properties =
            parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();

        ThrowOnError(parquet::arrow::FileWriter::Open(
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
    std::unique_ptr<parquet::arrow::FileWriter> Writer_;
};

class TOrcWriter
    : public IFormatWriter
{
public:
    TOrcWriter(const arrow::Schema& schema, const std::string& outputFilePath)
        : OutputStream_(liborc::writeLocalFile(outputFilePath))
    {
        auto orcSchemaOrError = arrow::adapters::orc::GetOrcType(schema);
        ThrowOnError(orcSchemaOrError.status());
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
                ThrowOnError(arrow::adapters::orc::WriteBatch(
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
    std::unique_ptr<liborc::Type> OrcSchema_;
    std::unique_ptr<liborc::Writer> Writer_;
    std::unique_ptr<liborc::OutputStream> OutputStream_;
};

////////////////////////////////////////////////////////////////////////////////

class TPipeForRecordBatchStreamReader
    : public arrow::io::InputStream
{
public:
    TPipeForRecordBatchStreamReader(IInputStream* reader)
        : Reader_(reader)
    {
        IsFinished_ = !Reader_->ReadChar(PreviousElement_);
    }

    arrow::Status Close() override
    {
        return arrow::Status::OK();
    }

    bool closed() const override
    {
        return false;
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

    bool IsFinished() const
    {
        return IsFinished_;
    }

private:
    int64_t Position_ = 0;
    bool IsFinished_ = false;
    char PreviousElement_;
    IInputStream* Reader_;

    size_t DoLoad(void* buf, size_t len)
    {
        if (IsFinished_ || len == 0) {
            return 0;
        }
        char* outChar = reinterpret_cast<char*>(buf);
        *outChar = PreviousElement_;
        outChar++;
        auto nBytes = Reader_->Load(outChar, len - 1) + 1;
        Position_ += nBytes;
        IsFinished_ = !Reader_->ReadChar(PreviousElement_);
        return nBytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::Array> ConvertDictionaryToDense(const arrow::Array& array)
{
    const arrow::DictionaryType& dictType =
        static_cast<const arrow::DictionaryType&>(*array.type());

    auto castOutputOrError =
        arrow::compute::Cast(array.data(), dictType.value_type(), arrow::compute::CastOptions());

    if (!castOutputOrError.ok()) {
        throw Py::TypeError(castOutputOrError.status().message());
    }

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

std::shared_ptr<arrow::RecordBatch> ConvertDictionaryArrays(const std::shared_ptr<arrow::RecordBatch>& data) {
    if (data == nullptr) {
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
            columns.emplace_back(ConvertDictionaryToDense(*column));
        } else {
            columns.emplace_back(column);
        }
    }
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, data->num_rows(), columns);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::ipc::RecordBatchStreamReader> GetNextBatchReader(TPipeForRecordBatchStreamReader& pipe)
{
    if (pipe.IsFinished()) {
        return nullptr;
    }
    auto batchReaderOrError = arrow::ipc::RecordBatchStreamReader::Open(&pipe);
    if (!batchReaderOrError.ok()) {
        throw Py::TypeError(batchReaderOrError.status().message());
    }

    return batchReaderOrError.ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> GetNextBatch(const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& batchReader)
{
    auto batchOrError = batchReader->Next();
    if (!batchOrError.ok()) {
        throw Py::TypeError(batchOrError.status().message());
    }
    return ConvertDictionaryArrays(batchOrError.ValueOrDie());
}

////////////////////////////////////////////////////////////////////////////////

void DumpFile(
    const std::string& outputFilePath,
    IInputStream* stream,
    EFileFormat format)
{
    TPipeForRecordBatchStreamReader pipe(stream);
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batchReader = GetNextBatchReader(pipe);
    if (!batchReader) {
        return;
    }

    std::unique_ptr<parquet::arrow::FileWriter> writer;

    auto schema = ConvertDictionarySchema(batchReader->schema());
    std::unique_ptr<IFormatWriter> formatWriter;
    switch (format) {
        case EFileFormat::Parquet:
            formatWriter = std::make_unique<TParquetWriter>(*schema, outputFilePath);
            break;
        case EFileFormat::ORC:
            formatWriter = std::make_unique<TOrcWriter>(*schema, outputFilePath);
    }

    do {
        auto batch = GetNextBatch(batchReader);

        while (batch != nullptr) {
            auto tableOrError = arrow::Table::FromRecordBatches(batch->schema(), {batch});

            if (!tableOrError.ok()) {
                throw Py::TypeError(tableOrError.status().message());
            }
            auto table = tableOrError.ValueOrDie();
            ThrowOnError(formatWriter->WriteTable(*table.get(), batch->num_rows()));

            batch = GetNextBatch(batchReader);
        }

    } while (batchReader = GetNextBatchReader(pipe));

    ThrowOnError(formatWriter->Close());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    ValidateArgumentsEmpty(args, kwargs);

    DumpFile(outputFilePath, stream.get(), EFileFormat::Parquet);

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpORC(Py::Tuple& args, Py::Dict& kwargs)
{
    auto outputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "output_file"));

    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto stream = CreateInputStreamWrapper(streamArg);

    ValidateArgumentsEmpty(args, kwargs);

    DumpFile(outputFilePath, stream.get(), EFileFormat::ORC);

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object UploadParquet(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "input_file"));

    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::Parquet);

    return pythonIter;
}

Py::Object UploadORC(Py::Tuple& args, Py::Dict& kwargs)
{
    auto inputFilePath = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "input_file"));

    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable classType(TArrowRawIterator::type());
    Py::PythonClassObject<TArrowRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));
    auto* iter = pythonIter.getCxxObject();
    iter->Initialize(inputFilePath, EFileFormat::ORC);

    return pythonIter;
}

////////////////////////////////////////////////////////////////////////////////

void InitArrowIteratorType()
{
    TArrowRawIterator::InitType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
