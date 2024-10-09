#include "arrow_raw_iterator.h"

#include <yt/yt/core/ytree/convert.h>

#include <yt/cpp/mapreduce/library/table_schema/arrow.h>

#include <library/cpp/yson/node/node.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/filesystem/filesystem.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter.h>

namespace NYT::NPython {

using namespace arrow;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const Status& status)
{
    if (!status.ok()) {
        throw Py::RuntimeError(status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRecordBatchReaderOrcAdapter
    : public arrow::RecordBatchReader
{
public:
    TRecordBatchReaderOrcAdapter(const TString& inputFilePath, MemoryPool* pool)
    {
        auto fileReaderOrError = arrow::io::MemoryMappedFile::Open(inputFilePath, arrow::io::FileMode::READ);
        ThrowOnError(fileReaderOrError.status());
        ThrowOnError(arrow::adapters::orc::ORCFileReader::Open(
            fileReaderOrError.ValueOrDie(),
            pool,
            &Reader_));
        ThrowOnError(Reader_->NextStripeReader(parquet::kDefaultBufferSize, &BatchReader_));
    }

    std::shared_ptr<arrow::Schema> schema() const override
    {
        return BatchReader_->schema();
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override
    {
        ThrowOnError(BatchReader_->ReadNext(batch));
        if (batch == nullptr) {
            ThrowOnError(Reader_->NextStripeReader(parquet::kDefaultBufferSize, &BatchReader_));
            if (BatchReader_ != nullptr) {
                ThrowOnError(BatchReader_->ReadNext(batch));
            }
        }
        return arrow::Status::OK();
    }

private:
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> Reader_;
    std::shared_ptr<RecordBatchReader> BatchReader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

Status TArrowOutputStream::Write(const void* data, int64_t nbytes)
{
    Position_ += nbytes;
    auto ptr = reinterpret_cast<const char*>(data);
    Data_.push(TString(ptr, nbytes));
    return arrow::Status::OK();
}

Status TArrowOutputStream::Flush()
{
    return arrow::Status::OK();
}

Status TArrowOutputStream::Close()
{
    IsClosed_ = true;
    return arrow::Status::OK();
}

Result<int64_t> TArrowOutputStream::Tell() const
{
    return Position_;
}

bool TArrowOutputStream::closed() const
{
    return IsClosed_;
}

bool TArrowOutputStream::IsEmpty() const
{
    return Data_.empty();
}

PyObject* TArrowOutputStream::Get()
{
    YT_VERIFY(!IsEmpty());
    auto buffer = Data_.front();
    auto object = Py::Bytes(buffer.data(), buffer.size());
    object.increment_reference_count();
    Data_.pop();
    return object.ptr();
}

////////////////////////////////////////////////////////////////////////////////

TArrowRawIterator::TArrowRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TArrowRawIterator>::PythonClass(self, args, kwargs)
{ }

void TArrowRawIterator::Initialize(const TString& inputFilePath, EFileFormat format)
{
    auto* pool = arrow::default_memory_pool();

    switch (format) {
        case EFileFormat::ORC: {
            RecordBatchReader_ = std::make_shared<TRecordBatchReaderOrcAdapter>(inputFilePath, pool);
            break;
        }
        case EFileFormat::Parquet: {
            parquet::ReaderProperties readerProperties;
            readerProperties.enable_buffered_stream();

            auto parquetFileReader = parquet::ParquetFileReader::OpenFile(inputFilePath, /*memory_map*/ true, readerProperties);

            ThrowOnError(parquet::arrow::FileReader::Make(
                pool,
                std::move(parquetFileReader),
                parquet::ArrowReaderProperties(),
                &ArrowFileReader_));

            std::vector<int> rowGroups(ArrowFileReader_->num_row_groups());
            std::iota(rowGroups.begin(), rowGroups.end(), 0);

            ThrowOnError(ArrowFileReader_->GetRecordBatchReader(rowGroups, &RecordBatchReader_));
        }
    }

    auto recordBatchWriterOrError = arrow::ipc::MakeStreamWriter(&Pipe_, RecordBatchReader_->schema());
    ThrowOnError(recordBatchWriterOrError.status());
    RecordBatchWriter_ = recordBatchWriterOrError.ValueOrDie();
}

Py::Object TArrowRawIterator::iter()
{
    return self();
}

PyObject* TArrowRawIterator::iternext()
{
    if (!Pipe_.IsEmpty()){
        return Pipe_.Get();
    }

    std::shared_ptr<RecordBatch> batch;
    ThrowOnError(RecordBatchReader_->ReadNext(&batch));
    if (!batch) {
        return nullptr;
    }
    ThrowOnError(RecordBatchWriter_->WriteRecordBatch(*batch));
    return Pipe_.Get();
}

Py::Object TArrowRawIterator::GetSchema(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    auto schema = CreateYTTableSchemaFromArrowSchema(RecordBatchReader_->schema()).ToNode();
    TString bytesSchema;
    TStringOutput output(bytesSchema);
    schema.Save(&output);
    output.Finish();
    return Py::Bytes(bytesSchema);
}

void TArrowRawIterator::InitType()
{
    behaviors().name("yt_yson_bindings.yson_lib.ArrowIterator");
    behaviors().doc("Iterates over parquet file");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    PYCXX_ADD_KEYWORDS_METHOD(get_schema, GetSchema, "Get schema in yson type");

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
