#include "arrow_raw_iterator.h"

#include <yt/yt/core/ytree/convert.h>

#include <yt/cpp/mapreduce/library/table_schema/arrow.h>

#include <library/cpp/yson/node/node.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/array_base.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/filesystem/filesystem.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/adapters/orc/adapter.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/file.h>

namespace NYT::NPython {

using namespace arrow20;

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
    : public arrow20::RecordBatchReader
{
public:
    TRecordBatchReaderOrcAdapter(const TString& inputFilePath, MemoryPool* pool, int arrowBatchSize)
        : ArrowBatchSize_(arrowBatchSize)
    {
        auto fileReaderOrError = arrow20::io::MemoryMappedFile::Open(inputFilePath, arrow20::io::FileMode::READ);
        ThrowOnError(fileReaderOrError.status());
        auto orcReaderOrError = arrow20::adapters::orc::ORCFileReader::Open(
            fileReaderOrError.ValueOrDie(),
            pool);
        ThrowOnError(orcReaderOrError.status());
        Reader_ = std::move(orcReaderOrError.ValueOrDie());
        auto batchReaderOrError = Reader_->NextStripeReader(ArrowBatchSize_);
        ThrowOnError(batchReaderOrError.status());
        BatchReader_ = batchReaderOrError.ValueOrDie();
    }

    std::shared_ptr<arrow20::Schema> schema() const override
    {
        return BatchReader_->schema();
    }

    arrow20::Status ReadNext(std::shared_ptr<arrow20::RecordBatch>* batch) override
    {
        ThrowOnError(BatchReader_->ReadNext(batch));
        if (!(*batch)) {
            auto batchReaderOrError = Reader_->NextStripeReader(ArrowBatchSize_);
            ThrowOnError(batchReaderOrError.status());
            BatchReader_ = batchReaderOrError.ValueOrDie();
            if (BatchReader_) {
                ThrowOnError(BatchReader_->ReadNext(batch));
            }
        }
        return arrow20::Status::OK();
    }

private:
    const int ArrowBatchSize_;

    std::unique_ptr<arrow20::adapters::orc::ORCFileReader> Reader_;
    std::shared_ptr<RecordBatchReader> BatchReader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

Status TArrowOutputStream::Write(const void* data, int64_t nbytes)
{
    Position_ += nbytes;
    DataWeight_ += nbytes;
    auto ptr = reinterpret_cast<const char*>(data);
    Data_.push(TString(ptr, nbytes));
    return arrow20::Status::OK();
}

Status TArrowOutputStream::Flush()
{
    return arrow20::Status::OK();
}

Status TArrowOutputStream::Close()
{
    IsClosed_ = true;
    return arrow20::Status::OK();
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
    auto resultBuffer = TSharedMutableRef::Allocate(DataWeight_);
    auto current = resultBuffer.Begin();
    while (!IsEmpty()) {
        std::memcpy(current, Data_.front().data(), Data_.front().size());
        current += Data_.front().size();
        Data_.pop();
    }
    DataWeight_ = 0;
    auto object = Py::Bytes(resultBuffer.data(), resultBuffer.size());
    object.increment_reference_count();
    return object.ptr();
}

////////////////////////////////////////////////////////////////////////////////

TArrowRawIterator::TArrowRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TArrowRawIterator>::PythonClass(self, args, kwargs)
{ }

void TArrowRawIterator::Initialize(const TString& inputFilePath, EFileFormat format, int arrowBatchSize)
{
    auto* pool = arrow20::default_memory_pool();

    switch (format) {
        case EFileFormat::Orc: {
            RecordBatchReader_ = std::make_shared<TRecordBatchReaderOrcAdapter>(inputFilePath, pool, arrowBatchSize);
            break;
        }
        case EFileFormat::Parquet: {
            parquet20::ReaderProperties readerProperties;
            readerProperties.enable_buffered_stream();

            auto parquetFileReader = parquet20::ParquetFileReader::OpenFile(inputFilePath, /*memory_map*/ true, readerProperties);

            parquet20::ArrowReaderProperties arrowProperties;
            arrowProperties.set_batch_size(arrowBatchSize);

            ThrowOnError(parquet20::arrow20::FileReader::Make(
                pool,
                std::move(parquetFileReader),
                std::move(arrowProperties),
                &ArrowFileReader_));

            std::vector<int> rowGroups(ArrowFileReader_->num_row_groups());
            std::iota(rowGroups.begin(), rowGroups.end(), 0);

            ThrowOnError(ArrowFileReader_->GetRecordBatchReader(rowGroups, &RecordBatchReader_));
        }
    }

    auto recordBatchWriterOrError = arrow20::ipc::MakeStreamWriter(&Pipe_, RecordBatchReader_->schema());
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

Py::Object TArrowRawIterator::NextChunk(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    auto recordBatchWriterOrError = arrow20::ipc::MakeStreamWriter(&Pipe_, RecordBatchReader_->schema());
    ThrowOnError(recordBatchWriterOrError.status());
    RecordBatchWriter_ = recordBatchWriterOrError.ValueOrDie();
    return Py::None();
}

void TArrowRawIterator::InitType()
{
    behaviors().name("yt_yson_bindings.yson_lib.ArrowIterator");
    behaviors().doc("Iterates over parquet file");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    PYCXX_ADD_KEYWORDS_METHOD(get_schema, GetSchema, "Get schema in yson type");
    PYCXX_ADD_KEYWORDS_METHOD(next_chunk, NextChunk, "Switch to next chunk");

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
