#include "arrow_raw_iterator.h"

#include <yt/yt/core/ytree/convert.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>

namespace NYT::NPython {

using namespace arrow;

////////////////////////////////////////////////////////////////////////////////

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
    auto object = Py::Bytes(buffer.Data(), buffer.Size());
    object.increment_reference_count();
    Data_.pop();
    return object.ptr();
}

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const Status& status)
{
    if (!status.ok()) {
        throw Py::RuntimeError(status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

TArrowRawIterator::TArrowRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TArrowRawIterator>::PythonClass(self, args, kwargs)
{ }

void TArrowRawIterator::Initialize(const TString& pathToFile)
{
    auto* pool = arrow::default_memory_pool();

    parquet::ReaderProperties readerProperties;
    readerProperties.enable_buffered_stream();

    auto parquetFileReader = parquet::ParquetFileReader::OpenFile(pathToFile, /*memory_map*/ true, readerProperties);

    ThrowOnError(parquet::arrow::FileReader::Make(
        pool,
        std::move(parquetFileReader),
        parquet::ArrowReaderProperties(),
        &ArrowFileReader_));

    std::vector<int> rowGroups(ArrowFileReader_->num_row_groups());
    std::iota(rowGroups.begin(), rowGroups.end(), 0);

    ThrowOnError(ArrowFileReader_->GetRecordBatchReader(rowGroups, &RecordBatchReader_));

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

void TArrowRawIterator::InitType()
{
    behaviors().name("yt_yson_bindings.yson_lib.ArrowIterator");
    behaviors().doc("Iterates over parquet file");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
