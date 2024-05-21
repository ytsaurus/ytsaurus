#include "arrow.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NArrow {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TParquetAdapter
    : public arrow::io::RandomAccessFile
{
public:
    TParquetAdapter(const TString* metadata, i64 startMetadataOffset, std::shared_ptr<IInputStream> reader)
        : Metadata_(metadata)
        , StartMetadataOffset_(startMetadataOffset)
        , Reader_(std::move(reader))
    { }

    arrow::Result<int64_t> GetSize() override
    {
        return Metadata_->size() + StartMetadataOffset_;
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override
    {
        if (FilePosition_ < StartMetadataOffset_) {
            auto bytesRead = Reader_->Load(out, nbytes);
            ReaderPosition_ += bytesRead;
            FilePosition_ += bytesRead;
            return bytesRead;
        }

        auto metadataOffset = FilePosition_ - StartMetadataOffset_;
        memcpy(out, Metadata_->begin() + metadataOffset, nbytes);
        FilePosition_ += nbytes;
        return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override
    {
        auto bufferResult = arrow::AllocateResizableBuffer(nbytes);
        ThrowOnError(bufferResult.status());
        auto buffer = std::move(*bufferResult);

        auto bytesReadResult = Read(nbytes, buffer->mutable_data());
        ThrowOnError(bytesReadResult.status());
        auto bytesRead = *bytesReadResult;
        if (bytesRead < nbytes) {
            ThrowOnError(buffer->Resize(bytesRead));
            buffer->ZeroPadding();
        }
        return buffer;
    }

    arrow::Status Seek(int64_t position) override
    {
        if (position >= StartMetadataOffset_) {
            FilePosition_ = position;
        } else {
            if (position < ReaderPosition_) {
                return arrow::Status::Invalid(Format("Position %v is less than current reader position %v", position, ReaderPosition_));
            }
            if (position > ReaderPosition_) {
                auto lenSkip = position - ReaderPosition_;
                while (lenSkip > 0) {
                    auto res = Reader_->Skip(lenSkip);
                    lenSkip -= res;
                    if (res == 0) {
                        return arrow::Status::Invalid("Unexpected end of input stream");
                    }
                }
            }
            ReaderPosition_ = position;
            FilePosition_ = position;
        }

        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override
    {
        return FilePosition_;
    }

    arrow::Status Close() override
    {
        Closed_ = true;
        return arrow::Status::OK();
    }

    bool closed() const override
    {
        return Closed_;
    }

private:
    const TString* const Metadata_;
    const i64 StartMetadataOffset_;
    const std::shared_ptr<IInputStream> Reader_;

    i64 ReaderPosition_ = 0;
    i64 FilePosition_ = 0;
    bool Closed_ = false;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error occurred: %Qv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader)
{
    return std::make_shared<TParquetAdapter>(metadata, startMetadataOffset, std::move(reader));
}

std::shared_ptr<arrow::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex)
{
    auto inputStream = CreateParquetAdapter(metadata, startIndex);
    auto pool = arrow::default_memory_pool();

    auto parquetFileReader = parquet::ParquetFileReader::Open(inputStream);

    std::unique_ptr<parquet::arrow::FileReader> arrowFileReader;
    ThrowOnError(parquet::arrow::FileReader::Make(
            pool,
            std::move(parquetFileReader),
            parquet::ArrowReaderProperties(),
            &arrowFileReader));

    std::shared_ptr<arrow::Schema> arrowSchema;
    ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));
    return arrowSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
