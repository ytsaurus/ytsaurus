#include "arrow.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/buffered.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/compressed.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/file.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/interfaces.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/writer.h>

namespace NYT::NArrow {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TParquetAdapter
    : public arrow20::io::RandomAccessFile
{
public:
    TParquetAdapter(const TString* metadata, i64 startMetadataOffset, std::shared_ptr<IInputStream> reader)
        : Metadata_(metadata)
        , StartMetadataOffset_(startMetadataOffset)
        , Reader_(std::move(reader))
    { }

    arrow20::Result<int64_t> GetSize() override
    {
        return Metadata_->size() + StartMetadataOffset_;
    }

    arrow20::Result<int64_t> Read(int64_t nbytes, void* out) override
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

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nbytes) override
    {
        auto bufferResult = arrow20::AllocateResizableBuffer(nbytes);
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

    arrow20::Status Seek(int64_t position) override
    {
        if (position >= StartMetadataOffset_) {
            FilePosition_ = position;
        } else {
            if (position < ReaderPosition_) {
                return arrow20::Status::Invalid(Format("Position %v is less than current reader position %v", position, ReaderPosition_));
            }
            if (position > ReaderPosition_) {
                auto lenSkip = position - ReaderPosition_;
                while (lenSkip > 0) {
                    auto res = Reader_->Skip(lenSkip);
                    lenSkip -= res;
                    if (res == 0) {
                        return arrow20::Status::Invalid("Unexpected end of input stream");
                    }
                }
            }
            ReaderPosition_ = position;
            FilePosition_ = position;
        }

        return arrow20::Status::OK();
    }

    arrow20::Result<int64_t> Tell() const override
    {
        return FilePosition_;
    }

    arrow20::Status Close() override
    {
        Closed_ = true;
        return arrow20::Status::OK();
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

void ThrowOnError(const arrow20::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error occurred: %Qv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader)
{
    return std::make_shared<TParquetAdapter>(metadata, startMetadataOffset, std::move(reader));
}

std::shared_ptr<arrow20::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex)
{
    auto inputStream = CreateParquetAdapter(metadata, startIndex);
    auto pool = arrow20::default_memory_pool();

    auto parquetFileReader = parquet20::ParquetFileReader::Open(inputStream);

    std::unique_ptr<parquet20::arrow20::FileReader> arrowFileReader;
    ThrowOnError(parquet20::arrow20::FileReader::Make(
            pool,
            std::move(parquetFileReader),
            parquet20::ArrowReaderProperties(),
            &arrowFileReader));

    std::shared_ptr<arrow20::Schema> arrowSchema;
    ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));
    return arrowSchema;
}

////////////////////////////////////////////////////////////////////////////////

arrow20::Status TStatlessArrowRandomAccessFileBase::Seek(int64_t position)
{
    FilePosition_ = position;
    return arrow20::Status::OK();
}

arrow20::Result<int64_t> TStatlessArrowRandomAccessFileBase::Tell() const
{
    return FilePosition_;
}

arrow20::Result<int64_t> TStatlessArrowRandomAccessFileBase::Read(int64_t nbytes, void* out)
{
    ARROW_ASSIGN_OR_RAISE(auto bytesRead, ReadAt(FilePosition_, nbytes, out));
    FilePosition_ += bytesRead;
    return bytesRead;
}

arrow20::Result<std::shared_ptr<arrow20::Buffer>> TStatlessArrowRandomAccessFileBase::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(FilePosition_, nbytes));
    FilePosition_ += buffer->size();
    return buffer;
}

arrow20::Status TStatlessArrowRandomAccessFileBase::Close()
{
    Closed_.store(true);
    return arrow20::Status::OK();
}

bool TStatlessArrowRandomAccessFileBase::closed() const
{
    return Closed_.load();
}

arrow20::Result<std::shared_ptr<arrow20::Buffer>> TStatlessArrowRandomAccessFileBase::ReadAt(int64_t position, int64_t nbytes)
{
    auto bufferResult = arrow20::AllocateResizableBuffer(nbytes);
    ARROW_ASSIGN_OR_RAISE(auto buffer, bufferResult);

    ARROW_ASSIGN_OR_RAISE(auto bytesRead, ReadAt(position, nbytes, buffer->mutable_data()));

    if (bytesRead < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(bytesRead));
        buffer->ZeroPadding();
    }

    return buffer;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeBufferArrowRandomAccessFile::TCompositeBufferArrowRandomAccessFile(const std::vector<TBufferDescriptor>& buffers, i64 fileSize)
    : Buffers_(buffers)
    , FileSize_(fileSize)
{
    YT_VERIFY(!Buffers_.empty());
    YT_VERIFY(FileSize_ > 0);
    YT_VERIFY(std::all_of(Buffers_.begin(), Buffers_.end(), [this] (const TBufferDescriptor& buffer) {
        return buffer.Offset >= 0 && buffer.Offset + std::ssize(buffer.Data) <= FileSize_;
    }));
}

arrow20::Result<int64_t> TCompositeBufferArrowRandomAccessFile::GetSize()
{
    return FileSize_;
}

arrow20::Result<int64_t> TCompositeBufferArrowRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    if (position < 0 || position + nbytes > FileSize_) {
        return arrow20::Status::IOError(Format(
            "Cannot read %v bytes at position %v from file of size %v", nbytes, position, FileSize_));
    }

    for (const auto& buffer : Buffers_) {
        if (position >= buffer.Offset && position + nbytes <= buffer.Offset + std::ssize(buffer.Data)) {
            i64 offsetInBuffer = position - buffer.Offset;
            std::memcpy(out, buffer.Data.Begin() + offsetInBuffer, nbytes);
            return nbytes;
        }
    }

    return arrow20::Status::IOError(Format("Requested read range [%v-%v) does not fall into any buffer", position, position + nbytes));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
