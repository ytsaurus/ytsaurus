#include "arrow.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/adapters/orc/adapter.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

struct TRingBufferAdapterTag
{ };

////////////////////////////////////////////////////////////////////////////////

TRingBuffer::TRingBuffer(i64 bufferSize)
    : BufferSize_(bufferSize)
    , Buffer_(TSharedMutableRef::Allocate<TRingBufferAdapterTag>(bufferSize, {.InitializeStorage = false}))
{ }

void TRingBuffer::Read(i64 offset, i64 nBytes, char* out)
{
    YT_VERIFY(offset + nBytes <= EndPosition_);
    YT_VERIFY(offset >= BeginPosition_);

    auto startReadPosition = (FirstRingBufferPosition_ + (offset - BeginPosition_)) % BufferSize_;
    auto restSize = BufferSize_ - startReadPosition;

    if (nBytes <= restSize) {
        // One copy is enough.
        // In the case when there is more space between the current read position and the end of the buffer
        // than the size of the data we want to read.
        std::memcpy(out, Buffer_.Begin() + startReadPosition, nBytes);
    } else {
        // Two copies are needed.
        // In the case when there is less space between the current read position and the end of the buffer
        // than the size of the data we want to read.
        std::memcpy(out, Buffer_.Begin() + startReadPosition, restSize);
        std::memcpy(out + restSize, Buffer_.Begin(), nBytes - restSize);
    }
}

arrow::Status TRingBuffer::Write(TSharedRef data)
{
    auto size = static_cast<i64>(data.Size());
    if (size > BufferSize_) {
        data = data.Slice(size - BufferSize_, size);
        size = BufferSize_;
    }

    auto restSize = BufferSize_ - BufferPosition_;
    if (size <= restSize) {
        // One copy is enough.
        // In the case when there is more space between the current write position and the end of the buffer
        // than the size of the data we want to write.
        // For example:
        // ..............
        //     ^ - current write position
        //     .... - data we want to write
        memcpy(Buffer_.Begin() + BufferPosition_, data.Begin(), size);
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
        memcpy(Buffer_.Begin() + BufferPosition_, data.Begin(), restSize);
        memcpy(Buffer_.Begin(), data.Begin() + restSize, size - restSize);
        BufferPosition_ += size;
    }
    BufferPosition_ %= BufferSize_;

    EndPosition_ += size;
    if (EndPosition_ - BeginPosition_ > BufferSize_) {
        BeginPosition_ = EndPosition_ - BufferSize_;
        FirstRingBufferPosition_ = BufferPosition_;
    }
    return arrow::Status::OK();
}

i64 TRingBuffer::GetBeginPosition() const
{
    return BeginPosition_;
}

i64 TRingBuffer::GetEndPosition() const
{
    return EndPosition_;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TORCAdapter
    : public arrow::io::RandomAccessFile
{
public:
    TORCAdapter(const TString* metadata, i64 startMetadataOffset, i64 maxStripeSize, std::shared_ptr<IInputStream> reader)
        : Metadata_(metadata)
        , StartMetadataOffset_(startMetadataOffset)
        , Reader_(std::move(reader))
        , RingBuffer_(maxStripeSize)
    { }

    arrow::Result<int64_t> GetSize() override
    {
        return Metadata_->size() + StartMetadataOffset_;
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override
    {
        if (FilePosition_ < StartMetadataOffset_) {
            if (FilePosition_ + nbytes >= RingBuffer_.GetEndPosition()) {
                auto partData = TSharedMutableRef::Allocate<TRingBufferAdapterTag>(FilePosition_ + nbytes + 1 - RingBuffer_.GetEndPosition());
                YT_VERIFY(Reader_);
                Reader_->Read(partData.Begin(), partData.Size());
                auto result = RingBuffer_.Write(partData);
                if (!result.ok()) {
                    return result;
                }
            }
            if (FilePosition_ < RingBuffer_.GetBeginPosition()) {
                return arrow::Status::Invalid("Position %v is less than first record position %v", FilePosition_, RingBuffer_.GetBeginPosition());
            }
            RingBuffer_.Read(FilePosition_, nbytes, static_cast<char*>(out));
            FilePosition_ += nbytes;
            return nbytes;
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
       if (position < StartMetadataOffset_) {
            if (position < RingBuffer_.GetBeginPosition()) {
                return arrow::Status::Invalid("Position %v is less than first record position %v", position, RingBuffer_.GetBeginPosition());
            }
        }
        FilePosition_ = position;

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

    TRingBuffer RingBuffer_;
    i64 FilePosition_ = 0;
    bool Closed_ = false;
};

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

class TORCMetadataStream
    : public orc::InputStream
{
public:
    TORCMetadataStream(const TString* metadata, i64 startMetadataOffset)
        : Metadata_(metadata)
        , StartMetadataOffset_(startMetadataOffset)
    { }

    uint64_t getLength() const override
    {
        return StartMetadataOffset_ + Metadata_->size();
    }

    uint64_t getNaturalReadSize() const override
    {
        return NaturalReadSize_;
    }

    void read(void* buf, uint64_t length, uint64_t offset) override
    {
        if (static_cast<i64>(offset) < StartMetadataOffset_) {
            THROW_ERROR_EXCEPTION("Metadata size of ORC file is too big");
        }
        std::memcpy(buf, Metadata_->data() + offset - StartMetadataOffset_, length);
    }

    const std::string& getName() const override
    {
        return Name_;
    }

private:
    // The number of bytes that should be read at once.
    static constexpr int NaturalReadSize_ = 1;

    const std::string Name_ = "ORCMetadataStream";
    const TString* Metadata_;
    const i64 StartMetadataOffset_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

i64 GetMaxStripeSize(const TString* metadata, i64 startMetadataOffset)
{
    orc::ReaderOptions readOpts;
    std::unique_ptr<orc::Reader> reader = orc::createReader(
        std::make_unique<TORCMetadataStream>(metadata, startMetadataOffset),
        readOpts);

    uint64_t stripeCount = reader->getNumberOfStripes();

    i64 maxStripeSize = 0;
    for (uint64_t stripeIndex = 0; stripeIndex < stripeCount; ++stripeIndex) {
        auto stripe = reader->getStripe(stripeIndex);
        maxStripeSize = std::max(maxStripeSize, static_cast<i64>(stripe->getLength()));
    }
    return maxStripeSize;
}

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error occurred: %Qv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

TArrowRandomAccessFilePtr CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader)
{
    return std::make_shared<TParquetAdapter>(metadata, startMetadataOffset, std::move(reader));
}

TArrowRandomAccessFilePtr CreateORCAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    i64 maxStripeSize,
    std::shared_ptr<IInputStream> reader)
{
    return std::make_shared<TORCAdapter>(metadata, startMetadataOffset, maxStripeSize, std::move(reader));
}

TArrowSchemaPtr CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex)
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

    TArrowSchemaPtr arrowSchema;
    ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));
    return arrowSchema;
}

TArrowSchemaPtr CreateArrowSchemaFromORCMetadata(const TString* metadata, i64 startIndex)
{
    auto inputStream = CreateORCAdapter(metadata, startIndex);
    auto pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;
    ThrowOnError(arrow::adapters::orc::ORCFileReader::Open(
        inputStream,
        pool,
        &reader));
    TArrowSchemaPtr arrowSchema;
    ThrowOnError(reader->ReadSchema(&arrowSchema));
    return arrowSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
