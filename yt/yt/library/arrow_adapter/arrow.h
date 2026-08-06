#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <atomic>

#include <util/generic/fwd.h>

#include <util/stream/input.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/interfaces.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

using TArrowSchemaPtr = std::shared_ptr<arrow20::Schema>;
using TArrowRandomAccessFilePtr = std::shared_ptr<arrow20::io::RandomAccessFile>;

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow20::Status& status);

////////////////////////////////////////////////////////////////////////////////

class TRingBuffer
{
public:
    explicit TRingBuffer(i64 bufferSize);

    void Read(i64 offset, i64 byteCount, char* output);

    arrow20::Status Write(TRef data);

    i64 GetBeginPosition() const;

    i64 GetEndPosition() const;

private:
    const i64 BufferSize_;

    const TSharedMutableRef Buffer_;
    i64 BufferPosition_ = 0;
    i64 BeginPosition_ = 0;
    i64 FirstRingBufferPosition_ = 0;
    i64 EndPosition_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Creating the TArrowRandomAccessFilePtr class, which combines a stream with data and a row with metadata into one stream.
// Reader can be nullptr when we read only from metadata.
TArrowRandomAccessFilePtr CreateParquetAdapter(
    const std::string* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader = nullptr);

TArrowRandomAccessFilePtr CreateOrcAdapter(
    const std::string* metadata,
    i64 startMetadataOffset,
    i64 maxStripeSize = 1,
    std::shared_ptr<IInputStream> reader = nullptr);

i64 GetMaxStripeSize(const std::string* metadata, i64 startMetadataOffset);

TArrowSchemaPtr CreateArrowSchemaFromParquetMetadata(const std::string* metadata, i64 startIndex);

TArrowSchemaPtr CreateArrowSchemaFromOrcMetadata(const std::string* metadata, i64 startIndex);

////////////////////////////////////////////////////////////////////////////////

//! A small adapter for sources which efficiently implement random reads only.
//! Arrow's stateful methods are implemented in terms of ReadAt.
class TStatelessArrowRandomAccessFileBase
    : public arrow20::io::RandomAccessFile
{
public:
    arrow20::Result<int64_t> GetSize() override = 0;
    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override = 0;

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

    arrow20::Status Seek(int64_t position) override;
    arrow20::Result<int64_t> Tell() const override;

    arrow20::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nbytes) override;

    arrow20::Status Close() override;
    bool closed() const override;

private:
    std::atomic<bool> Closed_ = false;
    i64 FilePosition_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Presents disjoint ranges of a file as one Arrow random-access file.
//! Used to reconstruct a Parquet row group together with its original footer.
class TCompositeBufferArrowRandomAccessFile
    : public TStatelessArrowRandomAccessFileBase
{
public:
    struct TBufferDescriptor
    {
        TSharedRef Data;
        i64 Offset;
    };

    TCompositeBufferArrowRandomAccessFile(std::vector<TBufferDescriptor> buffers, i64 fileSize);

    arrow20::Result<int64_t> GetSize() override;
    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

private:
    const std::vector<TBufferDescriptor> Buffers_;
    const i64 FileSize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
