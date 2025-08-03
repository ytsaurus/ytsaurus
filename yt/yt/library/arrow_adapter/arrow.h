#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/fwd.h>

#include <util/stream/input.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

using TArrowSchemaPtr = std::shared_ptr<arrow::Schema>;
using TArrowRandomAccessFilePtr = std::shared_ptr<arrow::io::RandomAccessFile>;

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status);

////////////////////////////////////////////////////////////////////////////////

class TRingBuffer
{
public:
    explicit TRingBuffer(i64 bufferSize);

    void Read(i64 offset, i64 nBytes, char* out);

    arrow::Status Write(TSharedRef data);

    i64 GetBeginPosition() const;

    i64 GetEndPosition() const;

private:
    const i64 BufferSize_;

    TSharedMutableRef Buffer_;
    i64 BufferPosition_ = 0;
    i64 BeginPosition_ = 0;
    i64 FirstRingBufferPosition_ = 0;
    i64 EndPosition_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Creating the TArrowRandomAccessFilePtr class, which combines a stream with data and a row with metadata into one stream.
// Reader can be nullptr when we read only from metadata.
TArrowRandomAccessFilePtr CreateParquetAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    std::shared_ptr<IInputStream> reader = nullptr);

TArrowRandomAccessFilePtr CreateORCAdapter(
    const TString* metadata,
    i64 startMetadataOffset,
    i64 maxStripeSize = 1,
    std::shared_ptr<IInputStream> reader = nullptr);

i64 GetMaxStripeSize(const TString* metadata, i64 startMetadataOffset);

TArrowSchemaPtr CreateArrowSchemaFromParquetMetadata(const TString* metadata, i64 startIndex);

TArrowSchemaPtr CreateArrowSchemaFromORCMetadata(const TString* metadata, i64 startIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
