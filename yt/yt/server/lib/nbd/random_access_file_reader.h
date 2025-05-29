#pragma once

#include "public.h"

#include "block_device.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/api/private.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TReadersStatistics
{
    i64 ReadBytes = 0;
    i64 DataBytesReadFromCache = 0;
    i64 DataBytesReadFromDisk = 0;
    i64 MetaBytesReadFromDisk = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IRandomAccessFileReader
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length,
        const TReadOptions& options) = 0;

    virtual i64 GetSize() const = 0;

    virtual TReadersStatistics GetStatistics() const = 0;

    virtual NYPath::TYPath GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRandomAccessFileReader);

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    NYPath::TYPath path,
    NChunkClient::TChunkReaderHostPtr readerHost,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
