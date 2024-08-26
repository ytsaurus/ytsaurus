#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/api/private.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TReadersStatistics
{
    i64 ReadBytes = 0;
    i64 ReadBlockBytesFromCache = 0;
    i64 ReadBlockBytesFromDisk = 0;
    i64 ReadBlockMetaBytesFromDisk = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IRandomAccessFileReader
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) = 0;

    virtual i64 GetSize() const = 0;

    virtual TReadersStatistics GetStatistics() const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRandomAccessFileReader);
DEFINE_REFCOUNTED_TYPE(IRandomAccessFileReader);

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TString path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
