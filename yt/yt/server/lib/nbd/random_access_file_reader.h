#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/api/private.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TRandomAccessFileReaderStatistics
{
    i64 ReadBytes;
    i64 ReadBlockBytesFromCache;
    i64 ReadBlockBytesFromDisk;
    i64 ReadBlockMetaBytesFromDisk;
};

////////////////////////////////////////////////////////////////////////////////

struct IRandomAccessFileReader
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) = 0;

    virtual i64 GetSize() const = 0;

    virtual TRandomAccessFileReaderStatistics GetStatistics() const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRandomAccessFileReader);
DEFINE_REFCOUNTED_TYPE(IRandomAccessFileReader);

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    TString path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler = NConcurrency::GetUnlimitedThrottler(),
    NLogging::TLogger logger = NApi::ApiLogger());

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TString path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    NChunkClient::TUserObject userObject,
    TString path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TUserObject GetUserObject(
    const NYPath::TRichYPath& richPath,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
