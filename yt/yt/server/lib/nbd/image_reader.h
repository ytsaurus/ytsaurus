#pragma once

#include "random_access_file_reader.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct IImageReader
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) = 0;

    virtual i64 GetSize() const = 0;

    virtual TReadersStatistics GetStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IImageReader)

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    NYPath::TYPath path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateVirtualSquashFSImageReader(
    std::unordered_map<NYPath::TYPath, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    NSquashFS::TSquashFSImagePtr image,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
