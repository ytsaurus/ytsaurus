#pragma once

#include <yt/yt/server/lib/nbd/random_access_file_reader.h>
#include <yt/yt/server/lib/nbd/squash_fs_image_builder.h>

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

DECLARE_REFCOUNTED_STRUCT(IImageReader)
DEFINE_REFCOUNTED_TYPE(IImageReader)

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TString path,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateVirtualSquashFsImageReader(
    std::unordered_map<TString, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    NSquashFs::TSquashFsImagePtr image,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
