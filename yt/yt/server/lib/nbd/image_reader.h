#pragma once

#include "random_access_file_reader.h"

#include <yt/yt/server/lib/squash_fs/squash_fs_layout_builder.h>

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

    virtual TString GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IImageReader)

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateCypressFileImageReader(
    IRandomAccessFileReaderPtr reader,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IImageReaderPtr CreateVirtualSquashFSImageReader(
    std::unordered_map<NYPath::TYPath, std::vector<NChunkClient::NProto::TChunkSpec>> pathToChunkSpecs,
    NSquashFS::TSquashFSLayoutPtr layout,
    NApi::NNative::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
