#pragma once

#include "random_access_file_reader.h"

#include "block_device.h"

#include <yt/yt/server/lib/squash_fs/squash_fs_layout_builder.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactMountOptions
{
    TString Path;
    ui16 Permissions;
    IRandomAccessFileReaderPtr Reader;
};

////////////////////////////////////////////////////////////////////////////////

struct IImageReader
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length,
        const TReadOptions& options) = 0;

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
    std::vector<TArtifactMountOptions> mountOptions,
    NSquashFS::TSquashFSLayoutBuilderOptions builderOptions,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
