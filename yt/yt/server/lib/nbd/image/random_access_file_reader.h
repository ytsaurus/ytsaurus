#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NImage {

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

    virtual const std::string& GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRandomAccessFileReader);

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    std::string path,
    NChunkClient::TChunkReaderHostPtr readerHost,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NImage
