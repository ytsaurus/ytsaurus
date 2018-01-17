#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/reader_base.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public virtual NChunkClient::IReaderBase
{
    virtual bool ReadBlock(NChunkClient::TBlock* block) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

////////////////////////////////////////////////////////////////////////////////

IFileReaderPtr CreateFileChunkReader(
    NChunkClient::TBlockFetcherConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    const NChunkClient::TReadSessionId& sessionId,
    i64 startOffset,
    i64 endOffset);

////////////////////////////////////////////////////////////////////////////////

IFileReaderPtr CreateFileMultiChunkReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NApi::INativeClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TReadSessionId& sessionId,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
