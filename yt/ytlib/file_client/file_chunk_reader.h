#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/reader_base.h>
#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/multi_chunk_reader.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

#include <core/compression/public.h>

#include <core/misc/ref.h>

#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public virtual NChunkClient::IReaderBase
{
    virtual bool ReadBlock(TSharedRef* block) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

////////////////////////////////////////////////////////////////////////////////

struct IFileChunkReader
    : public virtual NChunkClient::IChunkReaderBase
    , public IFileReader
{ };

DEFINE_REFCOUNTED_TYPE(IFileChunkReader)

////////////////////////////////////////////////////////////////////////////////

IFileChunkReaderPtr CreateFileChunkReader(
    NChunkClient::TSequentialReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    i64 startOffset,
    i64 endOffset);

////////////////////////////////////////////////////////////////////////////////

struct IFileMultiChunkReader
    : public virtual NChunkClient::IMultiChunkReader
    , public IFileReader
{ };

DEFINE_REFCOUNTED_TYPE(IFileMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

IFileMultiChunkReaderPtr CreateFileMultiChunkReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NApi::IClientPtr client,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
