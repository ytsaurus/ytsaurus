#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/reader_base.h>
#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/multi_chunk_reader.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

#include <core/compression/public.h>

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
    NChunkClient::TSequentialReaderConfigPtr sequentialConfig,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr uncompressedBlockCache,
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
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr compressedBlockCache,
    NChunkClient::IBlockCachePtr uncompressedBlockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
