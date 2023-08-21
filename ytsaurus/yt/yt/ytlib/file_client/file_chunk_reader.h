#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public virtual NChunkClient::IReaderBase
{
    virtual bool ReadBlock(NChunkClient::TBlock* block) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateFileReaderAdapter(IFileReaderPtr underlying);

////////////////////////////////////////////////////////////////////////////////

IFileReaderPtr CreateFileChunkReader(
    NChunkClient::TBlockFetcherConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    i64 startOffset,
    i64 endOffset,
    const NChunkClient::TDataSource& dataSource,
    NChunkClient::TChunkReaderMemoryManagerPtr chunkReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

IFileReaderPtr CreateFileMultiChunkReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    const NChunkClient::TDataSource& dataSource,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
