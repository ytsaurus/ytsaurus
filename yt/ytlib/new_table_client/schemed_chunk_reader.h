#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): deprecated calls.

//! Creates a reader for new, versioned, chunk format on top of any
//! NChunkClient::IAsyncReader, e.g. TMemoryReader, TReplicationReader etc.
IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const NChunkClient::TReadLimit& startLimit = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& endLimit = NChunkClient::TReadLimit(),
    TTimestamp timestamp = NullTimestamp);

//! Creates a universal reader for any chunk, of any format, no matter local or remote
//! it is. Should be particularly handy for reading old chunks.
IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::IBlockCachePtr blockCache,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader for new, versioned, chunk format on top of any
//! NChunkClient::IAsyncReader, e.g. TMemoryReader, TReplicationReader etc.
ISchemedReaderPtr CreateSchemedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const NChunkClient::TReadLimit& startLimit = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& endLimit = NChunkClient::TReadLimit(),
    TTimestamp timestamp = NullTimestamp);

//! Creates a universal reader for any chunk, of any format, no matter local or remote
//! it is. Should be particularly handy for reading old chunks.
ISchemedReaderPtr CreateSchemedChunkReader(
    TChunkReaderConfigPtr config,
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::IBlockCachePtr blockCache,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
