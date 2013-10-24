#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

// Creates reader for new, versioned, chunk format on top of any
// NChunkClient::IAsyncReader, e.g. TMemoryReader, TReplicationReader etc.
IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const NChunkClient::NProto::TReadLimit& startLimit = NChunkClient::NProto::TReadLimit(),
    const NChunkClient::NProto::TReadLimit& endLimit = NChunkClient::NProto::TReadLimit(),
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

// Creates universal reader for any chunk, of any format, no matter local or remote
// it is. Should be particularly handy for reading old chunks.
IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::IBlockCachePtr blockCache,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
