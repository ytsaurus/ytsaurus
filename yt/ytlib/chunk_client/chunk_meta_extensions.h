#pragma once

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TMiscExt, 0)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TBlocksExt, 1)

///////////////////////////////////////////////////////////////////////////////

namespace NChunkClient {

NChunkClient::NProto::TChunkMeta FilterChunkMetaExtensions(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const std::vector<int>& tags);

} // namespace NChunkClient

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT