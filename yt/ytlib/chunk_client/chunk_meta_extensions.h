#pragma once

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TMiscExt, 0)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TBlocksExt, 1)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TErasurePlacementExt, 2)

////////////////////////////////////////////////////////////////////////////////

namespace NChunkClient {

NChunkClient::NProto::TChunkMeta FilterChunkMetaByExtensionTags(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TNullable<std::vector<int>>& extensionTags);

} // namespace NChunkClient

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
