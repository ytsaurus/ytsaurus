#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TMiscExt, 0)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TBlocksExt, 1)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TErasurePlacementExt, 2)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::THunkRefsExt, 3)

////////////////////////////////////////////////////////////////////////////////

namespace NChunkClient {

NChunkClient::NProto::TChunkMeta FilterChunkMetaByExtensionTags(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const std::optional<std::vector<int>>& extensionTags);

} // namespace NChunkClient

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
