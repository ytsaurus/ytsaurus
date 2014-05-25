#pragma once

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TMiscExt, 0)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TBlocksExt, 1)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TErasurePlacementExt, 2)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TJournalExt, 3)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TSizeOverrideExt, 16)

///////////////////////////////////////////////////////////////////////////////

namespace NChunkClient {

NChunkClient::NProto::TChunkMeta FilterChunkMetaByExtensionTags(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const std::vector<int>& tags);

} // namespace NChunkClient

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
