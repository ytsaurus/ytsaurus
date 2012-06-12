#pragma once

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkHolder::NProto::TMiscExt, 0)
DECLARE_PROTO_EXTENSION(NChunkHolder::NProto::TBlocksExt, 1)

///////////////////////////////////////////////////////////////////////////////

namespace NChunkHolder {

NChunkHolder::NProto::TChunkMeta FilterChunkMetaExtensions(
    const NChunkHolder::NProto::TChunkMeta& chunkMeta,
    const std::vector<int>& tags);

} // namespace NChunkHolder

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT