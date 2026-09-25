#pragma once

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! Returns cumulative sizes of chunk prefixes: N+1 elements for N chunks.
//! Chunk[i] boundaries: [result[i], result[i + 1]).
//! Requires TMiscExt to be present in every chunk spec meta.
std::vector<i64> BuildCumulativeChunkSizes(
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs);

//! Slices file byte range [begin, end) into chunk specs with chunk-relative offset limits on boundary chunks.
//! Requires 0 <= begin <= end <= cumulativeChunkSizes.back().
std::vector<NChunkClient::NProto::TChunkSpec> SliceFileChunkSpecs(
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    const std::vector<i64>& cumulativeChunkSizes,
    i64 begin,
    i64 end);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
