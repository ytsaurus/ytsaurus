#include "partition_utils.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NFileClient {

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

std::vector<i64> BuildCumulativeChunkSizes(
    const std::vector<TChunkSpec>& chunkSpecs)
{
    std::vector<i64> cumulativeChunkSizes;
    cumulativeChunkSizes.reserve(chunkSpecs.size() + 1);
    cumulativeChunkSizes.push_back(0);
    for (const auto& chunkSpec : chunkSpecs) {
        auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());
        cumulativeChunkSizes.push_back(cumulativeChunkSizes.back() + miscExt.uncompressed_data_size());
    }
    return cumulativeChunkSizes;
}

std::vector<TChunkSpec> SliceFileChunkSpecs(
    const std::vector<TChunkSpec>& chunkSpecs,
    const std::vector<i64>& cumulativeChunkSizes,
    i64 begin,
    i64 end)
{
    YT_VERIFY(std::ssize(cumulativeChunkSizes) == std::ssize(chunkSpecs) + 1);
    YT_VERIFY(0 <= begin && begin <= end && end <= cumulativeChunkSizes.back());

    std::vector<TChunkSpec> result;

    if (begin == end) {
        return result;
    }

    // First chunk overlapping the range: the largest index with cumulativeChunkSizes[index] <= begin.
    auto firstChunkIndexIt = std::upper_bound(cumulativeChunkSizes.begin(), cumulativeChunkSizes.end(), begin);
    auto firstChunkIndex = std::distance(cumulativeChunkSizes.begin(), firstChunkIndexIt) - 1;

    for (auto index = firstChunkIndex; index < std::ssize(chunkSpecs) && cumulativeChunkSizes[index] < end; ++index) {
        auto& spec = result.emplace_back(chunkSpecs[index]);

        auto chunkBegin = cumulativeChunkSizes[index];
        auto chunkEnd = cumulativeChunkSizes[index + 1];

        // TFileChunkReader contract: limits are chunk-relative and only set on boundary chunks.
        if (begin > chunkBegin) {
            spec.mutable_lower_limit()->set_offset(begin - chunkBegin);
        }
        if (end < chunkEnd) {
            spec.mutable_upper_limit()->set_offset(end - chunkBegin);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
