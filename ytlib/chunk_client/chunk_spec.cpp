#include "chunk_spec.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"
#include "read_limit.h"

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(const TChunkReplicaList& replicas, NErasure::ECodec codecId, bool checkParityParts)
{
    if (codecId == NErasure::ECodec::None) {
        return replicas.empty();
    } else {
        auto* codec = NErasure::GetCodec(codecId);
        int partCount = checkParityParts ? codec->GetTotalPartCount() : codec->GetDataPartCount();
        NErasure::TPartIndexSet missingIndexSet((1 << partCount) - 1);
        for (auto replica : replicas) {
            missingIndexSet.reset(replica.GetIndex());
        }
        return missingIndexSet.any();
    }
}

bool IsUnavailable(const NProto::TChunkSpec& chunkSpec, bool checkParityParts)
{
    auto codecId = NErasure::ECodec(chunkSpec.erasure_codec());
    auto replicas = NYT::FromProto<TChunkReplicaList>(chunkSpec.replicas());
    return IsUnavailable(replicas, codecId, checkParityParts);
}

void GetStatistics(
    const TChunkMeta& meta,
    i64* dataSize,
    i64* rowCount)
{
    auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
    auto sizeOverrideExt = FindProtoExtension<TSizeOverrideExt>(meta.extensions());

    if (sizeOverrideExt) {
        if (dataSize) {
            *dataSize = sizeOverrideExt->uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = sizeOverrideExt->row_count();
        }
    } else {
        if (dataSize) {
            *dataSize = miscExt.uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = miscExt.row_count();
        }
    }
}

i64 GetCumulativeRowCount(const std::vector<NProto::TChunkSpec>& chunkSpecs)
{
    i64 result = 0;
    for (const auto& chunkSpec : chunkSpecs) {
        auto miscExt = FindProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());
        if (!miscExt) {
            return std::numeric_limits<i64>::max();
        }

        i64 upperRowLimit = miscExt->row_count();
        i64 lowerRowLimit = 0;
        if (chunkSpec.has_lower_limit() && chunkSpec.lower_limit().has_row_index()) {
            lowerRowLimit = chunkSpec.lower_limit().row_index();
        }

        if (chunkSpec.has_upper_limit() && chunkSpec.upper_limit().has_row_index()) {
            upperRowLimit = chunkSpec.upper_limit().row_index();
        }

        result += upperRowLimit - lowerRowLimit;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
