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
            missingIndexSet.reset(replica.GetReplicaIndex());
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

i64 GetCumulativeRowCount(const std::vector<NProto::TChunkSpec>& chunkSpecs)
{
    i64 result = 0;
    for (const auto& chunkSpec : chunkSpecs) {
        YCHECK(chunkSpec.has_row_count_override());
        result += chunkSpec.row_count_override();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
