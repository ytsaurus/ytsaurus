#include "chunk_spec.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(
    const TChunkReplicaWithMediumList& replicas,
    NErasure::ECodec codecId,
    EChunkAvailabilityPolicy policy)
{
    return IsUnavailable(
        TChunkReplicaWithMedium::ToChunkReplicas(replicas),
        codecId,
        policy);
}

bool IsUnavailable(
    const TChunkReplicaList& replicas,
    NErasure::ECodec codecId,
    EChunkAvailabilityPolicy policy)
{
    if (codecId == NErasure::ECodec::None) {
        return replicas.empty();
    } else {
        auto* codec = NErasure::GetCodec(codecId);

        NErasure::TPartIndexSet erasedIndexSet;
        for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
            erasedIndexSet.set(index);
        }
        for (auto replica : replicas) {
            erasedIndexSet.reset(replica.GetReplicaIndex());
        }

        switch (policy) {
            case EChunkAvailabilityPolicy::DataPartsAvailable:
                for (int index = codec->GetDataPartCount(); index < codec->GetTotalPartCount(); ++index) {
                    erasedIndexSet.reset(index);
                }
                return erasedIndexSet.any();
            case EChunkAvailabilityPolicy::AllPartsAvailable:
                return erasedIndexSet.any();
            case EChunkAvailabilityPolicy::Repairable:
                return !codec->CanRepair(erasedIndexSet);
            default:
                YT_ABORT();
        }
    }
}

bool IsUnavailable(
    const NProto::TChunkSpec& chunkSpec,
    EChunkAvailabilityPolicy policy)
{
    auto replicas = GetReplicasFromChunkSpec(chunkSpec);
    auto codecId = FromProto<NErasure::ECodec>(chunkSpec.erasure_codec());
    return IsUnavailable(replicas, codecId, policy);
}

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<NProto::TChunkSpec>& chunkSpecs)
{
    i64 result = 0;
    for (const auto& chunkSpec : chunkSpecs) {
        YT_VERIFY(chunkSpec.has_row_count_override());
        result += chunkSpec.row_count_override();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
