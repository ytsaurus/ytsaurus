#pragma once
#ifndef SESSION_ID_INL_H_
#error "Direct inclusion of this file is not allowed, include session_id.h"
#endif

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

inline void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId)
{
    FromProto(&sessionId->ChunkId, protoSessionId.chunk_id());
    sessionId->MediumIndex = protoSessionId.medium_index();
}

inline void ToProto(NProto::TSessionId* protoSessionId, const TSessionId& sessionId)
{
    ToProto(protoSessionId->mutable_chunk_id(), sessionId.ChunkId);
    protoSessionId->set_medium_index(sessionId.MediumIndex);
}

Y_FORCE_INLINE TSessionId::TSessionId()
    : TSessionId(TChunkId(), DefaultStoreMediumIndex)
{ }

Y_FORCE_INLINE TSessionId::TSessionId(const TChunkId& chunkId, int mediumIndex)
    : ChunkId(chunkId)
    , MediumIndex(mediumIndex)
{ }

Y_FORCE_INLINE bool operator==(const TSessionId& lhs, const TSessionId& rhs)
{
    return lhs.ChunkId == rhs.ChunkId && lhs.MediumIndex == rhs.MediumIndex;
}

Y_FORCE_INLINE bool operator!=(const TSessionId& lhs, const TSessionId& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
