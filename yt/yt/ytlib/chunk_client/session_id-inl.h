#ifndef SESSION_ID_INL_H_
#error "Direct inclusion of this file is not allowed, include session_id.h"
// For the sake of sane code completion.
#include "session_id.h"
#endif

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

inline void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId)
{
    FromProto(&sessionId->ChunkId, protoSessionId.chunk_id());
    sessionId->MediumIndex = protoSessionId.medium_index();
}

inline void ToProto(NProto::TSessionId* protoSessionId, TSessionId sessionId)
{
    ToProto(protoSessionId->mutable_chunk_id(), sessionId.ChunkId);
    protoSessionId->set_medium_index(sessionId.MediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

inline TSessionId::TSessionId()
    : TSessionId(TChunkId(), DefaultStoreMediumIndex)
{ }

inline TSessionId::TSessionId(TChunkId chunkId, int mediumIndex)
    : ChunkId(chunkId)
    , MediumIndex(mediumIndex)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
