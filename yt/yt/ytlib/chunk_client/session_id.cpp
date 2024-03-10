#include "session_id.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

TSessionId::TSessionId()
    : TSessionId(TChunkId(), DefaultStoreMediumIndex)
{ }

TSessionId::TSessionId(TChunkId chunkId, int mediumIndex)
    : ChunkId(chunkId)
    , MediumIndex(mediumIndex)
{ }

////////////////////////////////////////////////////////////////////////////////

void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId)
{
    FromProto(&sessionId->ChunkId, protoSessionId.chunk_id());
    sessionId->MediumIndex = protoSessionId.medium_index();
}

void ToProto(NProto::TSessionId* protoSessionId, TSessionId sessionId)
{
    ToProto(protoSessionId->mutable_chunk_id(), sessionId.ChunkId);
    protoSessionId->set_medium_index(sessionId.MediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TSessionId id, TStringBuf /*format*/)
{
    if (id.MediumIndex == AllMediaIndex) {
        builder->AppendFormat("%v@*", id.ChunkId);
    } else {
        builder->AppendFormat("%v@%v", id.ChunkId, id.MediumIndex);
    }
}

TString ToString(TSessionId id)
{
    return ToStringViaBuilder(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

size_t THash<NYT::NChunkClient::TSessionId>::operator()(NYT::NChunkClient::TSessionId value) const
{
    return THash<NYT::NChunkClient::TChunkId>()(value.ChunkId) * 497 + value.MediumIndex;
}
