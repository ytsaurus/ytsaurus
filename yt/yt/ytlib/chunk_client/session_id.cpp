#include "session_id.h"

namespace NYT::NChunkClient {

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
