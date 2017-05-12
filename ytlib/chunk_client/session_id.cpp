#include "session_id.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TSessionId& id)
{
    if (id.MediumIndex == AllMediaIndex) {
        return Format("%v@*", id.ChunkId);
    } else {
        return Format("%v@%v", id.ChunkId, id.MediumIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
