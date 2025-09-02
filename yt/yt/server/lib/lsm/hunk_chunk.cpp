#include "hunk_chunk.h"

namespace NYT::NLsm {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

THunkChunk::THunkChunk(TChunkId id, i64 totalHunkLength, i64 referencedTotalHunkLength)
    : Id_(id)
    , TotalHunkLength_(totalHunkLength)
    , ReferencedTotalHunkLength_(referencedTotalHunkLength)
{}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
