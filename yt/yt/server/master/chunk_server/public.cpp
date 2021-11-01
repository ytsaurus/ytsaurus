#include "public.h"

#include "chunk.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

bool TChunkPartLossTimeComparer::operator()(const TChunk* lhs, const TChunk* rhs) const
{
    return lhs->GetPartLossTime() < rhs->GetPartLossTime();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
