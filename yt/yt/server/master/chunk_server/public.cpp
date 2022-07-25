#include "public.h"

#include "chunk.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

bool TChunkPartLossTimeComparer::operator()(const TChunk* lhs, const TChunk* rhs) const
{
    return lhs->GetPartLossTime() < rhs->GetPartLossTime();
}

////////////////////////////////////////////////////////////////////////////////

int TChunkToShardIndex::operator()(const TChunk* chunk) const
{
    return chunk->GetShardIndex();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
