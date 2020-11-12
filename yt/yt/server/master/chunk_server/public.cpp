#include "public.h"

#include "chunk.h"

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkPartLossTimeComparer::TChunkPartLossTimeComparer(TEpoch epoch)
    : Epoch_(epoch)
{ }

bool TChunkPartLossTimeComparer::operator()(const TChunk* lhs, const TChunk* rhs) const
{
    return lhs->GetPartLossTime(Epoch_) < rhs->GetPartLossTime(Epoch_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
