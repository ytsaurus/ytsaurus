#include "chunk_pool.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void TDummyPersistent::Persist(const TPersistenceContext& /*context*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

const TProgressCounterPtr& TChunkPoolOutputMockBase::GetJobCounter() const
{
    return JobCounter;
}

const TProgressCounterPtr& TChunkPoolOutputMockBase::GetDataWeightCounter() const
{
    return DataWeightCounter;
}

const TProgressCounterPtr& TChunkPoolOutputMockBase::GetRowCounter() const
{
    return RowCounter;
}

const TProgressCounterPtr& TChunkPoolOutputMockBase::GetDataSliceCounter() const
{
    return DataSliceCounter;
}

void TChunkPoolOutputMockBase::TeleportChunk(TInputChunkPtr teleportChunk)
{
    ChunkTeleported_.Fire(std::move(teleportChunk), /*tag=*/std::any{});
}

void TChunkPoolOutputMockBase::Complete()
{
    Completed_.Fire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
