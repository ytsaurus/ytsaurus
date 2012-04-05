#include "stdafx.h"
#include "job.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkId& chunkId,
    const Stroka& runnerAddress,
    const yvector<Stroka>& targetAddresses,
    TInstant startTime)
    : Type_(type)
    , Id_(jobId)
    , ChunkId_(chunkId)
    , RunnerAddress_(runnerAddress)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
{ }

TJob::TJob(const TJobId& jobId)
    : Id_(jobId)
{ }

void TJob::Save(TOutputStream* output) const
{
    ::Save(output, Type_);
    ::Save(output, ChunkId_);
    ::Save(output, RunnerAddress_);
    ::Save(output, TargetAddresses_);
    ::Save(output, StartTime_);
}

void TJob::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    ::Load(input, Type_);
    ::Load(input, ChunkId_);
    ::Load(input, RunnerAddress_);
    ::Load(input, TargetAddresses_);
    ::Load(input, StartTime_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
