#include "stdafx.h"
#include "job.h"

namespace NYT {

using namespace NCellMaster;

namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkId& chunkId,
    const Stroka& runnerAddress,
    const yvector<Stroka>& targetAddresses,
    TInstant startTime)
    : Type_(type)
    , JobId_(jobId)
    , ChunkId_(chunkId)
    , RunnerAddress_(runnerAddress)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
{ }

TJob::TJob(const TJobId& jobId)
    : JobId_(jobId)
{ }

void TJob::Save(TOutputStream* output) const
{
    ::Save(output, Type_);
    ::Save(output, ChunkId_);
    ::Save(output, RunnerAddress_);
    ::Save(output, TargetAddresses_);
    ::Save(output, StartTime_);
}

void TJob::Load(TInputStream* input, const TLoadContext& context)
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
