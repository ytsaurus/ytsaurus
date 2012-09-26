#include "stdafx.h"
#include "job.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkId& chunkId,
    const Stroka& Address,
    const std::vector<Stroka>& targetAddresses,
    TInstant startTime)
    : Type_(type)
    , TObjectWithIdBase(jobId)
    , ChunkId_(chunkId)
    , Address_(Address)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
{ }

TJob::TJob(const TJobId& jobId)
    : TObjectWithIdBase(jobId)
{ }

void TJob::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    ::Save(output, Type_);
    ::Save(output, ChunkId_);
    ::Save(output, Address_);
    ::Save(output, TargetAddresses_);
    ::Save(output, StartTime_);
}

void TJob::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, Type_);
    ::Load(input, ChunkId_);
    ::Load(input, Address_);
    ::Load(input, TargetAddresses_);
    ::Load(input, StartTime_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
