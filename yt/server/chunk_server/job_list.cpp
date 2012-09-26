#include "stdafx.h"
#include "job_list.h"
#include "job.h"

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

void TJobList::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    SaveObjectRefs(output, Jobs_);
}

void TJobList::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    LoadObjectRefs(input, Jobs_, context);
}

void TJobList::AddJob(TJob* job)
{
    Jobs_.insert(job);
}

void TJobList::RemoveJob(TJob* job)
{
    Jobs_.erase(job);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
