#include "stdafx.h"
#include "job_list.h"
#include "job.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

void TJobList::Save(const NCellMaster::TSaveContext& context) const
{
    SaveObjectRefs(context, Jobs_);
}

void TJobList::Load(const NCellMaster::TLoadContext& context)
{
    LoadObjectRefs(context, Jobs_);
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
