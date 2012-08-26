#include "stdafx.h"
#include "job_list.h"
#include "job.h"

#include <server/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

void TJobList::Save(TOutputStream* output) const
{
    SaveObjectRefs(output, Jobs_);
}

void TJobList::Load(const TLoadContext& context, TInputStream* input)
{
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
