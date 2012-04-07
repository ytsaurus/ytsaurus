#include "stdafx.h"
#include "job_list.h"
#include "job.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

void TJobList::Save(TOutputStream* output) const
{
    SaveObjects(output, Jobs_);
}

void TJobList::Load(const TLoadContext& context, TInputStream* input)
{
    LoadObjects(input, Jobs_, context);
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
