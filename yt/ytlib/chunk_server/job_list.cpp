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

TJobList::TJobList(const TJobList& other)
    : ChunkId_(other.ChunkId_)
    , Jobs_(other.Jobs_)
{ }

void TJobList::Save(TOutputStream* output) const
{
    SaveObjects(output, Jobs_);
}

void TJobList::Load(const NCellMaster::TLoadContext& context, TInputStream* input)
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
