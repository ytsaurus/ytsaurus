#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move impl to cpp
struct TJobList
{
    TJobList(const TChunkId& chunkId)
        : ChunkId_(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId_(other.ChunkId_)
        , JobIds_(other.JobIds_)
    { }

    void Save(TOutputStream* output) const
    {
        ::Save(output, JobIds_);
    }

    void Load(TInputStream* input, const NCellMaster::TLoadContext& context)
    {
        ::Load(input, JobIds_);
    }

    void AddJob(const TJobId& id)
    {
        JobIds_.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        auto it = std::find(JobIds_.begin(), JobIds_.end(), id);
        if (it != JobIds_.end()) {
            JobIds_.erase(it);
        }
    }
    
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(yvector<TJobId>, JobIds);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
