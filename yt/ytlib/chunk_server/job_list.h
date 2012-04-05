#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move impl to cpp
class TJobList
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJob*>, Jobs);
public:

    TJobList(const TChunkId& chunkId)
        : ChunkId_(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId_(other.ChunkId_)
        , Jobs_(other.Jobs_)
    { }

    void Save(TOutputStream* output) const
    {
//        ::Save(output, Jobs_);
    }

    void Load(const NCellMaster::TLoadContext& context, TInputStream* input)
    {
//        ::Load(input, Jobs_);
    }

    void AddJob(TJob* job)
    {
        Jobs_.push_back(job);
    }

    void RemoveJob(TJob* job)
    {
        auto it = std::find(Jobs_.begin(), Jobs_.end(), job);
        if (it != Jobs_.end()) {
            Jobs_.erase(it);
        }
    }
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
