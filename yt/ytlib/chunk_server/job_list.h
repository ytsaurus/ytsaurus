#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TJobList
{
    TJobList(const TChunkId& chunkId)
        : ChunkId(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId(other.ChunkId)
        , Jobs(other.Jobs)
    { }

    TAutoPtr<TJobList> Clone() const
    {
        return new TJobList(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Jobs);
    }

    static TAutoPtr<TJobList> Load(const TChunkId& chunkId, TInputStream* input)
    {
        TAutoPtr<TJobList> jobList = new TJobList(chunkId);
        ::Load(input, jobList->Jobs);
        return jobList;
    }

    void AddJob(const TJobId& id)
    {
        Jobs.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        auto it = Find(Jobs.begin(), Jobs.end(), id);
        if (it != Jobs.end()) {
            Jobs.erase(it);
        }
    }
    
    TChunkId ChunkId;
    yvector<TJobId> Jobs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
