#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TJobList
{
    TJobList(const NChunkClient::TChunkId& chunkId)
        : ChunkId_(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId_(other.ChunkId_)
        , JobIds_(other.JobIds_)
    { }

    TAutoPtr<TJobList> Clone() const
    {
        return new TJobList(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, JobIds_);
    }

    static TAutoPtr<TJobList> Load(const NChunkClient::TChunkId& chunkId, TInputStream* input)
    {
        TAutoPtr<TJobList> jobList = new TJobList(chunkId);
        ::Load(input, jobList->JobIds_);
        return jobList;
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
    
    DECLARE_BYVAL_RO_PROPERTY(ChunkId, NChunkClient::TChunkId);
    DECLARE_BYREF_RO_PROPERTY(JobIds, yvector<TJobId>);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
