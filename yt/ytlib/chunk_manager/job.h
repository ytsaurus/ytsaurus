#pragma once

#include "common.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

struct TJob
{
    TJob()
    { }

    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        Stroka runnerAddress,
        const yvector<Stroka>& targetAddresses)
        : Type(type)
        , JobId(jobId)
        , ChunkId(chunkId)
        , RunnerAddress(runnerAddress)
        , TargetAddresses(targetAddresses)
    { }

    TJob(const TJob& other)
        : Type(other.Type)
        , JobId(other.JobId)
        , ChunkId(other.ChunkId)
        , RunnerAddress(other.RunnerAddress)
        , TargetAddresses(other.TargetAddresses)
    { }

    TJob& operator = (const TJob& other)
    {
        // TODO: implement
        UNUSED(other);
        YASSERT(false);
        return *this;
    }

    EJobType Type;
    TJobId JobId;
    TChunkId ChunkId;
    Stroka RunnerAddress;
    yvector<Stroka> TargetAddresses;

};

////////////////////////////////////////////////////////////////////////////////

struct TJobList
{
    typedef yvector<TJobId> TJobs;

    TJobList()
    { }

    TJobList(const TChunkId& chunkId)
        : ChunkId(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId(other.ChunkId)
        , Jobs(other.Jobs)
    { }

    TJobList& operator = (const TJobList& other)
    {
        // TODO: implement
        UNUSED(other);
        YASSERT(false);
        return *this;
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
    TJobs Jobs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
