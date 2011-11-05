#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TJob
{
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

    TAutoPtr<TJob> Clone()
    {
        return new TJob(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Type);
        ::Save(output, ChunkId);
        ::Save(output, RunnerAddress);
        ::Save(output, TargetAddresses);
    }

    static TAutoPtr<TJob> Load(const TJobId& jobId, TInputStream* input)
    {
        EJobType type;
        TChunkId chunkId;
        Stroka runnerAddress;
        yvector<Stroka> targetAddresses;
        ::Load(input, type);
        ::Load(input, chunkId);
        ::Load(input, runnerAddress);
        ::Load(input, targetAddresses);
        return new TJob(type, jobId, chunkId, runnerAddress, targetAddresses);
    }

    EJobType Type;
    TJobId JobId;
    TChunkId ChunkId;
    Stroka RunnerAddress;
    yvector<Stroka> TargetAddresses;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
