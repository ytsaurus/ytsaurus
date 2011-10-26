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
        ::Save(output, (i32) Type); // temp. For some reason could not DECLARE_PODTYPE(EJobType)
        ::Save(output, JobId);
        ::Save(output, ChunkId);
        ::Save(output, RunnerAddress);
        ::Save(output, TargetAddresses);
    }

    static TAutoPtr<TJob> Load(TInputStream* input)
    {
        i32 type; // temp. For some reason could not DECLARE_PODTYPE(EJobType)
        TJobId jobId;
        TChunkId chunkId;
        Stroka runnerAddress;
        yvector<Stroka> targetAddresses;
        ::Load(input, type);
        ::Load(input, jobId);
        ::Load(input, chunkId);
        ::Load(input, runnerAddress);
        ::Load(input, targetAddresses);
        return new TJob(EJobType(type), jobId, chunkId, runnerAddress, targetAddresses);
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
