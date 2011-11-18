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
        const NChunkClient::TChunkId& chunkId,
        Stroka runnerAddress,
        const yvector<Stroka>& targetAddresses)
        : Type_(type)
        , JobId_(jobId)
        , ChunkId_(chunkId)
        , RunnerAddress_(runnerAddress)
        , TargetAddresses_(targetAddresses)
    { }

    TJob(const TJob& other)
        : Type_(other.Type_)
        , JobId_(other.JobId_)
        , ChunkId_(other.ChunkId_)
        , RunnerAddress_(other.RunnerAddress_)
        , TargetAddresses_(other.TargetAddresses_)
    { }

    TAutoPtr<TJob> Clone()
    {
        return new TJob(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Type_);
        ::Save(output, ChunkId_);
        ::Save(output, RunnerAddress_);
        ::Save(output, TargetAddresses_);
    }

    static TAutoPtr<TJob> Load(const TJobId& jobId, TInputStream* input)
    {
        EJobType type;
        NChunkClient::TChunkId chunkId;
        Stroka runnerAddress;
        yvector<Stroka> targetAddresses;
        ::Load(input, type);
        ::Load(input, chunkId);
        ::Load(input, runnerAddress);
        ::Load(input, targetAddresses);
        return new TJob(type, jobId, chunkId, runnerAddress, targetAddresses);
    }

    DECLARE_BYVAL_RO_PROPERTY(EJobType, Type);
    DECLARE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DECLARE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, ChunkId);
    DECLARE_BYVAL_RO_PROPERTY(Stroka, RunnerAddress);
    DECLARE_BYREF_RO_PROPERTY(yvector<Stroka>, TargetAddresses);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
