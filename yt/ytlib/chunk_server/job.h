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

    DECLARE_BYVAL_RO_PROPERTY(Type, EJobType);
    DECLARE_BYVAL_RO_PROPERTY(JobId, TJobId);
    DECLARE_BYVAL_RO_PROPERTY(ChunkId, NChunkClient::TChunkId);
    DECLARE_BYVAL_RO_PROPERTY(RunnerAddress, Stroka);
    DECLARE_BYREF_RO_PROPERTY(TargetAddresses, yvector<Stroka>);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
