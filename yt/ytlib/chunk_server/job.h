#pragma once

#include "id.h"

#include <yt/ytlib/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): consider making it a full-fledged object
struct TJob
{
    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        const Stroka& runnerAddress,
        const yvector<Stroka>& targetAddresses,
        TInstant startTime)
        : Type_(type)
        , JobId_(jobId)
        , ChunkId_(chunkId)
        , RunnerAddress_(runnerAddress)
        , TargetAddresses_(targetAddresses)
        , StartTime_(startTime)
    { }

    TJob(const TJob& other)
        : Type_(other.Type_)
        , JobId_(other.JobId_)
        , ChunkId_(other.ChunkId_)
        , RunnerAddress_(other.RunnerAddress_)
        , TargetAddresses_(other.TargetAddresses_)
        , StartTime_(other.StartTime_)
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
        ::Save(output, StartTime_);
    }

    static TAutoPtr<TJob> Load(const TJobId& jobId, TInputStream* input)
    {
        EJobType type;
        TChunkId chunkId;
        Stroka runnerAddress;
        yvector<Stroka> targetAddresses;
        TInstant startTime;
        ::Load(input, type);
        ::Load(input, chunkId);
        ::Load(input, runnerAddress);
        ::Load(input, targetAddresses);
        ::Load(input, startTime);
        return new TJob(
            type,
            jobId, 
            chunkId, 
            runnerAddress, 
            targetAddresses, 
            startTime);
    }

    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, RunnerAddress);
    DEFINE_BYREF_RO_PROPERTY(yvector<Stroka>, TargetAddresses);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
