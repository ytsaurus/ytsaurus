#pragma once

#include "id.h"

#include <ytlib/misc/property.h>

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
        TInstant startTime);

    TJob(const TJob& other);

    TAutoPtr<TJob> Clone();

    void Save(TOutputStream* output) const;
    static TAutoPtr<TJob> Load(const TJobId& jobId, TInputStream* input);

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
