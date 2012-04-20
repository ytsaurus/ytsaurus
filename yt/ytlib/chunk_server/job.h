#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): consider making it a full-fledged object
class TJob
{
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    DEFINE_BYVAL_RO_PROPERTY(TJobId, Id);
    // Don't try making it TChunk*.
    // Removal jobs may refer nonexistent chunks.
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, RunnerAddress);
    DEFINE_BYREF_RO_PROPERTY(yvector<Stroka>, TargetAddresses);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

public:
    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        const Stroka& runnerAddress,
        const yvector<Stroka>& targetAddresses,
        TInstant startTime);

    explicit TJob(const TJobId& jobId);

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
