#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <server/cell_master/public.h>
#include <server/cell_master/serialization_context.h>

#include <server/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    // Don't try making it TChunk*.
    // Removal jobs may refer to nonexistent chunks.
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, TargetAddresses);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

public:
    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        const Stroka& address,
        const std::vector<Stroka>& targetAddresses,
        TInstant startTime);

    explicit TJob(const TJobId& jobId);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
