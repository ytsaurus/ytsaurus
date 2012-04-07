#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJobList
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TJob*>, Jobs);

public:
    TJobList(const TChunkId& chunkId);

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);

    void AddJob(TJob* job);
    void RemoveJob(TJob* job);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
