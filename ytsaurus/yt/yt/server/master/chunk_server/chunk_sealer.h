#pragma once

#include "private.h"
#include "job_controller.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkSealer
    : public IJobController
{
public:
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual bool IsEnabled() = 0;

    virtual void ScheduleSeal(TChunk* chunk) = 0;

    virtual void OnChunkDestroyed(NChunkServer::TChunk* chunk) = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkSealer)

////////////////////////////////////////////////////////////////////////////////

IChunkSealerPtr CreateChunkSealer(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
