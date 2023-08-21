#pragma once

#include "public.h"
#include "job_controller.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAutotomyJob)

////////////////////////////////////////////////////////////////////////////////

struct IChunkAutotomizer
    : public ITypedJobController<TAutotomyJob>
{
    virtual void Initialize() = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) const = 0;

    virtual bool IsChunkRegistered(TChunkId bodyChunkId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkAutotomizer)

////////////////////////////////////////////////////////////////////////////////

IChunkAutotomizerPtr CreateChunkAutotomizer(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
