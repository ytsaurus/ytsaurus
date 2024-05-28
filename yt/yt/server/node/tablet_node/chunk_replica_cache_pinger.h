#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IChunkReplicaCachePinger
    : public virtual TRefCounted
{
    virtual ~IChunkReplicaCachePinger() = default;

    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaCachePinger)

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaCachePingerPtr CreateChunkReplicaCachePinger(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
