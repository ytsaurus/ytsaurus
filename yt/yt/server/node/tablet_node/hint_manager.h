#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHintManager
    : public virtual TRefCounted
{
    //! \note Thread affinity: ControlThread
    virtual void Start() = 0;

    //! \note Thread affinity: any
    virtual bool IsReplicaClusterBanned(TStringBuf clusterName) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHintManager)

IHintManagerPtr CreateHintManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
