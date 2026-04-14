#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHintManager
    : public TRefCounted
{
    //! \note Thread affinity: ControlThread
    virtual void Start() = 0;

    //! \note Thread affinity: any
    virtual bool IsReplicaClusterBanned(TStringBuf clusterName) const = 0;

    //! \note Thread affinity: any
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHintManager)

IHintManagerPtr CreateHintManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
