#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHintManager
    : public NNodeTrackerClient::INodeStatusDirectory
{
    //! \note Thread affinity: ControlThread
    virtual void Start() = 0;

    //! \note Thread affinity: any
    virtual bool IsReplicaClusterBanned(TStringBuf clusterName) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHintManager)

IHintManagerPtr CreateHintManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
