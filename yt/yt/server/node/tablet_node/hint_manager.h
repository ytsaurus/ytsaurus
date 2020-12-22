#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class THintManager
    : public TRefCounted
{
public:
    THintManager(
        THintManagerConfigPtr config,
        const NClusterNode::TBootstrap* bootstrap);
    ~THintManager();

    //! \note Thread affinity: ControlThread
    void Start();

    bool IsReplicaClusterBanned(TStringBuf clusterName) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THintManager)

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
