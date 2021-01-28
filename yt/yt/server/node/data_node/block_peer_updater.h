#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TBlockPeerUpdater
    : public TRefCounted
{
public:
    explicit TBlockPeerUpdater(NClusterNode::TBootstrap* bootstrap);

    void Start();
    void Stop();

    TDuration GetPeerUpdateExpirationTime() const;

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TDataNodeConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void Update();

};

DEFINE_REFCOUNTED_TYPE(TBlockPeerUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
