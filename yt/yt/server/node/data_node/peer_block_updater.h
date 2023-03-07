#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    TPeerBlockUpdater(
        TDataNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);

    void Start();
    void Stop();

    TDuration GetPeerUpdateExpirationTime() const;

private:
    const TDataNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void Update();

};

DEFINE_REFCOUNTED_TYPE(TPeerBlockUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
