#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosAgent
    : public TRefCounted
{
    // Start periodic replication card updates.
    virtual void Enable() = 0;

    // Stop replication card updates.
    virtual void Disable() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosAgent)

IChaosAgentPtr CreateChaosAgent(
    TTablet* tablet,
    const NChaosClient::TReplicationCardToken& replicationCardToken,
    NApi::NNative::IConnectionPtr localConnection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
