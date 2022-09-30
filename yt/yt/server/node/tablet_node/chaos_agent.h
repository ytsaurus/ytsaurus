#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/public.h>

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
    ITabletSlotPtr slot,
    NChaosClient::TReplicationCardId replicationCardId,
    NApi::NNative::IConnectionPtr localConnection);

////////////////////////////////////////////////////////////////////////////////

bool AdvanceTabletReplicationProgress(
    NApi::NNative::IConnectionPtr connection,
    const NLogging::TLogger& Logger,
    TTabletCellId tabletCellId,
    TTabletId tabletId,
    const NChaosClient::TReplicationProgress& progress,
    bool validateStrictAdvance = false,
    std::optional<ui64> replicationRound = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
