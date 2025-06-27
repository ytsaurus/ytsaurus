#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/ytlib/chaos_client/public.h>

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

    virtual void ReconfigureTablet() = 0;
    virtual void RefreshEra(NChaosClient::TReplicationEra newEra) = 0;
    virtual NConcurrency::TAsyncSemaphoreGuard TryGetConfigLockGuard() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosAgent)

IChaosAgentPtr CreateChaosAgent(
    TTablet* tablet,
    ITabletSlotPtr slot,
    NChaosClient::TReplicationCardId replicationCardId,
    NApi::NNative::IClientPtr localClient,
    NChaosClient::IReplicationCardUpdatesBatcherPtr replicationCardUpdatesBatcher
);

////////////////////////////////////////////////////////////////////////////////

bool AdvanceTabletReplicationProgress(
    const NApi::NNative::IClientPtr& localClient,
    const NLogging::TLogger& Logger,
    TTabletCellId tabletCellId,
    NApi::TClusterTag clockClusterTag,
    TTabletId tabletId,
    const NChaosClient::TReplicationProgress& progress,
    bool validateStrictAdvance = false,
    std::optional<ui64> replicationRound = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
