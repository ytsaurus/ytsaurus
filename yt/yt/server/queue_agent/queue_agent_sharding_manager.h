#pragma once

#include "private.h"

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQueueAgentShardingManager
    : public TRefCounted
{
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void Start() = 0;

    virtual void OnDynamicConfigChanged(
        const TQueueAgentShardingManagerDynamicConfigPtr& oldConfig,
        const TQueueAgentShardingManagerDynamicConfigPtr& newConfig) = 0;

    virtual void PopulateAlerts(std::vector<NAlertManager::TAlert>* alerts) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueAgentShardingManager)

IQueueAgentShardingManagerPtr CreateQueueAgentShardingManager(
    IInvokerPtr controlInvoker,
    NQueueClient::TDynamicStatePtr dynamicState,
    NDiscoveryClient::IMemberClientPtr memberClient,
    NDiscoveryClient::IDiscoveryClientPtr discoveryClient,
    TString queueAgentStage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
