#pragma once

#include "node_tracker.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeListForRole
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNodeRawPtr>, Nodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::string>, Addresses);

public:
    void UpdateAddresses();
    void Clear();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeDiscoveryManager
    : public TRefCounted
{
public:
    TNodeDiscoveryManager(NCellMaster::TBootstrap* bootstrap, NNodeTrackerClient::ENodeRole nodeRole);
    void Reconfigure(TNodeDiscoveryManagerConfigPtr config);

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NNodeTrackerClient::ENodeRole NodeRole_;
    const NLogging::TLogger Logger;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    TNodeDiscoveryManagerConfigPtr Config_;

    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);

    void OnLeaderActive();
    void OnStopLeading();

    bool IsGoodNode(const TNode* node) const;
    void UpdateNodeList();
    void CommitNewNodes(const THashSet<TNode*>& nodes);
};

DEFINE_REFCOUNTED_TYPE(TNodeDiscoveryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
