#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ISmoothMovementTrackerHost
    : public virtual TRefCounted
{
    virtual TTabletNodeDynamicConfigPtr GetDynamicConfig() const = 0;

    virtual const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    virtual TCellId GetCellId() const = 0;

    virtual void UpdateTabletSnapshot(
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch = std::nullopt) = 0;

    virtual NProto::TReqReplicateTabletContent PrepareReplicateTabletContentRequest(
        TTablet* tablet) = 0;

    virtual void PostMasterMessage(
        TTablet* tablet,
        const ::google::protobuf::MessageLite& message,
        bool forceCellMailbox = false) = 0;

    virtual void RegisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId,
        TCellId siblingCellId) = 0;

    virtual void UnregisterSiblingTabletAvenue(
        NHiveServer::TAvenueEndpointId siblingEndpointId) = 0;

    virtual void PostAvenueMessage(
        NHiveServer::TAvenueEndpointId endpointId,
        const ::google::protobuf::MessageLite& message) = 0;

    virtual void RegisterMasterAvenue(
        TTabletId tabletId,
        NHiveServer::TAvenueEndpointId masterEndpointId,
        NHiveServer::TPersistentMailboxState&& cookie) = 0;

    virtual NHiveServer::TPersistentMailboxState UnregisterMasterAvenue(
        NHiveServer::TAvenueEndpointId masterEndpointId) = 0;

    virtual TTablet* GetTabletOrThrow(TTabletId id) = 0;
    virtual TTablet* FindTablet(const TTabletId& id) const = 0;
    virtual TTablet* GetTablet(const TTabletId& id) const = 0;
    virtual const NHydra::TReadOnlyEntityMap<TTablet>& Tablets() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISmoothMovementTrackerHost)

////////////////////////////////////////////////////////////////////////////////

struct ISmoothMovementTracker
    : public virtual TRefCounted
{
    virtual void CheckTablet(TTablet* tablet) = 0;
    virtual void OnGotReplicatedContent(TTablet* tablet) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISmoothMovementTracker)

////////////////////////////////////////////////////////////////////////////////

ISmoothMovementTrackerPtr CreateSmoothMovementTracker(
    ISmoothMovementTrackerHostPtr host,
    NHydra::ISimpleHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
