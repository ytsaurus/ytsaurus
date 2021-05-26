#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Chaos Hydra.
struct IChaosSlot
    : public NCellarAgent::ICellarOccupier
{
    static constexpr NCellarClient::ECellarType CellarType = NCellarClient::ECellarType::Chaos;

    virtual NHydra::TCellId GetCellId() const = 0;
    virtual NHydra::EPeerState GetAutomatonState() const = 0;

    virtual NHydra::IDistributedHydraManagerPtr GetHydraManager() const = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() const = 0;
    virtual ITransactionManagerPtr GetTransactionManager() const = 0;

    virtual const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() const = 0;

    // These methods are thread-safe.
    // They may return null invoker (see #GetNullInvoker) if the invoker of the requested type is not available.
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const = 0;

    virtual const NHiveServer::THiveManagerPtr& GetHiveManager() const = 0;
    virtual NHiveServer::TMailbox* GetMasterMailbox() = 0;

    virtual const IChaosManagerPtr& GetChaosManager() const = 0;

    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosSlot)

IChaosSlotPtr CreateChaosSlot(
    int slotIndex,
    TChaosNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
