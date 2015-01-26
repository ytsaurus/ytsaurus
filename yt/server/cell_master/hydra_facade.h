#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <server/hydra/mutation.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (ChunkMaintenance)
    (ChunkLocator)
    (ChunkTraverser)
    (Heartbeat)
);

class TNotALeaderException
    : public TErrorException
{
public:
    TNotALeaderException()
    { }

};

////////////////////////////////////////////////////////////////////////////////

class THydraFacade
    : public TRefCounted
{
public:
    THydraFacade(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~THydraFacade();

    void Start();

    TMasterAutomatonPtr GetAutomaton() const;
    NHydra::IHydraManagerPtr GetHydraManager() const;
    NRpc::IResponseKeeperPtr GetResponseKeeper() const;

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    //! Throws TNotALeaderException if the peer is an active leader.
    void ValidateActiveLeader();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(THydraFacade)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
