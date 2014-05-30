#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <server/hydra/mutation.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EAutomatonThreadQueue,
    (Default)
    (ChunkMaintenance)
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

class TMetaStateFacade
    : public TRefCounted
{
public:
    TMetaStateFacade(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~TMetaStateFacade();

    void Start();

    TMasterAutomatonPtr GetAutomaton() const;
    NHydra::IHydraManagerPtr GetManager() const;

    IInvokerPtr GetInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    //! Same as #IsActiveLeader but throws on failure.
    void ValidateActiveLeader();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

DEFINE_REFCOUNTED_TYPE(TMetaStateFacade)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
