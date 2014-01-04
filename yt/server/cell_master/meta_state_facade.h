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

    //! Checks if the cell is initialized.
    bool IsInitialized();

    //! Same as #IsInitialized but throws on failure.
    void ValidateInitialized();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
