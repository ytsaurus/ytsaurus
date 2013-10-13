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

    template <class TTarget, class TRequest, class TResponse>
    NHydra::TMutationPtr CreateMutation(
        TTarget* target,
        const TRequest& request,
        TResponse (TTarget::* method)(const TRequest&),
        EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default);

    NHydra::TMutationPtr CreateMutation(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default);

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

#define META_STATE_FACADE_INL_H_
#include "meta_state_facade-inl.h"
#undef META_STATE_FACADE_INL_H_
