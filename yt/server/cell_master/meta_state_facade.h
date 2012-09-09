#pragma once

#include "public.h"

#include <ytlib/rpc/public.h>

#include <ytlib/meta_state/mutation.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EStateThreadQueue,
    (Default)
    (ChunkRefresh)
    (Heartbeat)
);

class TMetaStateFacade
    : public TRefCounted
{
public:
    TMetaStateFacade(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~TMetaStateFacade();

    void Start();

    NMetaState::TCompositeMetaStatePtr GetState() const;
    NMetaState::IMetaStateManagerPtr GetManager() const;

    bool IsInitialized() const;

    IInvokerPtr GetUnguardedInvoker(EStateThreadQueue queue = EStateThreadQueue::Default) const;
    IInvokerPtr GetUnguardedEpochInvoker(EStateThreadQueue queue = EStateThreadQueue::Default) const;

    IInvokerPtr GetGuardedInvoker(EStateThreadQueue queue = EStateThreadQueue::Default) const;
    IInvokerPtr GetGuardedEpochInvoker(EStateThreadQueue queue = EStateThreadQueue::Default) const;

    template <class TTarget, class TRequest, class TResponse>
    NMetaState::TMutationPtr CreateMutation(
        TTarget* target,
        const TRequest& request,
        TResponse (TTarget::* method)(const TRequest&),
        EStateThreadQueue queue = EStateThreadQueue::Default);

    NMetaState::TMutationPtr CreateMutation(EStateThreadQueue queue = EStateThreadQueue::Default);

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
