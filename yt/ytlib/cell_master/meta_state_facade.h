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

    IInvokerPtr GetRawInvoker() const;

    IInvokerPtr GetWrappedInvoker(EStateThreadQueue queue = EStateThreadQueue::Default) const;

    bool ValidateActiveLeader(NRpc::IServiceContextPtr context);

    template <class TTarget, class TRequest, class TResponse>
    NMetaState::TMutationPtr CreateMutation(
        TTarget* target,
        const TRequest& request,
        TResponse (TTarget::* method)(const TRequest& request),
        EStateThreadQueue queue = EStateThreadQueue::Default)
    {
        return NMetaState::CreateMutation<TTarget, TRequest, TResponse>(
            GetManager(),
            GetWrappedInvoker(queue),
            target,
            request,
            method);
    }

    NMetaState::TMutationPtr CreateMutation(EStateThreadQueue queue = EStateThreadQueue::Default)
    {
        return New<NMetaState::TMutation>(
            GetManager(),
            GetWrappedInvoker(queue));
    }

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

