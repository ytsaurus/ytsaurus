#pragma once

#include "public.h"

#include <ytlib/meta_state/public.h>
#include <ytlib/rpc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EStateThreadQueue,
    (Default)
    (ChunkRefresh)
);

class TMetaStateFacade
    : public TRefCounted
{
public:
    TMetaStateFacade(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~TMetaStateFacade();

    NMetaState::TCompositeMetaStatePtr GetState() const;
    NMetaState::IMetaStateManagerPtr GetManager() const;
    IInvokerPtr GetInvoker(EStateThreadQueue queueIndex = EStateThreadQueue::Default) const;

    bool ValidateActiveLeaderStatus(NRpc::IServiceContextPtr context);
    bool ValidateActiveStatus(NRpc::IServiceContextPtr context);

    void Start();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

