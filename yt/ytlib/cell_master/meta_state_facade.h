#pragma once

#include "public.h"

#include <ytlib/meta_state/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

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

    void ValidateLeaderStatus();
    void ValidateFollowerStatus();

    void Start();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

