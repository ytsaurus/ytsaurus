#pragma once

#include "../actions/action.h"
#include "../rpc/service.h"
#include "../meta_state/meta_state_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    typedef TIntrusivePtr<TMetaStateServiceBase> TPtr;

    TMetaStateManager::TPtr MetaStateManager;

    TMetaStateServiceBase(
        TMetaStateManager* metaStateManager,
        Stroka serviceName,
        Stroka loggingCategory)
        : NRpc::TServiceBase(
            metaStateManager->GetStateInvoker(),
            serviceName,
            loggingCategory)
        , MetaStateManager(metaStateManager)
    {
        YASSERT(metaStateManager != NULL);
    }

    template <class TContext>
    IParamAction<TVoid>::TPtr CreateSuccessHandler(TIntrusivePtr<TContext> context)
    {
        return FromFunctor([=] (TVoid)
            {
                context->Reply();
            });
    }

    template <class TContext>
    IAction::TPtr CreateErrorHandler(TIntrusivePtr<TContext> context)
    {
        return FromFunctor([=] ()
            {
                context->Reply(NRpc::EErrorCode::Unavailable);
            });
    }

    void ValidateLeader()
    {
        if (MetaStateManager->GetStateStatus() != EPeerStatus::Leading) {
            ythrow NRpc::TServiceException(NRpc::EErrorCode::Unavailable) <<
                "Not a leader";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
