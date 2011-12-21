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

    IMetaStateManager::TPtr MetaStateManager;

    TMetaStateServiceBase(
        IMetaStateManager* metaStateManager,
        const Stroka& serviceName,
        const Stroka& loggingCategory)
        : NRpc::TServiceBase(
            ~metaStateManager->GetStateInvoker(),
            serviceName,
            loggingCategory)
        , MetaStateManager(metaStateManager)
    {
        YASSERT(metaStateManager);
    }

    template <class TContext>
    IParamAction<TVoid>::TPtr CreateSuccessHandler(TContext* context)
    {
        TIntrusivePtr<TContext> context_ = context;
        return FromFunctor([=] (TVoid)
            {
                context_->Reply();
            });
    }

    template <class TContext>
    IAction::TPtr CreateErrorHandler(TContext* context)
    {
        TIntrusivePtr<TContext> context_ = context;
        return FromFunctor([=] ()
            {
                context_->Reply(
                    NRpc::EErrorCode::Unavailable,
                    "Error committing meta state changes");
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
