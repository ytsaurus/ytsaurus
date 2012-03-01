#pragma once

#include <ytlib/actions/action.h>
#include <ytlib/rpc/service.h>
#include <ytlib/meta_state/meta_state_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Base class for all services that require an active leader and the state thread context
//! for handling requests.
/*!
 *  This class ensures that the handlers get called in a proper thread while the peer is
 *  an active leader by making a double check.
 *  
 *  The first check calls #IMetaStateManager::GetStateStatusAsync
 *  to see if it returns #EPeerStatus::Leading.
 *  
 *  If so, the action is propagated into the state thread. The second check involves calling 
 *  #IMetaStateManager::GetStateStatus and #IMetaStateManager::HasActiveQuorum
 *  right before executing the action.
 */
class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    typedef TIntrusivePtr<TMetaStateServiceBase> TPtr;

    IMetaStateManager::TPtr MetaStateManager;

    TMetaStateServiceBase(
        IMetaStateManager* metaStateManager,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

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

private:
    virtual void InvokeHandler(
        TRuntimeMethodInfo* runtimeInfo,
        IAction::TPtr handler,
        NRpc::IServiceContext* context);

    void CheckStatus(EPeerStatus status);
    void CheckQuorum();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
