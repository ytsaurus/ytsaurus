#pragma once

#include "public.h"

#include <ytlib/actions/callback_forward.h>
#include <ytlib/rpc/service.h>
#include <ytlib/meta_state/meta_state_manager.h>

namespace NYT {
namespace NCellMaster {

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
 *  #IMetaStateManager::GetStateStatus,
 *  #IMetaStateManager::HasActiveQuorum, and
 *  #TWorldInitializer::IsInitialized
 *  right before executing the action.
 */
class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    TBootstrap* Bootstrap;

    TMetaStateServiceBase(
        TBootstrap* bootstrap,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    template <class TContext>
    TClosure CreateErrorHandler(TContext* context)
    {
        return BIND(
            (void (TContext::*)(int, const Stroka&))&TContext::Reply,
            MakeStrong(context),
            NRpc::EErrorCode::Unavailable,
            "Error committing meta state changes");
    }

private:
    virtual void InvokeHandler(
        TRuntimeMethodInfo* runtimeInfo,
        const TClosure& handler,
        NRpc::IServiceContextPtr context);

    void CheckStatus(NMetaState::EPeerStatus status);
    void CheckQuorum();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
