#include "stdafx.h"
#include "meta_state_service.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TMetaStateServiceBase::TMetaStateServiceBase(
    IMetaStateManager* metaStateManager,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : NRpc::TServiceBase(
        ~metaStateManager->GetStateInvoker(),
        serviceName,
        loggingCategory)
    , MetaStateManager(metaStateManager)
{ }

void TMetaStateServiceBase::InvokeHandler(
    TRuntimeMethodInfo* runtimeInfo,
    IAction::TPtr handler,
    NRpc::IServiceContext* context)
{
    if (MetaStateManager->GetStateStatusAsync() != EPeerStatus::Leading) {
        context->Reply(TError(NRpc::EErrorCode::Unavailable, "Not an active leader"));
        return;
    }

    NRpc::IServiceContext::TPtr context_ = context;
    runtimeInfo->Invoker->Invoke(FromFunctor([=] ()
        {
            if (MetaStateManager->GetStateStatusAsync() != EPeerStatus::Leading ||
                !MetaStateManager->HasActiveQuorum())
            {
                context_->Reply(TError(NRpc::EErrorCode::Unavailable, "Not an active leader"));
                return;
            }

            handler->Do();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
