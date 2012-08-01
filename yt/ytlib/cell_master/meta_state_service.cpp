#include "stdafx.h"
#include "meta_state_service.h"
#include "bootstrap.h"
#include "meta_state_facade.h"

namespace NYT {
namespace NCellMaster {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TMetaStateServiceBase::TMetaStateServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : NRpc::TServiceBase(
        bootstrap->GetMetaStateFacade()->GetInvoker(),
        serviceName,
        loggingCategory)
    , Bootstrap(bootstrap)
{
    YASSERT(bootstrap);
}

//void TMetaStateServiceBase::InvokeHandler(
//    TRuntimeMethodInfo* runtimeInfo,
//    const TClosure& handler,
//    NRpc::IServiceContextPtr context)
//{
//    if (Bootstrap->GetMetaStateManager()->GetStateStatusAsync() != EPeerStatus::Leading) {
//        context->Reply(TError(NRpc::EErrorCode::Unavailable, "Not an active leader"));
//        return;
//    }
//
//    NRpc::IServiceContextPtr context_ = context;
//    runtimeInfo->Invoker->Invoke(BIND([=] ()
//        {
//            if (Bootstrap->GetMetaStateManager()->GetStateStatusAsync() != EPeerStatus::Leading ||
//                !Bootstrap->GetMetaStateManager()->HasActiveQuorum())
//            {
//                context_->Reply(TError(NRpc::EErrorCode::Unavailable, "Not an active leader"));
//                return;
//            }
//
//            if (!Bootstrap->GetWorldInitializer()->IsInitialized()) {
//                context_->Reply(TError(NRpc::EErrorCode::Unavailable, "Cell is not initialized yet, please try again later"));
//                return;
//            }
//
//            handler.Run();
//        }));
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
