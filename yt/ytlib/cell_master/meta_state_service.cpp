#include "stdafx.h"
#include "meta_state_service.h"
#include "bootstrap.h"
#include "world_initializer.h"

namespace NYT {
namespace NCellMaster {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TMetaStateServiceBase::TMetaStateServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : NRpc::TServiceBase(
        bootstrap->GetStateInvoker(),
        serviceName,
        loggingCategory)
    , Bootstrap(bootstrap)
{
    YASSERT(bootstrap);
}

void TMetaStateServiceBase::InvokeHandler(
    TActiveRequestPtr activeRequest,
    const TClosure& handler)
{
    if (Bootstrap->GetMetaStateManager()->GetStateStatusAsync() != EPeerStatus::Leading) {
        activeRequest->Context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Not an active leader"));
        return;
    }

    activeRequest->RuntimeInfo->Invoker->Invoke(BIND([=] () {
        if (Bootstrap->GetMetaStateManager()->GetStateStatus() != EPeerStatus::Leading ||
            !Bootstrap->GetMetaStateManager()->HasActiveQuorum())
        {
            activeRequest->Context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active leader"));
            return;
        }

        if (!Bootstrap->GetWorldInitializer()->IsInitialized()) {
            activeRequest->Context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Cell is not initialized yet, please try again later"));
            return;
        }

        handler.Run();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
