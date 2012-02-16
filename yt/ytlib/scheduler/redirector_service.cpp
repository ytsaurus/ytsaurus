#include "stdafx.h"
#include "redirector_service.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NScheduler {

using namespace NCypress;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("SchedulerRedirector");

////////////////////////////////////////////////////////////////////////////////

TRedirectorService::TRedirectorService(TCypressManager* cypressManager)
    // TODO(babenko): replace literal with TFooServiceProxy::GetServiceName()
    : TRedirectorServiceBase("Scheduler", Logger.GetCategory())
    , CypressManager(cypressManager)
{ }

TRedirectorService::TAsyncRedirectResult TRedirectorService::HandleRedirect(IServiceContext* context)
{
    return 
        FromMethod(&TRedirectorService::DoHandleRedirect, TPtr(this))
        ->AsyncVia(CypressManager->GetMetaStateManager()->GetStateInvoker())
        ->Do(context);
}

TRedirectorService::TRedirectResult TRedirectorService::DoHandleRedirect(IServiceContext::TPtr context)
{
    if (CypressManager->GetMetaStateManager()->GetStateStatus() != NMetaState::EPeerStatus::Leading) {
        return TError("Not a leader");
    }

    auto root = CypressManager->GetVersionedNodeProxy(CypressManager->GetRootNodeId(), NullTransactionId);

    TRedirectParams redirectParams;
    try {
        redirectParams.Address = SyncYPathGet(~root, "sys/scheduler@address");
    } catch (const std::exception& ex) {
        return TError(Sprintf("Error reading redirection parameters\n%s", ex.what()));
    }

    return redirectParams;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
