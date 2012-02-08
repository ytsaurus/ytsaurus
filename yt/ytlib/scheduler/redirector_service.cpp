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

struct TRedirectorService::TConfig
    : public TConfigurable
{
    Stroka Address;
    TDuration Timeout;

    TConfig()
    {
        Register("address", Address);
        Register("timeout", Timeout)
            .Default(TDuration::MilliSeconds(5000));
    }
};

TRedirectorService::TRedirectParams TRedirectorService::GetRedirectParams(IServiceContext* context) const
{
    // TODO(babenko): refactor using new API
    auto root = CypressManager->GetVersionedNodeProxy(CypressManager->GetRootNodeId(), NullTransactionId);
    auto configYson = SyncYPathGet(~root, "sys/scheduler@");
    auto configNode = DeserializeFromYson(configYson);
    auto config = New<TConfig>();
    config->LoadAndValidate(~configNode);

    TRedirectParams redirectParams;
    redirectParams.Address = config->Address;
    redirectParams.Timeout = config->Timeout;
    return redirectParams;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
