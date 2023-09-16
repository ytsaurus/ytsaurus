#include "restart_manager.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TRestartManager::TRestartManager(IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
    , OrchidService_(CreateOrchidService())
{ }

void TRestartManager::RequestRestart()
{
    NeedRestart_.store(true);
}

bool TRestartManager::IsRestartNeeded()
{
    return NeedRestart_.load();
}

void TRestartManager::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("need_restart")
            .Value(IsRestartNeeded())
        .EndMap();
}

IYPathServicePtr TRestartManager::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND(&TRestartManager::BuildOrchid, MakeStrong(this)))
        ->Via(Invoker_);
}

IYPathServicePtr TRestartManager::GetOrchidService()
{
    return OrchidService_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
