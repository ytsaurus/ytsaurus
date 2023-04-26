#include "reboot_manager.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TRebootManager::TRebootManager(IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
    , OrchidService_(CreateOrchidService())
{ }

void TRebootManager::RequestReboot()
{
    NeedReboot_ = true;
}

bool TRebootManager::IsRebootNeeded()
{
    return NeedReboot_;
}

void TRebootManager::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("need_reboot")
            .Value(IsRebootNeeded())
        .EndMap();
}

IYPathServicePtr TRebootManager::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND(&TRebootManager::BuildOrchid, MakeStrong(this)))
        ->Via(Invoker_);
}

IYPathServicePtr TRebootManager::GetOrchidService()
{
    return OrchidService_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
