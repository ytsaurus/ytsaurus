#include "stdafx.h"
#include "ytree_integration.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/virtual.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYPathServiceProducer CreateMonitoringProducer(
    TMonitoringManager* monitoringManager)
{
    auto monitoringManager_ = MakeStrong(monitoringManager);
    return FromFunctor([=] () -> IYPathServicePtr
        {
            return ~monitoringManager_->GetRoot();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
