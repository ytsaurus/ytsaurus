#include "stdafx.h"
#include "ytree_integration.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/ytree/ytree.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPathServiceProvider CreateMonitoringProvider(
    TMonitoringManager* monitoringManager)
{
    TMonitoringManager::TPtr monitoringManager_ = monitoringManager;
    return FromFunctor([=] () -> IYPathServicePtr
        {
            return monitoringManager_->GetRoot();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
