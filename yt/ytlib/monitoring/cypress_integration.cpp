#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPathServiceProducer::TPtr CreateMonitoringProducer(
    TMonitoringManager* monitoringManager)
{
    TMonitoringManager::TPtr monitoringManager_ = monitoringManager;
    return FromFunctor([=] ()
        {
            return IYPathService::FromNode(~monitoringManager_->GetRoot());
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
