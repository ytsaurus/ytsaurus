#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandler::TPtr CreateMonitoringTypeHandler(
    TCypressManager* cypressManager,
    TMonitoringManager* monitoringManager)
{
    TMonitoringManager::TPtr monitoringManager_ = monitoringManager;
    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::Monitoring,
        // TODO: extract constant
        "monitoring",
        ~FromFunctor([=] (const TVirtualYPathContext& context) -> IYPathService::TPtr
            {
                UNUSED(context);
                return IYPathService::FromNode(~monitoringManager_->GetRoot());
            }));
}

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
