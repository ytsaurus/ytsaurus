#pragma once

#include "common.h"
#include "monitoring_manager.h"

#include "../cypress/node.h"
#include "../cypress/cypress_manager.h"

#include "../ytree/ypath_service.h"

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPathServiceProducer::TPtr CreateMonitoringProducer(
    TMonitoringManager* monitoringManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
