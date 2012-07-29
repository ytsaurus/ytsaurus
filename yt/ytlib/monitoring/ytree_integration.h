#pragma once

#include "common.h"
#include "monitoring_manager.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NYTree::TYPathServiceProducer CreateMonitoringProducer(
    TMonitoringManager::TPtr monitoringManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
