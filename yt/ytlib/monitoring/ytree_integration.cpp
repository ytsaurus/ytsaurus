#include "stdafx.h"
#include "ytree_integration.h"

#include <ytlib/actions/bind.h>
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
    return BIND([=] () -> IYPathServicePtr {
    	return ~monitoringManager_->GetRoot();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
