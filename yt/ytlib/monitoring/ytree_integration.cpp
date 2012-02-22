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
	// TODO(babenko): use AsStrong
    TMonitoringManager::TPtr monitoringManager_ = monitoringManager;
    return FromFunctor([=] () -> IYPathServicePtr
        {
            return ~monitoringManager_->GetRoot();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
