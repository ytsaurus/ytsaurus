#ifndef ACCESS_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include access_log.h"
// For the sake of sane code completion.
#include "access_log.h"
#endif

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/ytlib/election/cell_manager.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

inline bool IsAccessLogEnabled(NCellMaster::TBootstrap* bootstrap)
{
    if (!bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog) {
        return false;
    }

    if (bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery()) {
        return false;
    }

    if (!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader()) {
        return true;
    }

    if (bootstrap->GetCellManager()->GetTotalPeerCount() <= 1) {
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

}
