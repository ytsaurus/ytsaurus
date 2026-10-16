#pragma once

#include "bootstrap.h"
#include "config.h"
#include "config_manager.h"
#include "gossip_value.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
void InitializeGossipValue(TGossipValue<TValue>* gossipValue, TBootstrap* bootstrap)
{
    gossipValue->Initialize(
        bootstrap->GetCellTag(),
        bootstrap->GetPrimaryCellTag(),
        bootstrap->GetSecondaryCellTags(),
        bootstrap->GetConfigManager()->GetConfig()->MulticellManager->Testing->AllowMasterCellRemoval);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
