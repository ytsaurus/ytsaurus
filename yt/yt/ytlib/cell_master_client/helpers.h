#pragma once

#include "public.h"

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

bool ClusterMasterCompositionChanged(
    const TSecondaryMasterConnectionConfigs& oldSecondaryMasterConnectionConfigs,
    const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs);

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TCellTagList GetMasterCellTags(const TSecondaryMasterConnectionConfigs& masterConnectionConfigs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
