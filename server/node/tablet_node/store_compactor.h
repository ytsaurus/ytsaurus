#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TStoreCompactorPtr CreateStoreCompactor(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

void StartStoreCompactor(
    TStoreCompactorPtr storeCompactor);

NYTree::IYPathServicePtr GetOrchidService(TStoreCompactorPtr storeCompactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
