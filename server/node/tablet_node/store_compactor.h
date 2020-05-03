#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TStoreCompactorPtr CreateStoreCompactor(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

void StartStoreCompactor(
    TStoreCompactorPtr storeCompactor);

NYTree::IYPathServicePtr GetOrchidService(TStoreCompactorPtr storeCompactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
