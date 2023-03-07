#include "table_node_proxy.h"
#include "table_node_proxy_detail.h"

namespace NYT::NTableServer {

using namespace NObjectServer;
using namespace NCypressServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateReplicatedTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TReplicatedTableNode* trunkNode)
{
    return New<TReplicatedTableNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


