#include "sys_node_type_handler.h"
#include "sys_node_proxy.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

namespace NYT::NObjectServer {

using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TSysNodeTypeHandler
    : public TCypressMapNodeTypeHandler
{
public:
    using TCypressMapNodeTypeHandler::TCypressMapNodeTypeHandler;

    EObjectType GetObjectType() const override
    {
        return EObjectType::SysNode;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TCypressMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateSysNodeProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateSysNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TSysNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
