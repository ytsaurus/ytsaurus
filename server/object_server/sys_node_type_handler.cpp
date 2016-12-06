#include "sys_node_type_handler.h"
#include "sys_node_proxy.h"

#include <yt/server/cypress_server/node_detail.h>

namespace NYT {
namespace NObjectServer {

using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TSysNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TSysNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::SysNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateSysNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateSysNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TSysNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
