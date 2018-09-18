#include "tablet_cell_map_type_handler.h"
#include "tablet_cell_map_proxy.h"

#include <yt/server/cypress_server/node_detail.h>

namespace NYT {
namespace NTabletServer {

using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TTabletCellMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::TabletCellMap;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateTabletCellMapProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateTabletCellMapTypeHandler(TBootstrap* bootstrap)
{
    return New<TTabletCellMapTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
