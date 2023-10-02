#include "cell_map_type_handler.h"

#include "cell_map_proxy.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarClient;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TCellMapTypeHandler
    : public TCypressMapNodeTypeHandler
{
public:
    TCellMapTypeHandler(
        TBootstrap* bootstrap,
        ECellarType cellarType,
        EObjectType cellMapType)
        : TCypressMapNodeTypeHandler(bootstrap)
        , CellarType_(cellarType)
        , CellMapType_(cellMapType)
    { }

    EObjectType GetObjectType() const override
    {
        return CellMapType_;
    }

private:
    const ECellarType CellarType_;
    const EObjectType CellMapType_;

    ICypressNodeProxyPtr DoGetProxy(
        TCypressMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateCellMapProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode,
            CellarType_);
    }
};

INodeTypeHandlerPtr CreateCellMapTypeHandler(
    TBootstrap* bootstrap,
    ECellarType cellarType,
    EObjectType cellMapType)
{
    return New<TCellMapTypeHandler>(bootstrap, cellarType, cellMapType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
