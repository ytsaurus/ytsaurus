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

namespace {

EObjectType MapObjectTypeFromCellarType(ECellarType cellarType)
{
    switch (cellarType) {
        case ECellarType::Tablet:
            return EObjectType::TabletCellMap;
        case ECellarType::Chaos:
            return EObjectType::ChaosCellMap;
        default:
            YT_ABORT();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCellMapTypeHandler
    : public TCypressMapNodeTypeHandler
{
public:
    TCellMapTypeHandler(
        TBootstrap* bootstrap,
        ECellarType cellarType)
        : TCypressMapNodeTypeHandler(bootstrap)
        , CellarType_(cellarType)
    { }

    EObjectType GetObjectType() const override
    {
        return MapObjectTypeFromCellarType(CellarType_);
    }

private:
    const ECellarType CellarType_;

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

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateCellMapTypeHandler(
    TBootstrap* bootstrap,
    ECellarType cellarType)
{
    return New<TCellMapTypeHandler>(bootstrap, cellarType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
