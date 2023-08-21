#include "rootstock_map_type_handler.h"

#include "grafting_manager.h"
#include "rootstock_node.h"
#include "virtual.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualRootstockMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& graftingManager = Bootstrap_->GetGraftingManager();
        return MakeFuture(ToObjectIds(GetValues(graftingManager->RootstockNodes(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::Rootstock;
    }

    TFuture<i64> GetSize() const override
    {
        const auto& graftingManager = Bootstrap_->GetGraftingManager();
        return MakeFuture<i64>(graftingManager->RootstockNodes().size());
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/rootstocks";
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateRootstockMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::RootstockMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualRootstockMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
