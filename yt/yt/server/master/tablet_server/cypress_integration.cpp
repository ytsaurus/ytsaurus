#include "cypress_integration.h"

#include "tablet.h"
#include "tablet_action.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

namespace NYT::NTabletServer {

using namespace NYPath;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TVirtualTabletMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return MakeFuture(ToObjectIds(GetValues(tabletManager->Tablets(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return IsTabletType(object->GetType());
    }

    TFuture<i64> GetSize() const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return MakeFuture<i64>(tabletManager->Tablets().GetSize());
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

protected:
    TYPath GetWellKnownPath() const override
    {
        return "//sys/tablets";
    }
};

INodeTypeHandlerPtr CreateTabletMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTabletMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTabletActionMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return MakeFuture(ToObjectIds(GetValues(tabletManager->TabletActions(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::TabletAction;
    }

    TFuture<i64> GetSize() const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return MakeFuture<i64>(tabletManager->TabletActions().GetSize());
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

protected:
    TYPath GetWellKnownPath() const override
    {
        return "//sys/tablet_actions";
    }
};

INodeTypeHandlerPtr CreateTabletActionMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletActionMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTabletActionMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
