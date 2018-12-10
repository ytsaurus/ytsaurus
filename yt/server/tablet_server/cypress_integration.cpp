#include "cypress_integration.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/node_detail.h>
#include <yt/server/cypress_server/node_proxy_detail.h>
#include <yt/server/cypress_server/virtual.h>

#include <yt/server/tablet_server/tablet_manager.h>

#include <yt/server/misc/object_helpers.h>

#include <yt/client/object_client/helpers.h>

namespace NYT {
namespace NTabletServer {

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

class TTabletCellNodeProxy
    : public TMapNodeProxy
{
public:
    TTabletCellNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

    virtual TResolveResult ResolveSelf(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        const auto& method = context->GetMethod();
        if (method == "Remove") {
            return TResolveResultThere{GetTargetProxy(), path};
        } else {
            return TMapNodeProxy::ResolveSelf(path, context);
        }
    }

    virtual IYPathService::TResolveResult ResolveAttributes(
        const TYPath& path,
        const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultThere{GetTargetProxy(), "/@" + path};
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override
    {
        GetTargetProxy()->WriteAttributesFragment(consumer, attributeKeys, stable);
    }

private:
    IObjectProxyPtr GetTargetProxy() const
    {
        auto key = GetParent()->AsMap()->GetChildKeyOrThrow(this);
        auto id = TTabletCellId::FromString(key);
     
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* cell = tabletManager->GetTabletCellOrThrow(id);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(cell, nullptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTabletCellNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TTabletCellNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::TabletCellNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TTabletCellNodeProxy>(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateTabletCellNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TTabletCellNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTabletMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualTabletMap(TBootstrap* bootstrap, INodePtr owningProxy)
        : TVirtualMulticellMapBase(bootstrap, owningProxy)
    { }

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return ToObjectIds(GetValues(tabletManager->Tablets(), sizeLimit));
    }

    virtual bool IsValid(TObjectBase* object) const
    {
        return object->GetType() == EObjectType::Tablet;
    }

    virtual i64 GetSize() const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return tabletManager->Tablets().GetSize();
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

protected:
    virtual TYPath GetWellKnownPath() const override
    {
        return "//sys/tablets";
    }
};

INodeTypeHandlerPtr CreateTabletMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTabletMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTabletCellBundleMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualTabletCellBundleMap(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

private:
    const TBootstrap* Bootstrap_;

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return ToNames(GetValues(tabletManager->TabletCellBundles(), sizeLimit));
    }

    virtual i64 GetSize() const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return tabletManager->TabletCellBundles().GetSize();
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* cellBundle = tabletManager->FindTabletCellBundleByName(TString(key));
        if (!IsObjectAlive(cellBundle)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(cellBundle);
    }
};

INodeTypeHandlerPtr CreateTabletCellBundleMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletCellBundleMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTabletCellBundleMap>(bootstrap);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualTabletActionMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualTabletActionMap(TBootstrap* bootstrap, INodePtr owningProxy)
        : TVirtualMulticellMapBase(bootstrap, owningProxy)
    { }

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return ToObjectIds(GetValues(tabletManager->TabletActions(), sizeLimit));
    }

    virtual bool IsValid(TObjectBase* object) const
    {
        return object->GetType() == EObjectType::TabletAction;
    }

    virtual i64 GetSize() const override
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return tabletManager->TabletActions().GetSize();
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

protected:
    virtual TYPath GetWellKnownPath() const override
    {
        return "//sys/tablet_actions";
    }
};

INodeTypeHandlerPtr CreateTabletActionMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletActionMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTabletActionMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
