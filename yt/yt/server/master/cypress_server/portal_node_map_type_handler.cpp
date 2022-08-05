#include "portal_node_map_type_handler.h"
#include "portal_manager.h"
#include "cypress_manager.h"
#include "virtual.h"
#include "portal_entrance_node.h"
#include "portal_exit_node.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
class TVirtualPortalNodeMapBase
    : public TVirtualMulticellMapBase
{
public:
    TVirtualPortalNodeMapBase(
        TBootstrap* bootstrap,
        INodePtr owningNode,
        const THashMap<TNodeId, TNode*>* nodes)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
        , Nodes_(nodes)
    { }

private:
    using TBase = TVirtualMapBase;

    const THashMap<TNodeId, TNode*>* const Nodes_;

    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TObjectId> result;
        result.reserve(std::min<i64>(sizeLimit, Nodes_->size()));

        for (auto [id, node] : *Nodes_) {
            if (std::ssize(result) >= sizeLimit) {
                break;
            }
            if (!IsObjectAlive(node)) {
                continue;
            }
            result.push_back(id);
        }
        return MakeFuture(std::move(result));
    }

    bool IsValid(TObject* /*object*/) const override
    {
        return true;
    }

    TFuture<i64> GetSize() const override
    {
        return MakeFuture<i64>(Nodes_->size());
    }

    TYPath GetWellKnownPath() const override
    {
        if constexpr(std::is_same_v<TNode, TPortalEntranceNode>) {
            return "//sys/portal_entrances";
        } else if constexpr(std::is_same_v<TNode, TPortalExitNode>) {
            return "//sys/portal_exits";
        } else {
            static_assert(TDependentFalse<TNode>, "Unexpected portal node type");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualPortalEntranceNodeMap
    : public TVirtualPortalNodeMapBase<TPortalEntranceNode>
{
public:
    TVirtualPortalEntranceNodeMap(
        TBootstrap* bootstrap,
        INodePtr owningNode)
        : TVirtualPortalNodeMapBase(
            bootstrap,
            owningNode,
            &bootstrap->GetPortalManager()->GetEntranceNodes())
    { }
};

INodeTypeHandlerPtr CreatePortalEntranceMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::PortalEntranceMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualPortalEntranceNodeMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualPortalExitNodeMap
    : public TVirtualPortalNodeMapBase<TPortalExitNode>
{
public:
    TVirtualPortalExitNodeMap(
        TBootstrap* bootstrap,
        INodePtr owningNode)
        : TVirtualPortalNodeMapBase(
            bootstrap,
            owningNode,
            &bootstrap->GetPortalManager()->GetExitNodes())
    { }
};

INodeTypeHandlerPtr CreatePortalExitMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::PortalExitMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualPortalExitNodeMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
