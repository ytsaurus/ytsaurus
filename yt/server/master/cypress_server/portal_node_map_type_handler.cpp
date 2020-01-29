#include "portal_node_map_type_handler.h"
#include "portal_manager.h"
#include "cypress_manager.h"
#include "virtual.h"
#include "portal_entrance_node.h"
#include "portal_exit_node.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/object_server/object.h>

#include <yt/core/ytree/virtual.h>

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

    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TObjectId> result;
        result.reserve(std::min<i64>(sizeLimit, Nodes_->size()));

        for (auto [id, node] : *Nodes_) {
            if (result.size() >= sizeLimit) {
                break;
            }
            if (!IsObjectAlive(node)) {
                continue;
            }
            result.push_back(id);
        }
        return result;
    }

    virtual bool IsValid(TObject* object) const
    {
        return true;
    }

    virtual i64 GetSize() const override
    {
        return Nodes_->size();
    }

    virtual TYPath GetWellKnownPath() const override
    {
        if constexpr(std::is_same_v<TNode, TPortalEntranceNode>) {
            return "//sys/portal_entrances";
        } else if constexpr(std::is_same_v<TNode, TPortalExitNode>) {
            return "//sys/portal_exits";
        } else {
            static_assert(TDependentFalse<TNode>::value, "Unexpected portal node type");
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
            &bootstrap->GetPortalManager()->EntranceNodes())
    { }
};

INodeTypeHandlerPtr CreatePortalEntranceMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::PortalEntranceMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
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
            &bootstrap->GetPortalManager()->ExitNodes())
    { }
};

INodeTypeHandlerPtr CreatePortalExitMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::PortalExitMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualPortalExitNodeMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
