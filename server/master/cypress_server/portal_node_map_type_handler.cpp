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
    : public TVirtualMapBase
{
public:
    TVirtualPortalNodeMapBase(
        TBootstrap* bootstrap,
        INodePtr owningNode,
        const THashMap<TNodeId, TNode*>* nodes)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
        , Nodes_(nodes)
    { }

private:
    using TBase = TVirtualMapBase;

    TBootstrap* const Bootstrap_;
    const THashMap<TNodeId, TNode*>* const Nodes_;

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> result;
        for (auto [id, node] : *Nodes_) {
            if (result.size() >= sizeLimit) {
                break;
            }
            if (!IsObjectAlive(node)) {
                continue;
            }
            result.push_back(ToString(id));
        }
        return result;
    }

    virtual i64 GetSize() const override
    {
        return Nodes_->size();
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        TNodeId id;
        if  (!TNodeId::FromString(key, &id)) {
            return nullptr;
        }

        auto it =  Nodes_->find(id);
        if (it == Nodes_->end()) {
            return nullptr;
        }

        auto* node = it->second;
        if (!IsObjectAlive(node)) {
            return nullptr;
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetNodeProxy(node);
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

//////////////////////////////////////////////////////////////////////////////////

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
