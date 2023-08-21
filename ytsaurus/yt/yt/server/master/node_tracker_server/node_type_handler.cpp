#include "node_type_handler.h"
#include "node_tracker.h"
#include "node.h"
#include "node_proxy.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

namespace NYT::NNodeTrackerServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TNodeTypeHandler
    : public TObjectTypeHandlerWithMapBase<TNode>
{
public:
    TNodeTypeHandler(
        NCellMaster::TBootstrap* bootstrap,
        TEntityMap<TNode>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::ClusterNode;
    }

    TObject* FindObject(TObjectId id) override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNode(NodeIdFromObjectId(id));
        if (!node) {
            return nullptr;
        }
        if (nodeTracker->ObjectIdFromNodeId(node->GetId()) != id) {
            return nullptr;
        }
        return node;
    }

private:
    TCellTagList DoGetReplicationCellTags(const TNode* /*node*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(TNode* node, TTransaction* /*transaction*/) override
    {
        return CreateClusterNodeProxy(Bootstrap_, &Metadata_, node);
    }

    void DoZombifyObject(TNode* node) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(node);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->ZombifyNode(node);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateNodeTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    TEntityMap<TNode>* map)
{
    return New<TNodeTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
