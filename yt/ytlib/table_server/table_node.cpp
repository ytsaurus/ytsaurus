#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "common.h"

#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypress;
using namespace NYTree;
using namespace NChunkServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(NULL)
    , BranchMode_(ETableBranchMode::None)
{ }

TTableNode::TTableNode(const TVersionedNodeId& id, const TTableNode& other)
    : TCypressNodeBase(id, other)
    , ChunkList_(other.ChunkList_)
    , KeyColumns_(other.KeyColumns_)
    , BranchMode_(other.BranchMode_)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

void TTableNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    SaveObjectRef(output, ChunkList_);
    ::Save(output, KeyColumns_);
    ::Save(output, BranchMode_);
}

void TTableNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
    LoadObjectRef(input, ChunkList_, context);
    ::Load(input, KeyColumns_);
    ::Load(input, BranchMode_);
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TTableNode> TBase;

    TTableNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    EObjectType GetObjectType()
    {
        return EObjectType::Table;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    virtual bool IsLockModeSupported(ELockMode mode)
    {
        return
            mode == ELockMode::Exclusive ||
            mode == ELockMode::Shared ||
            mode == ELockMode::Snapshot;
    }

    virtual TNodeId CreateDynamic(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Parse and validate channels, if any.
        auto ysonChannels = request->Attributes().FindYson("channels");
        if (ysonChannels) {
            try {
                ChannelsFromYson(ysonChannels.Get());
            } catch (const std::exception& ex) {
                ythrow yexception() << Sprintf("Invalid table channels\n%s", ex.what());
            }
        } else {
            request->Attributes().SetYson("channels", "[]");
        }

        auto nodeId = objectManager->GenerateId(EObjectType::Table);
        TAutoPtr<TTableNode> node(new TTableNode(nodeId));

        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);

        objectManager->RefObject(chunkList);

        cypressManager->RegisterNode(transaction, node.Release());

        return nodeId;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction)
    {
        return New<TTableNodeProxy>(
            this,
            Bootstrap,
            transaction,
            nodeId);
    }

protected:
    virtual void DoDestroy(TTableNode& node)
    {
        YCHECK(node.GetChunkList()->OwningNodes().erase(&node) == 1);
        Bootstrap->GetObjectManager()->UnrefObject(node.GetChunkList());
    }

    virtual void DoBranch(const TTableNode* originatingNode, TTableNode* branchedNode)
    {
        // branchedNode is a copy of originatingNode.
        
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();
      
        // Default branch mode is Append.
        branchedNode->SetBranchMode(ETableBranchMode::Append);

        // Create composite chunk list and place it in the root of branchedNode.
        auto* branchedChunkList = chunkManager->CreateChunkList();

        // For Append mode, the first child of the branched chunk list has a special
        // meaning: it captures the state of the table at the moment it was branched.
        // Suppress rebalancing for this chunk list to prevent
        // unwanted modifications of the children set.
        branchedChunkList->SetRebalancingEnabled(false);

        branchedNode->SetChunkList(branchedChunkList);
        YCHECK(branchedChunkList->OwningNodes().insert(branchedNode).second);
        objectManager->RefObject(branchedChunkList);

        // Make the original chunk list a child of the composite one.
        std::vector<TChunkTreeRef> children;
        auto* originatingChunkList = originatingNode->GetChunkList();
        children.push_back(TChunkTreeRef(originatingChunkList));
        chunkManager->AttachToChunkList(branchedChunkList, children);

        // Propagate "sorted" attribute.
        branchedChunkList->SetSorted(originatingChunkList->GetSorted());

        // TODO(babenko): IsRecovery
        LOG_DEBUG("Table node branched (BranchedNodeId: %s, OriginatingChunkListId: %s, BranchedChunkListId: %s)",
            ~branchedNode->GetId().ToString(),
            ~originatingChunkList->GetId().ToString(),
            ~branchedChunkList->GetId().ToString());
    }

    virtual void DoMerge(TTableNode* originatingNode, TTableNode* branchedNode)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Create a new chunk list obtained from the branched one
        // by replacing the first child with its up-to-date state.
        auto* branchedChunkList = branchedNode->GetChunkList();
        auto* currentChunkList = originatingNode->GetChunkList();

        // TODO(babenko): IsRecovery
        LOG_DEBUG("Table node merged (BranchedNodeId: %s, BranchMode: %s, CurrentChunkListId: %s, BranchedChunkListId: %s)",
            ~branchedNode->GetId().ToString(),
            ~branchedNode->GetBranchMode().ToString(),
            ~currentChunkList->GetId().ToString(),
            ~branchedChunkList->GetId().ToString());

        switch (branchedNode->GetBranchMode()) {
            case ETableBranchMode::Append: {
                // Construct newChunkList that will replace currentChunkList in originatingNode.
                // Append the following items to it:
                // 1) all children of currentChunkList
                // 2) all children of branchedChunkList except the first one
                auto* newChunkList = chunkManager->CreateChunkList();
                objectManager->RefObject(newChunkList);
                chunkManager->AttachToChunkList(
                    newChunkList,
                    currentChunkList->Children());
                const auto& branchedChildren = branchedChunkList->Children();
                YASSERT(!branchedChildren.empty());
                chunkManager->AttachToChunkList(
                    newChunkList,
                    &*branchedChildren.begin() + 1,
                    &*branchedChildren.begin() + branchedChildren.size());

                // Configure rebalancing depending on its mode for currentChunkList.
                newChunkList->SetRebalancingEnabled(currentChunkList->GetRebalancingEnabled());

                // Propagate "sorted" attribute back.
                newChunkList->SetSorted(branchedChunkList->GetSorted());

                // Assign newChunkList to originatingNode.
                originatingNode->SetChunkList(newChunkList);
                YCHECK(newChunkList->OwningNodes().insert(originatingNode).second);
                YCHECK(currentChunkList->OwningNodes().erase(originatingNode) == 1);
                objectManager->UnrefObject(currentChunkList);
                YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);
                objectManager->UnrefObject(branchedChunkList);

                break;
            }

            case ETableBranchMode::Overwrite: {
                // Just replace currentChunkList with branchedChunkList.
                originatingNode->SetChunkList(branchedChunkList);
                objectManager->UnrefObject(currentChunkList);
                YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);
                YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
                YCHECK(branchedChunkList->GetRebalancingEnabled());
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

