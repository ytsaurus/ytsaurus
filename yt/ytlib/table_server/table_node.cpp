#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NTableServer {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : TCypressNodeBase(id, runtimeType)
{ }

TTableNode::TTableNode(const TBranchedNodeId& id, const TTableNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListId_(other.ChunkListId_)
{ }

ERuntimeNodeType TTableNode::GetRuntimeType() const
{
    return ERuntimeNodeType::Table;
}

TAutoPtr<ICypressNode> TTableNode::Clone() const
{
    return new TTableNode(Id, *this);
}

void TTableNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListId_);
}

void TTableNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, ChunkListId_);
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    TTableNodeTypeHandler(
        TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager)
        : TCypressNodeTypeHandlerBase<TTableNode>(cypressManager)
        , ChunkManager(chunkManager)
    {
        RegisterGetter("chunk_list_id", FromMethod(&TThis::GetChunkListId));
    }

    ERuntimeNodeType GetRuntimeType()
    {
        return ERuntimeNodeType::Table;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    Stroka GetTypeName()
    {
        return TableTypeName;
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        INode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);

        TAutoPtr<TTableNode> node = new TTableNode(
            TBranchedNodeId(nodeId, NullTransactionId),
            GetRuntimeType());

        // Create an empty chunk list and reference it from the node.
        auto& chunkList = ChunkManager->CreateChunkList();
        ChunkManager->RefChunkTree(chunkList);
        node->SetChunkListId(chunkList.GetId());

        return node.Release();
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId)
    {
        return New<TTableNodeProxy>(
            this,
            ~CypressManager,
            ~ChunkManager,
            transactionId,
            node.GetId().NodeId);
    }

protected:
    virtual void DoDestroy(TTableNode& node)
    {
        ChunkManager->UnrefChunkTree(node.GetChunkListId());
    }

    virtual void DoBranch(
        const TTableNode& committedNode,
        TTableNode& branchedNode)
    {
        // branchedNode is a copy of committedNode.
        
        // Create composite chunk list and place it in the root of branchedNode.
        auto& compositeChunkList = ChunkManager->CreateChunkList();
        branchedNode.SetChunkListId(compositeChunkList.GetId());
        ChunkManager->RefChunkTree(compositeChunkList);

        // Make the original chunk list a child of the composite one.
        auto committedChunkListId = committedNode.GetChunkListId();
        compositeChunkList.ChildrenIds().push_back(committedChunkListId);
        ChunkManager->RefChunkTree(committedChunkListId);
    }

    virtual void DoMerge(
        TTableNode& committedNode,
        TTableNode& branchedNode)
    {
        // TODO(babenko): this needs much improvement

        // Obtain the chunk list of branchedNode.
        auto branchedChunkListId = branchedNode.GetChunkListId();
        auto& branchedChunkList = ChunkManager->GetChunkListForUpdate(branchedChunkListId);
        YASSERT(branchedChunkList.GetRefCounter() == 1);

        // Replace the first child of the branched chunk list with the current chunk list of committedNode.
        YASSERT(branchedChunkList.ChildrenIds().size() >= 1);
        auto oldFirstChildId = branchedChunkList.ChildrenIds()[0];
        auto newFirstChildId = committedNode.GetChunkListId();
        branchedChunkList.ChildrenIds()[0] = newFirstChildId;
        ChunkManager->RefChunkTree(newFirstChildId);
        ChunkManager->UnrefChunkTree(oldFirstChildId);

        // Replace the chunk list of committedNode.
        committedNode.SetChunkListId(branchedChunkListId);
        ChunkManager->UnrefChunkTree(newFirstChildId);
    }

private:
    typedef TTableNodeTypeHandler TThis;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    static void GetChunkListId(const TGetAttributeParam& param)
    {
        BuildYsonFluently(param.Consumer).Scalar(param.Node->GetChunkListId().ToString());
    }
};

INodeTypeHandler::TPtr CreateTableTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    return New<TTableNodeTypeHandler>(
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

