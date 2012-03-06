#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_ypath_proxy.h"

#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkListId_(NullChunkListId)
{ }

TFileNode::TFileNode(const TVersionedNodeId& id, const TFileNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListId_(other.ChunkListId_)
{ }

EObjectType TFileNode::GetObjectType() const
{
    return EObjectType::File;
}

void TFileNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListId_);
}

void TFileNode::Load(TInputStream* input, const TLoadContext& context)
{
    TCypressNodeBase::Load(input, context);
    ::Load(input, ChunkListId_);
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public NCypress::TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TFileNode> TBase;

    TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    EObjectType GetObjectType()
    {
        return EObjectType::File;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    virtual void CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode* manifestNode)
    {
        auto manifest = New<TFileManifest>();
        manifest->SetKeepOptions(true);
        manifest->LoadAndValidate(manifestNode);

        auto chunkId = manifest->ChunkId;
        auto* chunk = ChunkManager->FindChunk(chunkId);
        if (!chunk) {
            ythrow yexception() << Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
        }

        if (!chunk->IsConfirmed()) {
            ythrow yexception() << Sprintf("Chunk is not confirmed (ChunkId: %s)", ~chunkId.ToString());
        }

        TAutoPtr<TFileNode> node = new TFileNode(nodeId);
        auto& chunkList = ChunkManager->CreateChunkList();
        auto chunkListId = chunkList.GetId();
        node->SetChunkListId(chunkListId);
        Bootstrap->GetObjectManager()->RefObject(chunkListId);
        Bootstrap->GetCypressManager()->RegisterNode(transactionId, node.Release());

        auto proxy = Bootstrap->GetCypressManager()->GetVersionedNodeProxy(nodeId, NullTransactionId);
        proxy->Attributes().MergeFrom(~manifest->GetOptions());
        
        yvector<TChunkTreeId> childrenIds;
        childrenIds.push_back(chunkId);
        Bootstrap->GetChunkManager()->AttachToChunkList(chunkList, childrenIds);
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<TFileNodeProxy>(
            this,
            Bootstrap,
            id.TransactionId,
            id.ObjectId);
    }

protected:
    virtual void DoDestroy(TFileNode& node)
    {
        Bootstrap->GetObjectManager()->UnrefObject(node.GetChunkListId());
    }

    virtual void DoBranch(
        const TFileNode& originatingNode,
        TFileNode& branchedNode)
    {
        UNUSED(originatingNode);

        // branchedNode is a copy of originatingNode.
        // Reference the list chunk from branchedNode.
        Bootstrap->GetObjectManager()->RefObject(branchedNode.GetChunkListId());
    }

    virtual void DoMerge(
        TFileNode& originatingNode,
        TFileNode& branchedNode)
    {
        UNUSED(originatingNode);

        // Drop the reference from branchedNode.
        Bootstrap->GetObjectManager()->UnrefObject(branchedNode.GetChunkListId());
    }

private:
    typedef TFileNodeTypeHandler TThis;

    TIntrusivePtr<TChunkManager> ChunkManager;

};

INodeTypeHandler::TPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

