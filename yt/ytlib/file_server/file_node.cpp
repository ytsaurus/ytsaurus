#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_ypath_proxy.h"

#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NCypress::NProto;

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

void TFileNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
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

    virtual TNodeId CreateDynamic(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // TODO(babenko): use extensions
        auto chunkId = TNodeId::FromString(request->Attributes().Get<Stroka>("chunk_id"));
        request->Attributes().Remove("chunk_id");

        auto* chunk = chunkManager->FindChunk(chunkId);
        if (!chunk) {
            ythrow yexception() << Sprintf("No such chunk %s", ~chunkId.ToString());
        }

        if (!chunk->IsConfirmed()) {
            ythrow yexception() << Sprintf("Chunk %s is not confirmed", ~chunkId.ToString());
        }

        auto nodeId = objectManager->GenerateId(EObjectType::File);
        TAutoPtr<TFileNode> node(new TFileNode(nodeId));
        auto& chunkList = chunkManager->CreateChunkList();
        auto chunkListId = chunkList.GetId();
        node->SetChunkListId(chunkListId);
        YVERIFY(chunkList.OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkListId);

        yvector<TChunkTreeRef> children;
        children.push_back(TChunkTreeRef(chunk));
        chunkManager->AttachToChunkList(chunkList, children);

        cypressManager->RegisterNode(transaction, node.Release());

        return nodeId;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const TNodeId& nodeId,
        TTransaction* transaction)
    {
        return New<TFileNodeProxy>(
            this,
            Bootstrap,
            transaction,
            nodeId);
    }

protected:
    virtual void DoDestroy(TFileNode& node)
    {
        auto& chunkList = Bootstrap->GetChunkManager()->GetChunkList(node.GetChunkListId());
        YVERIFY(chunkList.OwningNodes().erase(&node) == 1);
        Bootstrap->GetObjectManager()->UnrefObject(node.GetChunkListId());
    }

    virtual void DoBranch(const TFileNode& originatingNode, TFileNode& branchedNode)
    {
        UNUSED(originatingNode);

        // branchedNode is a copy of originatingNode.
        // Reference the list chunk from branchedNode.
        Bootstrap->GetObjectManager()->RefObject(branchedNode.GetChunkListId());
        auto& chunkList = Bootstrap->GetChunkManager()->GetChunkList(
            branchedNode.GetChunkListId());
        YVERIFY(chunkList.OwningNodes().insert(&branchedNode).second);
    }

    virtual void DoMerge(TFileNode& originatingNode, TFileNode& branchedNode)
    {
        UNUSED(originatingNode);

        // Drop the reference from branchedNode.
        Bootstrap->GetObjectManager()->UnrefObject(branchedNode.GetChunkListId());
        auto& chunkList = Bootstrap->GetChunkManager()->GetChunkList(
            branchedNode.GetChunkListId());
        YVERIFY(chunkList.OwningNodes().erase(&branchedNode) == 1);
    }

};

INodeTypeHandler::TPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

