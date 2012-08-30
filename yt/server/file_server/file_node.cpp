#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/load_context.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(NULL)
{ }

EObjectType TFileNode::GetObjectType() const
{
    return EObjectType::File;
}

void TFileNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    SaveObjectRef(output, ChunkList_);
}

void TFileNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
    LoadObjectRef(input, ChunkList_, context);
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public NCypressServer::TCypressNodeTypeHandlerBase<TFileNode>
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

    virtual TAutoPtr<ICypressNode> Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        YCHECK(request);
        UNUSED(transaction);
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        if (!request->HasExtension(NFileClient::NProto::TReqCreateFileExt::create_file)) {
            THROW_ERROR_EXCEPTION("Missing request extension");
        }

        const auto& requestExt = request->GetExtension(NFileClient::NProto::TReqCreateFileExt::create_file);
        auto chunkId = TChunkId::FromProto(requestExt.chunk_id());

        auto* chunk = chunkManager->FindChunk(chunkId);
        if (!chunk) {
            THROW_ERROR_EXCEPTION("No such chunk %s", ~chunkId.ToString());
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("Chunk %s is not confirmed", ~chunkId.ToString());
        }

        auto node = TBase::DoCreate(transaction, request, response);

        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        std::vector<TChunkTreeRef> children;
        children.push_back(TChunkTreeRef(chunk));
        chunkManager->AttachToChunkList(chunkList, children);

        // TODO(babenko): Release is needed due to cast to ICypressNode.
        return node.Release();
    }

    virtual ICypressNodeProxyPtr GetProxy(
        ICypressNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TFileNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

protected:
    virtual void DoDestroy(TFileNode* node) override
    {
        TBase::DoDestroy(node);

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        Bootstrap->GetObjectManager()->UnrefObject(chunkList);
    }

    virtual void DoBranch(const TFileNode* originatingNode, TFileNode* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);
        
        auto* chunkList = originatingNode->GetChunkList();
        branchedNode->SetChunkList(chunkList);
        Bootstrap->GetObjectManager()->RefObject(chunkList);
        YCHECK(chunkList->OwningNodes().insert(branchedNode).second);
    }

    virtual void DoMerge(TFileNode* originatingNode, TFileNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        auto* chunkList = branchedNode->GetChunkList();
        Bootstrap->GetObjectManager()->UnrefObject(chunkList);
        YCHECK(chunkList->OwningNodes().erase(branchedNode) == 1);
    }

    virtual void DoClone(
        TFileNode* node,
        TTransaction* transaction,
        TFileNode* clonedNode) override
    {
        TBase::DoClone(node, transaction, clonedNode);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = node->GetChunkList();
        clonedNode->SetChunkList(chunkList);
        objectManager->RefObject(chunkList);
        YCHECK(chunkList->OwningNodes().insert(clonedNode).second);
    }

};

INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

