#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(NULL)
    , ReplicationFactor_(0)
{ }

int TFileNode::GetOwningReplicationFactor() const 
{
    auto* trunkNode = TrunkNode_ == this ? this : dynamic_cast<TFileNode*>(TrunkNode_);
    YCHECK(trunkNode);
    return trunkNode->GetReplicationFactor();
}

void TFileNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRef(output, ChunkList_);
    ::Save(output, ReplicationFactor_);
}

void TFileNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRef(input, ChunkList_, context);
    ::Load(input, ReplicationFactor_);
}

TClusterResources TFileNode::GetResourceUsage() const 
{
    if (Transaction_) {
        return ZeroClusterResources();
    }

    if (!ChunkList_) {
        return ZeroClusterResources();
    }

    i64 diskSpace = ChunkList_->Statistics().DiskSpace * ReplicationFactor_;
    return TClusterResources::FromDiskSpace(diskSpace);
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public NCypressServer::TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TFileNode> TBase;

    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual void SetDefaultAttributes(IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("replication_factor")) {
            attributes->Set("replication_factor", 3);
        }
    }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::File;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TFileNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual TAutoPtr<TFileNode> DoCreate(
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
        if (!chunk || !chunk->IsAlive()) {
            THROW_ERROR_EXCEPTION("No such chunk: %s", ~chunkId.ToString());
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("File chunk is not confirmed: %s", ~chunkId.ToString());
        }

        auto node = TBase::DoCreate(transaction, request, response);

        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        chunkManager->AttachToChunkList(chunkList, TChunkTreeRef(chunk));

        return node;
    }

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

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
    }

    virtual void DoMerge(TFileNode* originatingNode, TFileNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        auto* chunkList = branchedNode->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(branchedNode) == 1);
        Bootstrap->GetObjectManager()->UnrefObject(chunkList);
    }

    virtual void DoClone(
        TFileNode* sourceNode,
        TFileNode* clonedNode,
        TTransaction* transaction) override
    {
        TBase::DoClone(sourceNode, clonedNode, transaction);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = sourceNode->GetChunkList();
        YCHECK(!clonedNode->GetChunkList());
        clonedNode->SetChunkList(chunkList);
        clonedNode->SetReplicationFactor(sourceNode->GetReplicationFactor());
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

