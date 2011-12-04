#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_chunk_server_meta.pb.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NFileServer {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NFileClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : TCypressNodeBase(id, runtimeType)
    , ChunkListId_(NullChunkListId)
{ }

TFileNode::TFileNode(const TBranchedNodeId& id, const TFileNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListId_(other.ChunkListId_)
{ }

ERuntimeNodeType TFileNode::GetRuntimeType() const
{
    return ERuntimeNodeType::File;
}

TAutoPtr<ICypressNode> TFileNode::Clone() const
{
    return new TFileNode(Id, *this);
}

void TFileNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListId_);
}

void TFileNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, ChunkListId_);
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public NCypress::TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    TFileNodeTypeHandler(
        TCypressManager* cypressManager,
        TChunkManager* chunkManager);

    ERuntimeNodeType GetRuntimeType();
    ENodeType GetNodeType();
    Stroka GetTypeName();

    virtual TAutoPtr<NCypress::ICypressNode> CreateFromManifest(
        const NCypress::TNodeId& nodeId,
        const NTransactionServer::TTransactionId& transactionId,
        INode* manifest);

    virtual TIntrusivePtr<NCypress::ICypressNodeProxy> GetProxy(
        const NCypress::ICypressNode& node,
        const NTransactionServer::TTransactionId& transactionId);

protected:
    virtual void DoDestroy(TFileNode& node);

    virtual void DoBranch(
        const TFileNode& committedNode,
        TFileNode& branchedNode);

    virtual void DoMerge(
        TFileNode& committedNode,
        TFileNode& branchedNode);

private:
    typedef TFileNodeTypeHandler TThis;

    TIntrusivePtr<NChunkServer::TChunkManager> ChunkManager;

    void GetSize(const TGetAttributeParam& param);
    void GetBlockCount(const TGetAttributeParam& param);
    static void GetChunkListId(const TGetAttributeParam& param);
    void GetChunkId(const TGetAttributeParam& param);

    const NChunkServer::TChunk* GetChunk(const TFileNode& node);

};

NCypress::INodeTypeHandler::TPtr CreateFileTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    return New<TFileNodeTypeHandler>(
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

TFileNodeTypeHandler::TFileNodeTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
    : TCypressNodeTypeHandlerBase<TFileNode>(cypressManager)
    , ChunkManager(chunkManager)
{
    // NB: No smartpointer for this here.
    RegisterGetter("size", FromMethod(&TThis::GetSize, this));
    RegisterGetter("block_count", FromMethod(&TThis::GetBlockCount, this));
    RegisterGetter("chunk_list_id", FromMethod(&TThis::GetChunkListId));
    RegisterGetter("chunk_id", FromMethod(&TThis::GetChunkId, this));
}

void TFileNodeTypeHandler::GetSize(const TGetAttributeParam& param)
{
    const auto* chunk = GetChunk(*param.Node);

    if (chunk == NULL || chunk->GetChunkInfo() == TSharedRef()) {
        BuildYsonFluently(param.Consumer)
            .Scalar(-1);
        return;
    }

    auto info = chunk->DeserializeChunkInfo();
    BuildYsonFluently(param.Consumer)
        .Scalar(info.GetSize());
}

void TFileNodeTypeHandler::GetBlockCount(const TGetAttributeParam& param)
{
    const auto* chunk = GetChunk(*param.Node);

    if (chunk == NULL || chunk->GetChunkInfo() == TSharedRef()) {
        BuildYsonFluently(param.Consumer)
            .Scalar(-1);
        return;
    }

    auto info = chunk->DeserializeChunkInfo();
    BuildYsonFluently(param.Consumer)
        .Scalar(info.BlocksSize());
}

void TFileNodeTypeHandler::GetChunkListId(const TGetAttributeParam& param)
{
    BuildYsonFluently(param.Consumer)
        .Scalar(param.Node->GetChunkListId().ToString());
}

void TFileNodeTypeHandler::GetChunkId(const TGetAttributeParam& param)
{
    const auto* chunk = GetChunk(*param.Node);
    auto chunkId = chunk == NULL ? NChunkClient::TChunkId() : chunk->GetId();
    BuildYsonFluently(param.Consumer)
        .Scalar(chunkId.ToString());
}

const NChunkServer::TChunk* TFileNodeTypeHandler::GetChunk(const TFileNode& node)
{
    if (node.GetChunkListId() == NullChunkListId) {
        return NULL;
    }

    const auto& chunkList = ChunkManager->GetChunkList(node.GetChunkListId());
    YASSERT(chunkList.ChunkIds().ysize() == 1);

    return &ChunkManager->GetChunk(chunkList.ChunkIds()[0]);
}

void TFileNodeTypeHandler::DoDestroy(TFileNode& node)
{
    if (node.GetChunkListId() != NullChunkListId) {
        ChunkManager->UnrefChunkList(node.GetChunkListId());
    }
}

void TFileNodeTypeHandler::DoBranch(
    const TFileNode& committedNode,
    TFileNode& branchedNode)
{
    UNUSED(committedNode);

    // branchedNode is a copy of committedNode.

    // Reference the list chunk from branchedNode.
    if (branchedNode.GetChunkListId() != NullChunkListId) {
        ChunkManager->RefChunkList(branchedNode.GetChunkListId());
    }
}

void TFileNodeTypeHandler::DoMerge(
    TFileNode& committedNode,
    TFileNode& branchedNode)
{
    // Drop the reference from committedNode.
    if (committedNode.GetChunkListId() != NullChunkListId) {
        ChunkManager->UnrefChunkList(committedNode.GetChunkListId());
    }

    // Transfer the chunklist from branchedNode to committedNode.
    committedNode.SetChunkListId(branchedNode.GetChunkListId());
}

TIntrusivePtr<ICypressNodeProxy> TFileNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TFileNodeProxy>(
        this,
        ~CypressManager,
        ~ChunkManager,
        transactionId,
        node.GetId().NodeId);
}

ERuntimeNodeType TFileNodeTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::File;
}

ENodeType TFileNodeTypeHandler::GetNodeType()
{
    return ENodeType::Entity;
}

Stroka TFileNodeTypeHandler::GetTypeName()
{
    return FileTypeName;
}

TAutoPtr<ICypressNode> TFileNodeTypeHandler::CreateFromManifest(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    INode* manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);

    return new TFileNode(
        TBranchedNodeId(nodeId, NullTransactionId),
        GetRuntimeType());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

