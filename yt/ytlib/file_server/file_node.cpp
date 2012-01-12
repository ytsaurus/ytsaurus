#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_ypath_proxy.h"
#include "file_chunk_meta.pb.h"

#include "../misc/codec.h"
#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NFileServer {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NFileClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id, EObjectType objectType)
    : TCypressNodeBase(id, objectType)
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
        TChunkManager* chunkManager)
        : TCypressNodeTypeHandlerBase<TFileNode>(cypressManager)
        , ChunkManager(chunkManager)
    {
        // NB: No smartpointer for this here.
        RegisterGetter("size", FromMethod(&TThis::GetSize, this));
        RegisterGetter("codec_id", FromMethod(&TThis::GetCodecId, this));
        RegisterGetter("chunk_list_id", FromMethod(&TThis::GetChunkListId));
        RegisterGetter("chunk_id", FromMethod(&TThis::GetChunkId, this));
    }

    EObjectType GetObjectType()
    {
        return EObjectType::File;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    Stroka GetTypeName()
    {
        return FileTypeName;
    }

    virtual TAutoPtr<NCypress::ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode* manifestNode)
    {
        UNUSED(transactionId);

        auto manifest = New<TFileManifest>();
        manifest->LoadAndValidate(manifestNode);

        auto chunkId = manifest->ChunkId;
        auto* chunk = ChunkManager->FindChunkForUpdate(chunkId);
        if (!chunk) {
            ythrow yexception() << Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
        }

        if (!chunk->IsConfirmed()) {
            ythrow yexception() << Sprintf("Chunk is not confirmed (ChunkId: %s)", ~chunkId.ToString());
        }

        TAutoPtr<TFileNode> node = new TFileNode(nodeId, GetObjectType());

        // File node references chunk list.
        auto& chunkList = ChunkManager->CreateChunkList();
        auto chunkListId = chunkList.GetId();
        node->SetChunkListId(chunkListId);
        CypressManager->GetObjectManager()->RefObject(chunkListId);

        // Chunk list references chunk.
        chunkList.ChildrenIds().push_back(chunkId);
        CypressManager->GetObjectManager()->RefObject(chunkId);

        return node.Release();
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
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

protected:
    virtual void DoDestroy(TFileNode& node)
    {
        CypressManager->GetObjectManager()->UnrefObject(node.GetChunkListId());
    }

    virtual void DoBranch(
        const TFileNode& committedNode,
        TFileNode& branchedNode)
    {
        UNUSED(committedNode);

        // branchedNode is a copy of committedNode.
        // Reference the list chunk from branchedNode.
        CypressManager->GetObjectManager()->RefObject(branchedNode.GetChunkListId());
    }

    virtual void DoMerge(
        TFileNode& committedNode,
        TFileNode& branchedNode)
    {
        UNUSED(committedNode);

        // Drop the reference from branchedNode.
        CypressManager->GetObjectManager()->UnrefObject(branchedNode.GetChunkListId());
    }

private:
    typedef TFileNodeTypeHandler TThis;

    TIntrusivePtr<TChunkManager> ChunkManager;

    void GetSize(const TGetAttributeParam& param)
    {
        const auto& chunk = GetChunk(*param.Node);
        const auto& attributes = chunk
            .DeserializeAttributes()
            .GetExtension(TFileChunkAttributes::file_attributes);
        BuildYsonFluently(param.Consumer).Scalar(attributes.size());
    }

    void GetCodecId(const TGetAttributeParam& param)
    {
        const auto& chunk = GetChunk(*param.Node);
        const auto& attributes = chunk
            .DeserializeAttributes()
            .GetExtension(TFileChunkAttributes::file_attributes);
        BuildYsonFluently(param.Consumer)
            .Scalar(ECodecId(attributes.codec_id()).ToString());
    }
    
    static void GetChunkListId(const TGetAttributeParam& param)
    {
        BuildYsonFluently(param.Consumer)
            .Scalar(param.Node->GetChunkListId().ToString());
    }

    void GetChunkId(const TGetAttributeParam& param)
    {
        const auto& chunk = GetChunk(*param.Node);
        BuildYsonFluently(param.Consumer)
            .Scalar(chunk.GetId().ToString());
    }

    const TChunk& GetChunk(const TFileNode& node)
    {
        const auto& chunkList = ChunkManager->GetChunkList(node.GetChunkListId());
        YASSERT(chunkList.ChildrenIds().ysize() == 1);
        auto chunkId = chunkList.ChildrenIds()[0];
        return ChunkManager->GetChunk(chunkId);
    }

};

INodeTypeHandler::TPtr CreateFileTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    return New<TFileNodeTypeHandler>(
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

