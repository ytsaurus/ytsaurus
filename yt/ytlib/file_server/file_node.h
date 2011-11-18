#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypress::TCypressNodeBase
{
    DECLARE_BYVAL_RW_PROPERTY(NChunkServer::TChunkListId, ChunkListId);

public:
    explicit TFileNode(const NCypress::TBranchedNodeId& id);
    TFileNode(const NCypress::TBranchedNodeId& id, const TFileNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual NCypress::ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

class TFileManager;

class TFileNodeTypeHandler
    : public NCypress::TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    TFileNodeTypeHandler(
        NCypress::TCypressManager* cypressManager,
        TFileManager* fileManager,
        NChunkServer::TChunkManager* chunkManager);

    NCypress::ERuntimeNodeType GetRuntimeType();
    NYTree::ENodeType GetNodeType();
    Stroka GetTypeName();

    virtual TAutoPtr<NCypress::ICypressNode> CreateFromManifest(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest);

    virtual TIntrusivePtr<NCypress::ICypressNodeProxy> GetProxy(
        const NCypress::ICypressNode& node,
        const NTransaction::TTransactionId& transactionId);

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

    TIntrusivePtr<TFileManager> FileManager;
    TIntrusivePtr<NChunkServer::TChunkManager> ChunkManager;

    void GetSize(const TGetAttributeParam& param);
    static void GetChunkListId(const TGetAttributeParam& param);
    void GetChunkId(const TGetAttributeParam& param);
    
    const NChunkServer::TChunk* GetChunk(const TFileNode& node);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

