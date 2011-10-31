#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NFileServer {

using namespace NCypress;
using NChunkServer::TChunkListId;
using NChunkServer::NullChunkListId;

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypress::TCypressNodeBase
{
    DECLARE_BYVAL_RW_PROPERTY(ChunkListId, TChunkListId);

public:
    explicit TFileNode(const TBranchedNodeId& id);
    TFileNode(const TBranchedNodeId& id, const TFileNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

class TFileManager;

class TFileNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    TFileNodeTypeHandler(
        TCypressManager* cypressManager,
        TFileManager* fileManager,
        NChunkServer::TChunkManager* chunkManager);

    ERuntimeNodeType GetRuntimeType();

    Stroka GetTypeName();

    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest);

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

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

