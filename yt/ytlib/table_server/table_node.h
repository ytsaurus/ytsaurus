#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NTableServer {

using namespace NCypress;
using NChunkServer::TChunkListId;
using NChunkServer::NullChunkListId;

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NCypress::TCypressNodeBase
{
    DECLARE_BYVAL_RW_PROPERTY(ChunkListId, TChunkListId);

public:
    explicit TTableNode(const TBranchedNodeId& id);
    TTableNode(const TBranchedNodeId& id, const TTableNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

class TTableManager;

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    TTableNodeTypeHandler(
        TCypressManager* cypressManager,
        TTableManager* tableManager,
        NChunkServer::TChunkManager* chunkManager);

    ERuntimeNodeType GetRuntimeType();
    NYTree::ENodeType GetNodeType();
    Stroka GetTypeName();

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode::TPtr manifest);

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId);

protected:
    virtual void DoDestroy(TTableNode& node);

    virtual void DoBranch(
        const TTableNode& committedNode,
        TTableNode& branchedNode);

    virtual void DoMerge(
        TTableNode& committedNode,
        TTableNode& branchedNode);

private:
    typedef TTableNodeTypeHandler TThis;

    TIntrusivePtr<TTableManager> TableManager;
    TIntrusivePtr<NChunkServer::TChunkManager> ChunkManager;

    void GetSize(const TGetAttributeParam& param);
    static void GetChunkListId(const TGetAttributeParam& param);
    void GetChunkId(const TGetAttributeParam& param);
    
    const NChunkServer::TChunk* GetChunk(const TTableNode& node);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

