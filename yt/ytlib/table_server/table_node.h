#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NCypress::TCypressNodeBase
{
    DECLARE_BYREF_RW_PROPERTY(ChunkListIds, yvector<NChunkServer::TChunkListId>);

public:
    explicit TTableNode(const NCypress::TBranchedNodeId& id);
    TTableNode(const NCypress::TBranchedNodeId& id, const TTableNode& other);

    virtual TAutoPtr<NCypress::ICypressNode> Clone() const;

    virtual NCypress::ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

class TTableManager;

class TTableNodeTypeHandler
    : public NCypress::TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    TTableNodeTypeHandler(
        NCypress::TCypressManager* cypressManager,
        TTableManager* tableManager,
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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

