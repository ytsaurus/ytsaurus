#pragma once

#include "table_node.h"

#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public NChunkServer::TChunkOwnerTypeHandler<TTableNode>
{
public:
    typedef NChunkServer::TChunkOwnerTypeHandler<TTableNode> TBase;

    explicit TTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual bool IsExternalizable() const override;

protected:
    virtual NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual std::unique_ptr<TTableNode> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag cellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes) override;

    virtual void DoDestroy(TTableNode* table) override;

    virtual void DoBranch(
        const TTableNode* originatingNode,
        TTableNode* branchedNode,
        NCypressClient::ELockMode mode) override;
    virtual void DoMerge(
        TTableNode* originatingNode,
        TTableNode* branchedNode) override;
    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode) override;

    virtual int GetDefaultReplicationFactor() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNodeTypeHandler
    : public TTableNodeTypeHandler
{
public:
    explicit TReplicatedTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;

protected:
    virtual NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual std::unique_ptr<TTableNode> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag cellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes) override;

    virtual void DoDestroy(TTableNode* table) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

