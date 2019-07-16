#pragma once

#include "table_node.h"

#include <yt/server/master/chunk_server/chunk_owner_type_handler.h>

#include <yt/core/ytree/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TTableNodeTypeHandlerBase
    : public NChunkServer::TChunkOwnerTypeHandler<TImpl>
{
public:
    explicit TTableNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectServer::ETypeFlags GetFlags() const override;

    virtual bool IsSupportedInheritableAttribute(const TString& key) const override;

    virtual bool HasBranchedChangesImpl(TImpl* originatingNode, TImpl* branchedNode) override;

protected:
    using TBase = NChunkServer::TChunkOwnerTypeHandler<TImpl>;

    virtual std::unique_ptr<TImpl> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag cellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account) override;

    virtual void DoDestroy(TImpl* table) override;

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;
    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;
    virtual void DoClone(
        TImpl* sourceNode,
        TImpl* clonedNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TTableNodeTypeHandlerBase<TTableNode>
{
public:
    typedef TTableNodeTypeHandlerBase<TTableNode> TBase;

    explicit TTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;

protected:
    virtual NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNodeTypeHandler
    : public TTableNodeTypeHandlerBase<TReplicatedTableNode>
{
public:
    typedef TTableNodeTypeHandlerBase<TReplicatedTableNode> TBase;

    explicit TReplicatedTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;

    virtual bool HasBranchedChangesImpl(
        TReplicatedTableNode* originatingNode,
        TReplicatedTableNode* branchedNode) override;

protected:
    virtual NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TReplicatedTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

