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
private:
    using TBase = NChunkServer::TChunkOwnerTypeHandler<TImpl>;

public:
    using TBase::TBase;

    virtual bool IsSupportedInheritableAttribute(const TString& key) const override;

    virtual bool HasBranchedChangesImpl(TImpl* originatingNode, TImpl* branchedNode) override;

protected:
    virtual std::unique_ptr<TImpl> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        const NCypressServer::TCreateNodeContext& context) override;

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
        TImpl* clonedTrunkNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
    virtual void DoBeginCopy(
        TImpl* node,
        NCypressServer::TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TImpl* node,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TTableNodeTypeHandlerBase<TTableNode>
{
private:
    using TBase = TTableNodeTypeHandlerBase<TTableNode>;

public:
    using TBase::TBase;

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
private:
    using TBase = TTableNodeTypeHandlerBase<TReplicatedTableNode>;

public:
    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override;

    virtual bool HasBranchedChangesImpl(
        TReplicatedTableNode* originatingNode,
        TReplicatedTableNode* branchedNode) override;

protected:
    virtual NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TReplicatedTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBeginCopy(
        TReplicatedTableNode* node,
        NCypressServer::TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TReplicatedTableNode* node,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

