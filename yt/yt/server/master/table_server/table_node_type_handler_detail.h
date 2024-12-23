#pragma once

#include "table_node.h"

#include <yt/yt/server/master/table_server/schemaful_node_type_handler.h>

#include <yt/yt/server/master/tablet_server/tablet_owner_type_handler_base.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TTableNodeTypeHandlerBase
    : public TSchemafulNodeTypeHandlerBase<TImpl>
    , public NTabletServer::TTabletOwnerTypeHandlerBase<TImpl>
{
private:
    using TTabletOwnerTypeHandler = NTabletServer::TTabletOwnerTypeHandlerBase<TImpl>;
    using TSchemafulNodeTypeHandler = TSchemafulNodeTypeHandlerBase<TImpl>;

public:
    explicit TTableNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    bool IsSupportedInheritableAttribute(const TString& key) const override;

    bool HasBranchedChangesImpl(TImpl* originatingNode, TImpl* branchedNode) override;

protected:
    std::unique_ptr<TImpl> DoCreate(
        NCypressServer::TVersionedNodeId id,
        const NCypressServer::TCreateNodeContext& context) override;

    void DoDestroy(TImpl* table) override;
    void DoZombify(TImpl* table) override;

    void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;
    void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
    void DoSerializeNode(
        TImpl* node,
        NCypressServer::TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TImpl* node,
        NCypressServer::TMaterializeNodeContext* context) override;

    std::optional<std::vector<std::string>> DoListColumns(TImpl* node) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TTableNodeTypeHandlerBase<TTableNode>
{
private:
    using TBase = TTableNodeTypeHandlerBase<TTableNode>;

public:
    explicit TTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    NObjectClient::EObjectType GetObjectType() const override;

protected:
    NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationLogTableNodeTypeHandler
    : public TTableNodeTypeHandler
{
public:
    explicit TReplicationLogTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    NObjectClient::EObjectType GetObjectType() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNodeTypeHandler
    : public TTableNodeTypeHandlerBase<TReplicatedTableNode>
{
private:
    using TBase = TTableNodeTypeHandlerBase<TReplicatedTableNode>;

public:
    explicit TReplicatedTableNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    NObjectClient::EObjectType GetObjectType() const override;

    bool HasBranchedChangesImpl(
        TReplicatedTableNode* originatingNode,
        TReplicatedTableNode* branchedNode) override;

protected:
    NCypressServer::ICypressNodeProxyPtr DoGetProxy(
        TReplicatedTableNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    void DoBranch(
        const TReplicatedTableNode* originatingNode,
        TReplicatedTableNode* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    void DoClone(
        TReplicatedTableNode* sourceNode,
        TReplicatedTableNode* clonedTrunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
    void DoSerializeNode(
        TReplicatedTableNode* node,
        NCypressServer::TSerializeNodeContext* context) override;
    void DoMaterializeNode(
        TReplicatedTableNode* node,
        NCypressServer::TMaterializeNodeContext* context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
