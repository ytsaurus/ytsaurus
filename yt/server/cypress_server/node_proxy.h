#pragma once

#include "public.h"
#include "node.h"

#include <yt/server/object_server/object_proxy.h>

#include <yt/server/security_server/cluster_resources.h>
#include <yt/server/security_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/system_attribute_provider.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INodeFactory by adding Cypress-specific functionality.
struct ICypressNodeFactory
    : public NYTree::ITransactionalNodeFactory
{
    virtual NTransactionServer::TTransaction* GetTransaction() const = 0;
    virtual bool ShouldPreserveExpirationTime() const  = 0;
    virtual bool ShouldPreserveCreationTime() const  = 0;
    virtual NSecurityServer::TAccount* GetNewNodeAccount() const = 0;
    virtual NSecurityServer::TAccount* GetClonedNodeAccount(TCypressNodeBase* sourceNode) const = 0;

    virtual ICypressNodeProxyPtr CreateNode(
        NObjectClient::EObjectType type,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) = 0;

    virtual TCypressNodeBase* InstantiateNode(
        const TNodeId& id,
        NObjectClient::TCellTag externalCellTag) = 0;

    virtual TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ENodeCloneMode mode) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INode by adding functionality that is common to all
//! logical Cypress nodes.
struct ICypressNodeProxy
    : public virtual NYTree::INode
    , public virtual NObjectServer::IObjectProxy
{
    //! Returns the transaction for which the proxy is created.
    virtual NTransactionServer::TTransaction* GetTransaction() const = 0;

    //! Returns the trunk node for which the proxy is created.
    virtual TCypressNodeBase* GetTrunkNode() const = 0;

    //! "Covariant" extension of NYTree::INode::CreateFactory.
    virtual std::unique_ptr<ICypressNodeFactory> CreateCypressFactory(
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options) const = 0;

    static ICypressNodeProxy* FromNode(NYTree::INode* ptr);
    static TIntrusivePtr<ICypressNodeProxy> FromNode(const TIntrusivePtr<NYTree::INode>& ptr);
    static const ICypressNodeProxy* FromNode(const NYTree::INode* ptr);
    static TIntrusivePtr<const ICypressNodeProxy> FromNode(const TIntrusivePtr<const NYTree::INode>& ptr);
};

DEFINE_REFCOUNTED_TYPE(ICypressNodeProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
