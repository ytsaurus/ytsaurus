#pragma once

#include "public.h"
#include "node.h"

#include <yt/server/master/object_server/object_proxy.h>

#include <yt/server/master/security_server/cluster_resources.h>
#include <yt/server/master/security_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

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

    virtual bool ShouldPreserveCreationTime() const  = 0;
    virtual bool ShouldPreserveModificationTime() const  = 0;
    virtual bool ShouldPreserveExpirationTime() const  = 0;
    virtual bool ShouldPreserveOwner() const  = 0;
    virtual bool ShouldPreserveAcl() const  = 0;

    virtual NSecurityServer::TAccount* GetNewNodeAccount() const = 0;
    virtual NSecurityServer::TAccount* GetClonedNodeAccount(
        NSecurityServer::TAccount* sourceAccount) const = 0;
    virtual void ValidateClonedAccount(
        ENodeCloneMode mode,
        NSecurityServer::TAccount* sourceAccount,
        NSecurityServer::TClusterResources sourceResourceUsage,
        NSecurityServer::TAccount* clonedAccount) = 0;

    virtual ICypressNodeProxyPtr CreateNode(
        NObjectClient::EObjectType type,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) = 0;
    virtual TCypressNode* InstantiateNode(
        TNodeId id,
        NObjectClient::TCellTag externalCellTag) = 0;

    virtual TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ENodeCloneMode mode) = 0;
    virtual TCypressNode* EndCopyNode(
        TEndCopyContext* context) = 0;
    virtual TCypressNode* EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INode by adding functionality that is common to all
//! logical Cypress nodes.
struct ICypressNodeProxy
    : public virtual NYTree::INode
    , public virtual NObjectServer::IObjectProxy
{
    //! Returns the trunk node for which the proxy is created.
    virtual TCypressNode* GetTrunkNode() const = 0;

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
