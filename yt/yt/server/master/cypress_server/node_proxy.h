#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object_proxy.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/system_attribute_provider.h>

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
    virtual bool ShouldPreserveExpirationTimeout() const  = 0;
    virtual bool ShouldPreserveOwner() const  = 0;
    virtual bool ShouldPreserveAcl(NCypressClient::ENodeCloneMode cloneMode) const  = 0;
    virtual bool ShouldAllowSecondaryIndexAbandonment() const = 0;

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
        TNodeId hintId,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) = 0;
    virtual TCypressNode* InstantiateNode(
        TNodeId id,
        NObjectClient::TCellTag externalCellTag) = 0;

    virtual TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ENodeCloneMode mode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        TNodeId hintId = NCypressClient::NullObjectId) = 0;
    virtual TCypressNode* MaterializeNode(
        NYTree::IAttributeDictionary* inheritedAttributes,
        TMaterializeNodeContext* context) = 0;
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
        const TNodeFactoryOptions& options,
        NYPath::TYPath unresolvedPathSuffix) const = 0;

    static ICypressNodeProxy* FromNode(NYTree::INode* ptr);
    static TIntrusivePtr<ICypressNodeProxy> FromNode(const TIntrusivePtr<NYTree::INode>& ptr);
    static const ICypressNodeProxy* FromNode(const NYTree::INode* ptr);
    static TIntrusivePtr<const ICypressNodeProxy> FromNode(const TIntrusivePtr<const NYTree::INode>& ptr);

    virtual void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) = 0;

    virtual void SetAccessed() = 0;
    virtual void SetTouched() = 0;

    virtual void Lock(const TLockRequest& lockRequest, bool waitable, TLockId lockIdHint = {}) = 0;
    virtual void Unlock() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressNodeProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
