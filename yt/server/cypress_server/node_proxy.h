#pragma once

#include "public.h"
#include "node.h"

#include <core/ytree/node.h>
#include <core/ytree/system_attribute_provider.h>

#include <core/rpc/service_detail.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <server/object_server/object_proxy.h>

#include <server/security_server/public.h>
#include <server/security_server/cluster_resources.h>

#include <server/transaction_server/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INodeFactory by adding Cypress-specific functionality.
struct ICypressNodeFactory
    : public NYTree::INodeFactory
{
    typedef NRpc::TTypedServiceRequest<NCypressClient::NProto::TReqCreate> TReqCreate;
    typedef NRpc::TTypedServiceResponse<NCypressClient::NProto::TRspCreate> TRspCreate;

    virtual NTransactionServer::TTransaction* GetTransaction() = 0;

    virtual NSecurityServer::TAccount* GetNewNodeAccount() = 0;
    virtual NSecurityServer::TAccount* GetClonedNodeAccount(
        TCypressNodeBase* sourceNode) = 0;

    virtual ICypressNodeProxyPtr CreateNode(
        NObjectClient::EObjectType type,
        NYTree::IAttributeDictionary* attributes) = 0;

    virtual TCypressNodeBase* InstantiateNode(
        const TNodeId& id,
        NObjectClient::TCellTag externalCellTag) = 0;

    virtual TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ENodeCloneMode mode) = 0;

};

DEFINE_REFCOUNTED_TYPE(ICypressNodeFactory)

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
    virtual ICypressNodeFactoryPtr CreateCypressFactory(
        NSecurityServer::TAccount* account,
        bool preserveAccount) const = 0;

};

DEFINE_REFCOUNTED_TYPE(ICypressNodeProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
