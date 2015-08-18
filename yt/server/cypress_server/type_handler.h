#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

#include <core/ytree/public.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <server/transaction_server/public.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides node type-specific services.
struct INodeTypeHandler
    : public virtual TRefCounted
{
    //! Constructs a proxy.
    virtual ICypressNodeProxyPtr GetProxy(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    //! Returns the (dynamic) node type.
    virtual NObjectClient::EObjectType GetObjectType() = 0;

    //! Returns the (static) node type.
    virtual NYTree::ENodeType GetNodeType() = 0;

    //! Create an empty instance of a node.
    //! Called during snapshot deserialization and node cloning.
    virtual std::unique_ptr<TCypressNodeBase> Instantiate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag) = 0;

    typedef NRpc::TTypedServiceRequest<NCypressClient::NProto::TReqCreate> TReqCreate;
    typedef NRpc::TTypedServiceResponse<NCypressClient::NProto::TRspCreate> TRspCreate;
    //! Creates a new trunk node.
    /*!
     *  This is called during |Create| verb.
     */
    virtual std::unique_ptr<TCypressNodeBase> Create(
        const TNodeId& hintId,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes,
        TReqCreate* request,
        TRspCreate* response) = 0;

    //! Performs cleanup on node destruction.
    /*!
     *  This is called prior to the actual removal of the node from the meta-map.
     *  A typical implementation will release the resources held by the node,
     *  decrement the ref-counters of its children etc.
     */
    virtual void Destroy(TCypressNodeBase* node) = 0;

    //! Branches a node into a given transaction.
    virtual std::unique_ptr<TCypressNodeBase> Branch(
        TCypressNodeBase* originatingNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode) = 0;

    //! Called on transaction commit to merge the changes made in the branched node back into the originating one.
    /*!
     *  \note
     *  #branchedNode is non-const for performance reasons (i.e. to swap the data instead of copying).
     */
    virtual void Merge(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) = 0;

    //! Called on transaction abort to perform any cleanup necessary.
    /*!
     *  \note
     *  #Destroy is also called for #branchedNode.
     */
    virtual void Unbranch(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) = 0;

    //! Constructs a deep copy of the node.
    virtual TCypressNodeBase* Clone(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactoryPtr factory,
        const TNodeId& hintId,
        ENodeCloneMode mode) = 0;

    //! Returns |true| if nodes of this type can be stored at external cells.
    virtual bool IsExternalizable() = 0;

    //! Returns the total resource usage as seen by the user.
    //! These values are exposes via |resource_usage| attribute.
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage(const TCypressNodeBase* node) = 0;

    //! Returns the incremental resource usage for accounting purposes.
    //! E.g. when a node is branched in append mode, its initial incremental usage is zero.
    virtual NSecurityServer::TClusterResources GetAccountingResourceUsage(const TCypressNodeBase* node) = 0;

};

DEFINE_REFCOUNTED_TYPE(INodeTypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
