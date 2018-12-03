#pragma once

#include "public.h"

#include <yt/server/transaction_server/public.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT::NCypressServer {

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
    virtual NObjectClient::EObjectType GetObjectType() const = 0;

    //! Returns the (static) node type.
    virtual NYTree::ENodeType GetNodeType() const = 0;

    //! Create an empty instance of a node.
    //! Called during snapshot deserialization and node cloning.
    virtual std::unique_ptr<TCypressNodeBase> Instantiate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag) = 0;

    //! Creates a new trunk node.
    /*!
     *  This is called during |Create| verb.
     */
    virtual std::unique_ptr<TCypressNodeBase> Create(
        const TNodeId& hintId,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account) = 0;

    //! Fills attributes of a trunk node. Usually applied to newly created nodes.
    virtual void FillAttributes(
        TCypressNodeBase* trunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) = 0;

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
        const TLockRequest& lockRquest) = 0;

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

    //! Returns #true iff the branched node differs from (contains changes to) the originating node.
    /*!
     *  \note
     *  Called prior to unlocking a node (by an explicit request) to make sure no changes will be lost.
     */
    virtual bool HasBranchedChanges(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) = 0;

    //! Constructs a deep copy of the node.
    virtual TCypressNodeBase* Clone(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactory* factory,
        const TNodeId& hintId,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) = 0;

    //! Returns |true| if nodes of this type can be stored at external cells.
    virtual bool IsExternalizable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeTypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
