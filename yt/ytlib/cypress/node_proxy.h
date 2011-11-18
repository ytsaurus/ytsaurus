#pragma once

#include "common.h"
#include "node.h"

#include "../ytree/ytree.h"
#include "../ytree/ypath_service.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INode by adding functionality that is common to all
//! logical Cypress nodes.
struct ICypressNodeProxy
    : virtual NYTree::INode
{
    typedef TIntrusivePtr<ICypressNodeProxy> TPtr;

    // TODO: removing this causes link error, investigate!
    ICypressNodeProxy()
    { }

    //! Returns the id of the transaction for which the proxy is created.
    virtual TTransactionId GetTransactionId() const = 0;

    //! Returns the id of the physical node.
    virtual TNodeId GetNodeId() const = 0;

    //! Returns the physical node.
    virtual const ICypressNode& GetImpl() const = 0;
    
    //! Returns the physical node and allows its mutation.
    virtual ICypressNode& GetImplForUpdate() = 0;

    //! Returns the handler of this node type.
    virtual INodeTypeHandler::TPtr GetTypeHandler() const = 0;

    //! Returns true iff the change specified by the #context
    //! requires meta state logging.
    virtual bool IsLogged(NRpc::IServiceContext* context) const = 0;

    //! Returns true iff the change specified by the #context
    //! requires an automatic transaction.
    virtual bool IsTransactionRequired(NRpc::IServiceContext* context) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
