#pragma once

#include "node.h"

#include <ytlib/ytree/public.h>
#include <ytlib/object_server/object_proxy.h>

namespace NYT {
namespace NCypressServer {

using NObjectServer::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INode by adding functionality that is common to all
//! logical Cypress nodes.
struct ICypressNodeProxy
    : public virtual NYTree::INode
    , public virtual NObjectServer::IObjectProxy
{
    // TODO: removing this causes link error, investigate!
    ICypressNodeProxy()
    { }

    //! Returns the id of the transaction for which the proxy is created.
    virtual TTransactionId GetTransactionId() const = 0;

    //! Returns the physical node.
    virtual const ICypressNode* GetImpl() const = 0;
    
    //! Returns the physical node and allows its mutation.
    virtual ICypressNode* GetImplForUpdate() = 0;

    //! Constructs a deep copy of the node.
    virtual ICypressNodeProxyPtr Clone() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
