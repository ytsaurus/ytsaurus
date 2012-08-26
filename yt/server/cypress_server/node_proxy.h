#pragma once

#include "public.h"
#include "node.h"

#include <ytlib/ytree/public.h>
#include <server/object_server/object_proxy.h>

namespace NYT {
namespace NCypressServer {

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
    virtual ICypressNode* GetTrunkNode() const = 0;

    //! Constructs a deep copy of the node.
    virtual ICypressNodeProxyPtr Clone() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
