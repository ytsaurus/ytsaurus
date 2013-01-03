#pragma once

#include "public.h"
#include "node.h"

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/system_attribute_provider.h>

#include <server/object_server/object_proxy.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Extends NYTree::INode by adding functionality that is common to all
//! logical Cypress nodes.
struct ICypressNodeProxy
    : public virtual NYTree::INode
    , public virtual NYTree::ISystemAttributeProvider
    , public virtual NObjectServer::IObjectProxy
{
    //! Returns the transaction for which the proxy is created.
    virtual NTransactionServer::TTransaction* GetTransaction() const = 0;

    //! Returns the trunk node for which the proxy is created.
    virtual TCypressNodeBase* GetTrunkNode() const = 0;

    //! Returns resources used by the object.
    /*!
     *  This is displayed in @resource_usage attribute and is not used for accounting.
     *  
     *  \see #ICypressNode::GetResourceUsage
     */
    virtual NSecurityServer::TClusterResources GetResourceUsage() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
