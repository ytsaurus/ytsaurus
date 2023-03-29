#pragma once

#include "public.h"

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeVisitor
    : public virtual TRefCounted
{
    virtual void OnNode(TCypressNode* trunkNode, NTransactionServer::TTransaction* transaction) = 0;
    virtual void OnError(const TError& error) = 0;
    virtual void OnCompleted() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressNodeVisitor)

////////////////////////////////////////////////////////////////////////////////

void TraverseCypress(
    ICypressManagerPtr cypressManager,
    NTransactionServer::ITransactionManagerPtr transactionManager,
    NObjectServer::IObjectManagerPtr objectManager,
    NSecurityServer::ISecurityManagerPtr securityManager,
    IInvokerPtr invoker,
    TCypressNode* trunkRootNode,
    NTransactionServer::TTransaction* transaction,
    ICypressNodeVisitorPtr visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
