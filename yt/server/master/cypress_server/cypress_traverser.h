#pragma once

#include "public.h"

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/security_server/public.h>

#include <yt/server/master/cell_master/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/actions/public.h>

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

void TraverseCypress(
    TCypressManagerPtr cypressManager,
    NTransactionServer::TTransactionManagerPtr transactionManager,
    NObjectServer::TObjectManagerPtr objectManager,
    NSecurityServer::TSecurityManagerPtr securityManager,
    IInvokerPtr invoker,
    TCypressNode* trunkRootNode,
    NTransactionServer::TTransaction* transaction,
    ICypressNodeVisitorPtr visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
