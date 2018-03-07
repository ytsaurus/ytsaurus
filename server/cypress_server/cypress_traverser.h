#pragma once

#include "public.h"

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/security_server/public.h>

#include <yt/server/cell_master/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/actions/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeVisitor
    : public virtual TRefCounted
{
    virtual void OnNode(TCypressNodeBase* trunkNode, NTransactionServer::TTransaction* transaction) = 0;
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
    TCypressNodeBase* trunkRootNode,
    NTransactionServer::TTransaction* transaction,
    ICypressNodeVisitorPtr visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
