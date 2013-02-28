#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManager;
typedef TIntrusivePtr<TCypressManager> TCypressManagerPtr;

struct INodeBehavior;
typedef TIntrusivePtr<INodeBehavior> INodeBehaviorPtr;

struct INodeTypeHandler;
typedef TIntrusivePtr<INodeTypeHandler> INodeTypeHandlerPtr;

class TCypressNodeBase;

struct ICypressNodeProxy;
typedef TIntrusivePtr<ICypressNodeProxy> ICypressNodeProxyPtr;

struct ICypressNodeVisitor;
typedef TIntrusivePtr<ICypressNodeVisitor> ICypressNodeVisitorPtr;

struct TCloneContext;

using NCypressClient::TNodeId;
using NCypressClient::ELockMode;
using NCypressClient::TVersionedNodeId;

using NObjectClient::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
