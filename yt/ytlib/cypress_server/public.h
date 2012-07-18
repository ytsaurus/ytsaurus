#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/cypress_client/id.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManager;
typedef TIntrusivePtr<TCypressManager> TCypressManagerPtr;

struct INodeBehavior;
typedef TIntrusivePtr<INodeBehavior> INodeBehaviorPtr;

struct INodeTypeHandler;
typedef TIntrusivePtr<INodeTypeHandler> INodeTypeHandlerPtr;

struct ICypressNode;

struct ICypressNodeProxy;
typedef TIntrusivePtr<ICypressNodeProxy> ICypressNodeProxyPtr;

using NCypressClient::TNodeId;
using NCypressClient::ELockMode;
using NCypressClient::TVersionedNodeId;

using NObjectServer::EObjectType;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCypressServer
} // namespace NYT
