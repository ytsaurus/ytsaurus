#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NCypressClient {

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

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCypressClient
} // namespace NYT
