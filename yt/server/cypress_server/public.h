#pragma once

#include <core/misc/public.h>

#include <core/actions/callback.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManagerConfig;
typedef TIntrusivePtr<TCypressManagerConfig> TCypressManagerConfigPtr;

class TAccessTracker;
typedef TIntrusivePtr<TAccessTracker> TAccessTrackerPtr;

class TCypressManager;
typedef TIntrusivePtr<TCypressManager> TCypressManagerPtr;

struct INodeTypeHandler;
typedef TIntrusivePtr<INodeTypeHandler> INodeTypeHandlerPtr;

class TCypressNodeBase;

struct TLockRequest;
class TLock;

struct ICypressNodeFactory;
typedef TIntrusivePtr<ICypressNodeFactory> ICypressNodeFactoryPtr;

struct ICypressNodeProxy;
typedef TIntrusivePtr<ICypressNodeProxy> ICypressNodeProxyPtr;

struct ICypressNodeVisitor;
typedef TIntrusivePtr<ICypressNodeVisitor> ICypressNodeVisitorPtr;

////////////////////////////////////////////////////////////////////////////////

//! Describes the reason for cloning a node.
//! Some node types may allow moving but not copying.
DEFINE_ENUM(ENodeCloneMode,
    (Copy)
    (Move)
);

////////////////////////////////////////////////////////////////////////////////

using NCypressClient::TNodeId;
using NCypressClient::TLockId;
using NCypressClient::ELockMode;
using NCypressClient::ELockState;
using NCypressClient::TVersionedNodeId;

using NObjectClient::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
