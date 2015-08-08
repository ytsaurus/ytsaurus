#pragma once

#include <core/misc/public.h>
#include <core/misc/small_vector.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(INodeTypeHandler)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeFactory)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeProxy)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeVisitor)

DECLARE_REFCOUNTED_CLASS(TAccessTracker)
DECLARE_REFCOUNTED_CLASS(TCypressManager)

class TCypressNodeBase;

using TCypressNodeList = SmallVector<TCypressNodeBase*, 8>;

struct TLockRequest;
class TLock;

DECLARE_REFCOUNTED_CLASS(TCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Describes the reason for cloning a node.
//! Some node types may allow moving but not copying.
DEFINE_ENUM(ENodeCloneMode,
    (Copy)
    (Move)
);

//! Defines the way Cypress Manager externalizes nodes
DEFINE_ENUM(EExternalizationMode,
    (Disabled)   // no external nodes can be created
    (Automatic)  // all externalizable are externalized by default
    (Manual)     // only explicit externalization is allowed
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
