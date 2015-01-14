#pragma once

#include <core/misc/public.h>

#include <core/actions/callback.h>

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
