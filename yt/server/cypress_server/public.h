#pragma once

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(INodeTypeHandler)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeProxy)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeVisitor)

DECLARE_REFCOUNTED_CLASS(TAccessTracker)
DECLARE_REFCOUNTED_CLASS(TCypressManager)

class TCypressNodeBase;

struct ICypressNodeFactory;

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
