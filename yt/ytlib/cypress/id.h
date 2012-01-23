#pragma once

#include <ytlib/object_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): move to common.h
using NObjectServer::TObjectId;
using NObjectServer::NullObjectId;
using NObjectServer::EObjectType;
using NObjectServer::TVersionedObjectId;

// TODO(roizner): move to public.h
typedef TObjectId TNodeId;
typedef TObjectId TLockId;
typedef TVersionedObjectId TVersionedNodeId;

// TODO(roizner): move to common.h
using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
