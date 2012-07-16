#pragma once

#include <ytlib/object_server/id.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): move to common.h
using NObjectServer::TObjectId;
using NObjectServer::NullObjectId;
using NObjectServer::EObjectType;
using NObjectServer::TVersionedObjectId;

// TODO(roizner): move to public.h
typedef TObjectId TNodeId;
typedef TVersionedObjectId TVersionedNodeId;

// TODO(roizner): move to common.h
using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

DECLARE_ENUM(ELockMode,
    ((None)(0))
    ((Snapshot)(1))
    ((Shared)(2))
    ((Exclusive)(3))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
