#pragma once

#include <core/misc/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;
using NObjectClient::EObjectType;
using NObjectClient::TVersionedObjectId;

typedef TObjectId TNodeId;
typedef TObjectId TLockId;
typedef TVersionedObjectId TVersionedNodeId;

extern const TLockId NullLockId;

DEFINE_ENUM(ELockMode,
    ((None)      (0))
    ((Snapshot)  (1))
    ((Shared)    (2))
    ((Exclusive) (3))
);

DEFINE_ENUM(ELockState,
    ((Pending)   (0))
    ((Acquired)  (1))
);

DEFINE_ENUM(EErrorCode,
    ((SameTransactionLockConflict)         (400))
    ((DescendantTransactionLockConflict)   (401))
    ((ConcurrentTransactionLockConflict)   (402))
    ((PendingLockConflict)                 (403))
    ((LockDestroyed)                       (404))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
