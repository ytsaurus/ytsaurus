#pragma once

#include <yt/client/object_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;
using NObjectClient::EObjectType;
using NObjectClient::TVersionedObjectId;

using TNodeId = TObjectId;
using TLockId = TObjectId;
using TVersionedNodeId = TVersionedObjectId;

extern const TLockId NullLockId;

// NB: The order is from weakest to strongest.
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

} // namespace NYT::NCypressClient
