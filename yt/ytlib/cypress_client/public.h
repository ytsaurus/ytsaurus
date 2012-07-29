#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/object_server/public.h>
#include <ytlib/transaction_server/public.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

using NObjectServer::TObjectId;
using NObjectServer::NullObjectId;
using NObjectServer::EObjectType;
using NObjectServer::TVersionedObjectId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

typedef TObjectId TNodeId;
typedef TVersionedObjectId TVersionedNodeId;

DECLARE_ENUM(ELockMode,
    ((None)(0))
    ((Snapshot)(1))
    ((Shared)(2))
    ((Exclusive)(3))
);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCypressClient
} // namespace NYT
