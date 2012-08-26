#pragma once

#include <ytlib/misc/common.h>

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
