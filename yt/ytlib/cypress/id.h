#pragma once

#include <yt/ytlib/object_server/id.h>
#include <yt/ytlib/transaction_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////
typedef NObjectServer::TObjectId TNodeId;
extern TNodeId NullNodeId;

typedef NObjectServer::TObjectId TLockId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

using NObjectServer::EObjectType;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

