#pragma once

#include <yt/ytlib/misc/enum.h>
#include <yt/ytlib/object_server/id.h>
#include <yt/ytlib/transaction_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ERuntimeNodeType,
    // Static types
    ((String)(0))
    ((Int64)(1))
    ((Double)(2))
    ((Map)(3))
    ((List)(4))
    
    // Dynamic types
    ((File)(10))
    ((Table)(11))
    ((ChunkMap)(12))
    ((LostChunkMap)(13))
    ((OverreplicatedChunkMap)(14))
    ((UnderreplicatedChunkMap)(15))
    ((ChunkListMap)(16))
    ((TransactionMap)(17))
    ((NodeMap)(18))
    ((LockMap)(19))
    ((Holder)(20))
    ((HolderMap)(21))
    ((Orchid)(22))
    ((Root)(23))
);

//! Valid types are supposed to be in range [0, MaxRuntimeNodeType - 1].
/*!
 *  \note
 *  An upper bound will do.
 */
const int MaxRuntimeNodeType = 256;

////////////////////////////////////////////////////////////////////////////////

typedef NObjectServer::TObjectId TNodeId;
extern TNodeId NullNodeId;

typedef NObjectServer::TObjectId TLockId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

