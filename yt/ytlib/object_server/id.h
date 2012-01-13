#pragma once

#include "common.h"

#include <ytlib/misc/enum.h>
#include <ytlib/misc/guid.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////
    
//! Provides a globally unique identifier for an object.
/*!
 *  TGuid consists of four 32-bit parts.
 *  For TObjectId, these parts have the following meaning:
 *  
 *  Part 0: some hash
 *  Part 1: bits 0..15:  object type
 *          bits 16..31: cell id
 *  Part 2: the lower  part of 64-bit sequential counter
 *  Part 3: the higher part of 64-bit sequential counter
 *
 */
typedef TGuid TObjectId;

//! The all-zero id used to denote a non-existing object.
extern TObjectId NullObjectId;

DECLARE_ENUM(EObjectType,
    // Transaction Manager stuff
    ((Transaction)(0))

    // Chunk Manager stuff
    ((Chunk)(100))
    ((ChunkList)(101))

    // Cypress stuff
    //   Aux
    ((Lock)(200))
    //   Static
    ((StringNode)(300))
    ((Int64Node)(301))
    ((DoubleNode)(302))
    ((MapNode)(303))
    ((ListNode)(304))
    //   Dynamic
    ((File)(400))
    ((Table)(401))
    ((ChunkMap)(402))
    ((LostChunkMap)(403))
    ((OverreplicatedChunkMap)(404))
    ((UnderreplicatedChunkMap)(405))
    ((ChunkListMap)(406))
    ((TransactionMap)(407))
    ((NodeMap)(408))
    ((LockMap)(409))
    ((Holder)(410))
    ((HolderMap)(411))
    ((OrchidNode)(412))
);

//! Valid types are supposed to be in range [0, MaxObjectType - 1].
/*!
 *  \note
 *  An upper bound will do.
 */
const int MaxObjectType = 1024;

EObjectType TypeFromId(const TObjectId& id);

TObjectId CreateId(
    EObjectType type,
    ui16 cellId,
    ui64 counter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

