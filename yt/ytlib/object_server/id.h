#pragma once

#include "common.h"

#include <yt/ytlib/misc/enum.h>
#include <yt/ytlib/misc/guid.h>

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
    ((Chunk)(1))
    ((ChunkList)(2))

    // Cypress stuff
    ((Lock)(3))
    ((Node)(4))
);

//! Valid types are supposed to be in range [0, MaxObjectType - 1].
/*!
 *  \note
 *  An upper bound will do.
 */
const int MaxObjectType = 256;

EObjectType TypeFromId(const TObjectId& id);

TObjectId CreateId(
    EObjectType type,
    ui16 cellId,
    ui64 counter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

