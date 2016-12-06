#pragma once

#include "public.h"

#include <yt/ytlib/ypath/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/hydra/version.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

//! |#|-prefix.
extern const TStringBuf ObjectIdPathPrefix;

//! Creates the YPath pointing to an object with a given #id.
NYPath::TYPath FromObjectId(const TObjectId& id);

//! Checks if the given type is versioned, i.e. represents a Cypress node.
bool IsVersionedType(EObjectType type);

//! Checks if the given type is user, i.e. regular users are allowed to create its instances.
bool IsUserType(EObjectType type);

//! Extracts the type component from #id.
EObjectType TypeFromId(const TObjectId& id);

//! Extracts the cell id component from #id.
TCellTag CellTagFromId(const TObjectId& id);

//! Extracts the counter component from #id.
ui64 CounterFromId(const TObjectId& id);

//! Returns |true| iff a given regular #type has an associated schema type.
bool HasSchema(EObjectType type);

//! Returns the schema type for a given regular #type.
EObjectType SchemaTypeFromType(EObjectType type);

//! Returns the regular type for a given schema #type.
EObjectType TypeFromSchemaType(EObjectType type);

//! Constructs the id from its parts.
TObjectId MakeId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter,
    ui32 hash);

//! Creates a random id with given type and cell tag.
TObjectId MakeRandomId(
    EObjectType type,
    TCellTag cellTag);

//! Returns |true| if a given #id is well-known.
/*
 *  This method checks the highest bit of counter part.
 */
bool IsWellKnownId(const TObjectId& id);

//! Constructs the id for a regular object.
TObjectId MakeRegularId(
    EObjectType type,
    TCellTag cellTag,
    NHydra::TVersion version,
    ui32 hash);

//! Constructs the id corresponding to well-known (usually singleton) entities.
/*
 *  The highest bit of #counter must be set.
 */
TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter = 0xffffffffffffffff);

//! Returns the id of the schema object for a given regular type.
TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellTag cellTag);

//! Constructs a new object id by replacing type component in a given one.
TObjectId ReplaceTypeInId(
    const TObjectId& id,
    EObjectType type);

//! Constructs a new object id by replacing cell tag component in a given one.
TObjectId ReplaceCellTagInId(
    const TObjectId& id,
    TCellTag cellTag);

////////////////////////////////////////////////////////////////////////////////

//! Relies on first 32 bits of object id to be pseudo-random,
//! cf. TObjectManager::GenerateId.
struct TDirectObjectIdHash
{
    size_t operator()(const TObjectId& id) const;
};

//! Cf. TDirectObjectIdHash
struct TDirectVersionedObjectIdHash
{
    size_t operator()(const TVersionedObjectId& id) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
