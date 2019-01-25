#include "resource_cache.h"
#include "replica_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TResourceCache, TResourceCache::TSpec> TResourceCache::SpecSchema{
    &ResourceCachesTable.Fields.Spec,
    [] (TResourceCache* resourceCache) { return &resourceCache->Spec(); }
};

const TScalarAttributeSchema<TResourceCache, TResourceCache::TStatus> TResourceCache::StatusSchema{
    &ResourceCachesTable.Fields.Status,
    [] (TResourceCache* resourceCache) { return &resourceCache->Status(); }
};

////////////////////////////////////////////////////////////////////////////////

TResourceCache::TResourceCache(
    const TObjectId& id,
    const TObjectId& replicaSetId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, replicaSetId, typeHandler, session)
    , ReplicaSet_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TResourceCache::GetType() const
{
    return EObjectType::ResourceCache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

